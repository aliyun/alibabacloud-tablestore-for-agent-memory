import base64
import json
import logging
import math
import time
from collections.abc import Iterator
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import six
import tablestore
from tablestore import (
    ComparatorType,
    SecondaryIndexMeta,
    SecondaryIndexType,
    SingleColumnCondition, Query, MatchAllQuery, BoolQuery, IndexSetting, )

from tablestore_for_agent_memory.base.base_knowledge_store import Document, DocumentHit
from tablestore_for_agent_memory.base.base_memory_store import Message, Session
from tablestore_for_agent_memory.base.common import MetaType, Order
from tablestore_for_agent_memory.base.filter import (
    AND,
    GT,
    GTE,
    LT,
    LTE,
    NOT,
    OR,
    BaseConditionFilter,
    BaseOperatorFilter,
    Eq,
    Filter,
    NotEQ, NotIN, TextMatchPhrase, TextMatch, All, VectorQuery,
)

logger = logging.getLogger(__name__)

class TablestoreHelper:
    
    @staticmethod
    def create_table(
            tablestore_client: tablestore.OTSClient,
            table_name: str,
            schema_of_primary_key: List[Tuple[str, MetaType]],
            defined_columns: List[Tuple[str, MetaType]] = None,
    ) -> None:
        table_names = tablestore_client.list_table()
        if table_name in table_names:
            logger.warning(f"tablestore table:[{table_name}] already exists")
            return
        schema_of_primary_key = [(t[0], t[1].value) for t in schema_of_primary_key]
        defined_columns = [] if defined_columns is None else [(t[0], t[1].value) for t in defined_columns]
        table_meta = tablestore.TableMeta(table_name, schema_of_primary_key, defined_columns)
        table_options = tablestore.TableOptions()
        reserved_throughput = tablestore.ReservedThroughput(tablestore.CapacityUnit(0, 0))
        try:
            tablestore_client.create_table(table_meta, table_options, reserved_throughput)
            logger.info("Tablestore create table[%s] successfully.", table_name)
        except Exception as e:
            logger.exception("Tablestore create table[%s] failed", table_name)
            raise e

    @staticmethod
    def create_secondary_index(
            tablestore_client: tablestore.OTSClient,
            table_name: str,
            secondary_index_name: str,
            primary_key_names: List[str],
            defined_column_names: List[str],
            index_type=SecondaryIndexType.LOCAL_INDEX,
    ) -> None:
        describe_table_response = tablestore_client.describe_table(table_name)
        if secondary_index_name in [index.index_name for index in describe_table_response.secondary_indexes]:
            logger.warning(f"tablestore secondary index:[{secondary_index_name}] already exists")
            return
        index_meta = SecondaryIndexMeta(secondary_index_name, primary_key_names, defined_column_names, index_type)
        include_base_data = False
        try:
            tablestore_client.create_secondary_index(table_name, index_meta, include_base_data)
            logger.info("Tablestore create secondary_index[%s] successfully.", secondary_index_name)
        except Exception as e:
            logger.exception("Tablestore create secondary_index[%s] failed", secondary_index_name)
            raise e
    
    @staticmethod
    def delete_table(
            tablestore_client: tablestore.OTSClient,
            table_name: str,
    ) -> None:
        """Delete table if exists."""
        try:
            search_index_list = tablestore_client.list_search_index(table_name=table_name)
            for resp_tuple in search_index_list:
                tablestore_client.delete_search_index(resp_tuple[0], resp_tuple[1])
                logger.info("Tablestore delete search index[%s] successfully.", resp_tuple[1])
        except tablestore.OTSServiceError as e:
            if (
                    e.get_error_code() == "OTSParameterInvalid" or e.get_error_code() == "OTSObjectNotExist"
            ) and "does not exist" in e.get_error_message():
                logger.exception("delete table[%s] failed, which is not exist", table_name)
            else:
                raise e
    
        try:
            tablestore_client.delete_table(table_name)
            logger.info("Tablestore delete table[%s] successfully.", table_name)
        except tablestore.OTSServiceError as e:
            if e.get_error_code() == "OTSObjectNotExist" and "does not exist" in e.get_error_message():
                logger.exception("delete table[%s] failed, which is not exist", table_name)
            else:
                raise e
    
    @staticmethod
    def meta_data_to_ots_columns(metadata: Dict[str, Any]) -> List[Tuple]:
        metadata_columns = []
        for meta_key in metadata:
            meta_value = metadata[meta_key]
            if isinstance(meta_value, bool):
                metadata_columns.append((meta_key, meta_value))
            elif isinstance(meta_value, int):
                metadata_columns.append((meta_key, meta_value))
            elif isinstance(meta_value, six.text_type) or isinstance(meta_value, six.binary_type):
                if isinstance(meta_value, six.text_type):
                    meta_value = meta_value.encode("utf-8")
                metadata_columns.append((meta_key, meta_value))
            elif isinstance(meta_value, bytearray):
                metadata_columns.append((meta_key, meta_value))
            elif isinstance(meta_value, float):
                metadata_columns.append((meta_key, meta_value))
            else:
                raise RuntimeError("Unsupported column type: " + str(type(meta_value)))
    
        return metadata_columns
    
    @staticmethod
    def row_to_session(row: Optional[tablestore.Row]) -> Optional[Session]:
        if row is None:
            return None
        if len(row.primary_key) == 2:
            user_id = row.primary_key[0][1]
            session_id = row.primary_key[1][1]
            update_time = None
        else:
            user_id = row.primary_key[0][1]
            update_time = row.primary_key[1][1]
            session_id = row.primary_key[2][1]
        metadata = {}
        for col in row.attribute_columns:
            key = col[0]
            val = col[1]
            if key == "update_time":
                update_time = val
                continue
            metadata[key] = val
        return Session(
            user_id=user_id,
            session_id=session_id,
            update_time=update_time,
            metadata=metadata,
        )
    
    @staticmethod
    def row_to_document(row: Optional[tablestore.Row], text_field: str, embedding_field: str) -> Optional[Document]:
        if row is None:
            return None
        document_id = row.primary_key[0][1]
        tenant_id = row.primary_key[1][1]
        metadata = {}
        text_content = None
        vector_content = None
        for col in row.attribute_columns:
            key = col[0]
            val = col[1]
            if key == text_field:
                text_content = val
                continue
            if key == embedding_field:
                vector_content = json.loads(val)
                continue
            metadata[key] = val
        return Document(
            document_id=document_id,
            tenant_id=tenant_id,
            text=text_content,
            embedding=vector_content,
            metadata=metadata,
        )
    
    @staticmethod
    def row_to_message_create_time(row: Optional[tablestore.Row]) -> Optional[int]:
        if row is None:
            return None
        create_time = row.primary_key[2][1]
        assert isinstance(create_time, int)
        return create_time
    
    @staticmethod
    def row_to_message(row: Optional[tablestore.Row]) -> Optional[Message]:
        if row is None:
            return None
        session_id = row.primary_key[0][1]
        create_time = row.primary_key[1][1]
        message_id = row.primary_key[2][1]
        metadata = {}
        content = None
        for col in row.attribute_columns:
            key = col[0]
            val = col[1]
            if key == "content":
                content = val
                continue
            metadata[key] = val
        return Message(
            session_id=session_id,
            message_id=message_id,
            create_time=create_time,
            content=content,
            metadata=metadata,
        )
    
    @staticmethod
    def paser_table_filters(
            metadata_filter: Optional[Filter],
    ) -> Optional[tablestore.CompositeColumnCondition]:
        return TablestoreHelper.inner_parse_table_filters(
            metadata_filter=metadata_filter,
            parse_operator_filter_function=TablestoreHelper.parse_table_filter,
        )
    
    @staticmethod
    def inner_parse_table_filters(
            metadata_filter: Optional[Filter],
            parse_operator_filter_function: Callable[[BaseOperatorFilter], Union[tablestore.SingleColumnCondition]],
    ) -> Optional[Union[tablestore.SingleColumnCondition, tablestore.CompositeColumnCondition]]:
        if metadata_filter is None:
            return None
        if isinstance(metadata_filter, BaseConditionFilter):
            if isinstance(metadata_filter, AND):
                cond = tablestore.CompositeColumnCondition(tablestore.LogicalOperator.AND)
                for filter_item in metadata_filter.filters:
                    cond.add_sub_condition(TablestoreHelper.inner_parse_table_filters(filter_item, parse_operator_filter_function))
                return cond
            elif isinstance(metadata_filter, OR):
                cond = tablestore.CompositeColumnCondition(tablestore.LogicalOperator.OR)
                for filter_item in metadata_filter.filters:
                    cond.add_sub_condition(TablestoreHelper.inner_parse_table_filters(filter_item, parse_operator_filter_function))
                return cond
            elif isinstance(metadata_filter, NOT):
                cond = tablestore.CompositeColumnCondition(tablestore.LogicalOperator.NOT)
                for filter_item in metadata_filter.filters:
                    cond.add_sub_condition(TablestoreHelper.inner_parse_table_filters(filter_item, parse_operator_filter_function))
                return cond
            else:
                raise ValueError(f"Unsupported filter condition: {metadata_filter}")
        elif isinstance(metadata_filter, BaseOperatorFilter):
            return parse_operator_filter_function(metadata_filter)
    
    @staticmethod
    def parse_table_filter(
            filter_item: BaseOperatorFilter,
    ) -> tablestore.SingleColumnCondition:
        if isinstance(filter_item, BaseOperatorFilter):
            if isinstance(filter_item, Eq):
                return SingleColumnCondition(filter_item.meta_key, filter_item.meta_value, ComparatorType.EQUAL)
            elif isinstance(filter_item, NotEQ):
                return SingleColumnCondition(filter_item.meta_key, filter_item.meta_value, ComparatorType.NOT_EQUAL)
            elif isinstance(filter_item, GT):
                return SingleColumnCondition(
                    filter_item.meta_key,
                    filter_item.meta_value,
                    ComparatorType.GREATER_THAN,
                )
            elif isinstance(filter_item, GTE):
                return SingleColumnCondition(
                    filter_item.meta_key,
                    filter_item.meta_value,
                    ComparatorType.GREATER_EQUAL,
                )
            elif isinstance(filter_item, LT):
                return SingleColumnCondition(filter_item.meta_key, filter_item.meta_value, ComparatorType.LESS_THAN)
            elif isinstance(filter_item, LTE):
                return SingleColumnCondition(filter_item.meta_key, filter_item.meta_value, ComparatorType.LESS_EQUAL)
            else:
                raise ValueError(
                    f"Unsupported filter type: {type(filter_item)} with key: {filter_item.meta_key}, value: {filter_item.meta_value}"
                )
        else:
            raise ValueError(f"Unsupported filter type: {type(filter_item)}")
    
    @staticmethod
    def paser_search_index_filters(
            metadata_filter: Optional[Filter],
    ) -> (tablestore.Query, bool):
        return TablestoreHelper.inner_parse_search_index_filters(
            metadata_filter=metadata_filter,
            parse_operator_filter_function=TablestoreHelper.parse_search_index_filter,
        )
    
    @staticmethod
    def inner_parse_search_index_filters(
            metadata_filter: Optional[Filter],
            parse_operator_filter_function: Callable[[BaseOperatorFilter], Tuple[tablestore.Query, bool]],
    ) -> (Union[Query, MatchAllQuery, BoolQuery, None], bool):
        if metadata_filter is None:
            return tablestore.MatchAllQuery(), False
        if isinstance(metadata_filter, BaseConditionFilter):
            bool_query = tablestore.BoolQuery(
                must_queries=[],
                must_not_queries=[],
                filter_queries=[],
                should_queries=[],
                minimum_should_match=None,
            )
            if isinstance(metadata_filter, AND):
                need_score_sort = False
                for filter_item in metadata_filter.filters:
                    q, _need_score_sort = TablestoreHelper.inner_parse_search_index_filters(filter_item, parse_operator_filter_function)
                    need_score_sort = need_score_sort or _need_score_sort
                    bool_query.must_queries.append(q)
                return bool_query, need_score_sort
            elif isinstance(metadata_filter, OR):
                need_score_sort = False
                for filter_item in metadata_filter.filters:
                    q, _need_score_sort = TablestoreHelper.inner_parse_search_index_filters(filter_item, parse_operator_filter_function)
                    need_score_sort = need_score_sort or _need_score_sort
                    bool_query.should_queries.append(q)
                return bool_query, need_score_sort
            elif isinstance(metadata_filter, NOT):
                need_score_sort = False
                for filter_item in metadata_filter.filters:
                    q, _need_score_sort = TablestoreHelper.inner_parse_search_index_filters(filter_item, parse_operator_filter_function)
                    need_score_sort = need_score_sort or _need_score_sort
                    bool_query.must_not_queries.append(q)
                return bool_query, need_score_sort
            else:
                raise ValueError(f"Unsupported filter condition: {metadata_filter}")
        elif isinstance(metadata_filter, BaseOperatorFilter):
            return parse_operator_filter_function(metadata_filter)
    
    @staticmethod
    def parse_search_index_filter(
            filter_item: BaseOperatorFilter,
    ) -> (tablestore.Query, bool):
        if isinstance(filter_item, BaseOperatorFilter):
            if isinstance(filter_item, Eq):
                return tablestore.TermQuery(field_name=filter_item.meta_key, column_value=filter_item.meta_value), False
            elif isinstance(filter_item, NotEQ):
                bool_query = tablestore.BoolQuery(
                    must_queries=[],
                    must_not_queries=[],
                    filter_queries=[],
                    should_queries=[],
                    minimum_should_match=None,
                )
                bool_query.must_not_queries.append(tablestore.TermQuery(field_name=filter_item.meta_key, column_value=filter_item.meta_value))
                return bool_query, False
            elif isinstance(filter_item, GT):
                return tablestore.RangeQuery(
                    field_name=filter_item.meta_key, range_from=filter_item.meta_value, include_lower=False
                ), False
            elif isinstance(filter_item, GTE):
                return tablestore.RangeQuery(
                    field_name=filter_item.meta_key, range_from=filter_item.meta_value, include_lower=True
                ), False
            elif isinstance(filter_item, LT):
                return tablestore.RangeQuery(
                    field_name=filter_item.meta_key, range_to=filter_item.meta_value, include_upper=False
                ), False
            elif isinstance(filter_item, LTE):
                return tablestore.RangeQuery(
                    field_name=filter_item.meta_key, range_to=filter_item.meta_value, include_upper=True
                ), False
            elif isinstance(filter_item, NotIN):
                bool_query = tablestore.BoolQuery(
                    must_queries=[],
                    must_not_queries=[],
                    filter_queries=[],
                    should_queries=[],
                    minimum_should_match=None,
                )
                bool_query.must_not_queries.append(tablestore.TermsQuery(field_name=filter_item.meta_key, column_values=filter_item.meta_values))
                return bool_query, False
            elif isinstance(filter_item, TextMatch):
                return tablestore.MatchQuery(field_name=filter_item.meta_key, text=filter_item.meta_value), True
            elif isinstance(filter_item, TextMatchPhrase):
                return tablestore.MatchPhraseQuery(field_name=filter_item.meta_key, text=filter_item.meta_value), True
            elif isinstance(filter_item, VectorQuery):
                metadata_filter, _ = TablestoreHelper.paser_search_index_filters(filter_item.metadata_filter)
                return tablestore.KnnVectorQuery(
                    field_name=filter_item.meta_key,
                    top_k=filter_item.top_k,
                    float32_query_vector=filter_item.query_vector,
                    filter=metadata_filter,
                ), True
            elif isinstance(filter_item, All):
                return tablestore.MatchAllQuery(), False
            else:
                raise ValueError(
                    f"Unsupported filter type: {type(filter_item)} with key: {filter_item.meta_key}, value: {filter_item.meta_value}"
                )
        else:
            raise ValueError(f"Unsupported filter type: {type(filter_item)}")
    
    
    class GetRangeIterator(Iterator):
        def __init__(
                self,
                tablestore_client: tablestore.OTSClient,
                table_name: str,
                translate_function: Callable[[Optional[tablestore.Row]], Optional[Any]],
                inclusive_start_primary_key: List[Tuple],
                exclusive_end_primary_key: List[Tuple],
                metadata_filter: Optional[Filter] = None,
                order: Order = Order.DESC,
                batch_size: Optional[int] = None,
                max_count: Optional[int] = None,
        ):
            self.tablestore_client = tablestore_client
            self.table_name = table_name
            self.translate_function = translate_function
            self.condition = TablestoreHelper.paser_table_filters(metadata_filter=metadata_filter)
            self.order = order
            self.inclusive_start_primary_key = inclusive_start_primary_key
            self.exclusive_end_primary_key = exclusive_end_primary_key
            self.batch_size = batch_size
            self.row_list = None
            self.count = 0
            self.max_count = max_count
            self._fetch_next_batch()
    
        def __iter__(self):
            return self
    
        def __next__(self) -> Any:
            if self.max_count is not None and 0 < self.max_count <= self.count:
                raise StopIteration
            if not self.row_list and self._has_next_batch():
                self._fetch_next_batch()
    
            if not self.row_list:
                raise StopIteration
    
            item = self.row_list.pop(0)
            self.count += 1
            return self.translate_function(item)
    
        def _fetch_next_batch(self) -> None:
            _, next_start_primary_key, row_list, _ = self.tablestore_client.get_range(
                table_name=self.table_name,
                direction=self.order.value,
                inclusive_start_primary_key=self.inclusive_start_primary_key,
                exclusive_end_primary_key=self.exclusive_end_primary_key,
                columns_to_get=None,
                limit=self.batch_size,
                column_filter=self.condition,
                max_version=1,
            )
            self.row_list = row_list
            self.inclusive_start_primary_key = next_start_primary_key
    
        def _has_next_batch(self):
            return self.inclusive_start_primary_key is not None
    
        def next_start_primary_key(self):
            if len(self.row_list) > 0:
                return self.row_list[0]
            else:
                return self.inclusive_start_primary_key
    
    @staticmethod
    def batch_delete(
            tablestore_client: tablestore.OTSClient,
            table_name: str,
            iterator: Iterator[Union[Message, Session, Document]]
    ) -> None:
        batch_delete_row_list = []
    
        def delete_fun():
            request = tablestore.BatchWriteRowRequest()
            request.add(tablestore.TableInBatchWriteRowItem(table_name, batch_delete_row_list))
            try:
                result = tablestore_client.batch_write_row(request)
                _, failed_rows = result.get_delete()
                for f in failed_rows:
                    logger.info(f"delete row failed, error code:{f.error_code}, error msg:{f.error_message}, delete row detail: {batch_delete_row_list[f.index]}")
            except BaseException as ex:
                logger.info(f"delete rows failed: {ex}, delete rows detail: {batch_delete_row_list}")
    
        for item in iterator:
            if isinstance(item, Message):
                primary_key = [
                    ("session_id", item.session_id),
                    ("create_time", item.create_time),
                    ("message_id", item.message_id),
                ]
            elif isinstance(item, Session):
                primary_key = [
                    ("user_id", item.user_id),
                    ("session_id", item.session_id),
                ]
            elif isinstance(item, Document):
                primary_key = [
                    ("document_id", item.document_id),
                    ("tenant_id", item.tenant_id),
                ]
            else:
                raise ValueError(f"Unsupported delete item type: {type(item)}")
            row = tablestore.Row(primary_key)
            condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
            delete_row_item = tablestore.DeleteRowItem(row, condition)
            batch_delete_row_list.append(delete_row_item)
            if len(batch_delete_row_list) == 200:
                delete_fun()
        if len(batch_delete_row_list) != 0:
            delete_fun()
    
    @staticmethod
    def encode_next_primary_key_token(next_primary_key: List[Tuple]) -> str:
        json_str = json.dumps(next_primary_key, ensure_ascii=False)
        return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
    
    @staticmethod
    def decode_next_primary_key_token(next_token: str) -> List[Tuple]:
        next_token_json_str = base64.b64decode(next_token.encode('utf-8')).decode('utf-8')
        keys = json.loads(next_token_json_str)
        return [(key[0], key[1]) for key in keys]
    
    @staticmethod
    def add_schema(new_schema: tablestore.FieldSchema, schemas: Optional[List[tablestore.FieldSchema]] = None) -> List[tablestore.FieldSchema]:
        if schemas is None:
            return [new_schema]
        else:
            for schema in schemas:
                if schema.field_name == new_schema.field_name:
                    return schemas
            schemas.append(new_schema)
            return schemas
    
    @staticmethod
    def create_search_index_if_not_exist(
            tablestore_client: tablestore.OTSClient,
            table_name: str,
            index_name: str,
            index_schemas: List[tablestore.FieldSchema],
            routing_fields:Optional[List[str]]=None,
    ):
        if routing_fields is None:
            routing_fields = []
        index_setting = IndexSetting(routing_fields=routing_fields)
        search_index_list = tablestore_client.list_search_index(
            table_name=table_name
        )
        if index_name in [t[1] for t in search_index_list]:
            logger.warning(f"tablestore search index[{index_name}] already exists")
            return
        index_meta = tablestore.SearchIndexMeta(index_schemas, index_setting=index_setting)
        tablestore_client.create_search_index(
            table_name, index_name, index_meta
        )
        logger.info(f"tablestore create search index[{index_name}] successfully.")
    
    @staticmethod
    def search_response_to_sessions(search_response: tablestore.SearchResponse) -> (List[Session], Optional[str]):
        sessions = []
        next_token = search_response.next_token
        if next_token:
            next_token = base64.b64encode(next_token).decode('utf-8')
        else:
            next_token = None
        for hit in search_response.search_hits:
            row = hit.row
            user_id = row[0][0][1]
            session_id = row[0][1][1]
            meta_data = {}
            update_time = None
            for col in row[1]:
                key = col[0]
                val = col[1]
                if key == "update_time":
                    update_time = val
                    continue
                meta_data[key] = val
            session = Session(user_id=user_id, session_id=session_id, update_time=update_time, metadata=meta_data)
            sessions.append(session)
        return sessions, next_token
    
    @staticmethod
    def search_response_to_message(search_response: tablestore.SearchResponse) -> (List[Message], Optional[str]):
        messages = []
        next_token = search_response.next_token
        if next_token:
            next_token = base64.b64encode(next_token).decode('utf-8')
        else:
            next_token = None
        for hit in search_response.search_hits:
            row = hit.row
            session_id = row[0][0][1]
            create_time = row[0][1][1]
            message_id = row[0][2][1]
            meta_data = {}
            content = None
            for col in row[1]:
                key = col[0]
                val = col[1]
                if key == "content":
                    content = val
                    continue
                meta_data[key] = val
            message = Message(session_id=session_id, message_id=message_id, create_time=create_time, content=content, metadata=meta_data)
            messages.append(message)
        return messages, next_token
    
    @staticmethod
    def search_response_to_document(search_response: tablestore.SearchResponse, text_field: str, embedding_field: str) -> (List[DocumentHit], Optional[str]):
        hits = []
        next_token = search_response.next_token
        if next_token:
            next_token = base64.b64encode(next_token).decode('utf-8')
        else:
            next_token = None
        for hit in search_response.search_hits:
            row = hit.row
            score = hit.score
            document_id = row[0][0][1]
            tenant_id = row[0][1][1]
            meta_data = {}
            text_content = None
            vector_content = None
            for col in row[1]:
                key = col[0]
                val = col[1]
                if key == text_field:
                    text_content = val
                    continue
                if key == embedding_field:
                    vector_content = json.loads(val)
                    continue
                meta_data[key] = val
            document = Document(document_id=document_id, tenant_id=tenant_id, text=text_content, embedding=vector_content, metadata=meta_data)
            doc_hit = DocumentHit(document=document)
            if score is not None and math.isnan(score) == False:
                doc_hit.score = score
            hits.append(doc_hit)
        return hits, next_token
    
    @staticmethod
    def wait_search_index_ready(tablestore_client: tablestore.OTSClient,
                                table_name: str,
                                index_name: str,
                                total_count: int
                                ) -> None:
        max_wait_time = 300
        interval_time = 1
        start_time = time.time()
        while max_wait_time > 0:
            search_response = tablestore_client.search(
                table_name=table_name,
                index_name=index_name,
                search_query=tablestore.SearchQuery(tablestore.MatchAllQuery(), limit=0, get_total_count=True),
                columns_to_get=tablestore.ColumnsToGet(return_type=tablestore.ColumnReturnType.NONE),
            )
            if search_response.total_count == total_count:
                print(f'table:[{table_name}] search index:[{index_name}] ready! use time:{time.time() - start_time}')
                return
            time.sleep(interval_time)
            max_wait_time = max_wait_time - interval_time
        assert False, f'table:[{table_name}] search index:[{index_name}] is not ready!! use time:{time.time() - start_time}'
