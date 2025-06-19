import base64
import json
import logging
import time
from typing import Any, Iterator, List, Optional, Union, Tuple

import tablestore
from pydantic import Field, validate_call
from tablestore import Row, BatchGetRowRequest, TableInBatchGetRowItem

from tablestore_for_agent_memory.base.base_knowledge_store import BaseKnowledgeStore, Document, DOCUMENT_DEFAULT_TENANT_ID, DocumentHit
from tablestore_for_agent_memory.base.common import MetaType, Order, Response
from tablestore_for_agent_memory.base.filter import Filter, Filters
from tablestore_for_agent_memory.util.tablestore_helper import TablestoreHelper

logger = logging.getLogger(__name__)


# noinspection DuplicatedCode
class KnowledgeStore(BaseKnowledgeStore):

    def __init__(
            self,
            tablestore_client: tablestore.OTSClient,
            vector_dimension: int,
            enable_multi_tenant: Optional[bool] = False,
            table_name: Optional[str] = "knowledge",
            search_index_name: Optional[str] = "knowledge_search_index_name",
            search_index_schema: Optional[List[tablestore.FieldSchema]] = None,
            text_field: Optional[str] = "text",
            embedding_field: Optional[str] = "embedding",
            vector_metric_type: tablestore.VectorMetricType = tablestore.VectorMetricType.VM_COSINE,
            **kwargs: Any,
    ):
        super().__init__()
        self._client = tablestore_client
        self._vector_dimension = vector_dimension
        self._enable_multi_tenant = enable_multi_tenant
        self._vector_metric_type = vector_metric_type
        self._table_name = table_name
        self._search_index_name = search_index_name
        self._search_index_schema = search_index_schema
        self._text_field = text_field
        self._embedding_field = embedding_field
        default_meta_data_to_get = ["document_id", "tenant_id", text_field]
        for schema in search_index_schema:
            # By default, vectors are not returned
            if schema.field_type is not tablestore.FieldType.VECTOR:
                default_meta_data_to_get.append(schema.field_name)
        self._default_meta_data_to_get = default_meta_data_to_get

    def init_table(self) -> None:
        """
        Initialize table
        """
        self._create_table()
        self._create_search_index()
        time.sleep(1)

    def put_document(self, document: Document) -> None:
        self._check_vector_dimension(document)
        self._check_enable_multi_tenant(document)
        primary_key = [("document_id", document.document_id), ("tenant_id", document.tenant_id)]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(document.metadata)
        if document.text:
            attribute_columns.append((self._text_field, document.text))
        if document.embedding:
            attribute_columns.append((self._embedding_field, json.dumps(document.embedding)))
        row = tablestore.Row(primary_key, attribute_columns)
        self._client.put_row(self._table_name, row)

    def update_document(self, document: Document) -> None:
        self._check_vector_dimension(document)
        self._check_enable_multi_tenant(document)
        primary_key = [("document_id", document.document_id), ("tenant_id", document.tenant_id)]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(document.metadata)
        if document.text:
            attribute_columns.append((self._text_field, document.text))
        if document.embedding:
            attribute_columns.append((self._embedding_field, json.dumps(document.embedding)))
        update_of_attribute_columns = {
            "put": attribute_columns,
        }
        row = tablestore.Row(primary_key, update_of_attribute_columns)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.update_row(self._table_name, row, condition)

    def delete_document(self, document_id: str, tenant_id: Optional[str] = None) -> None:
        tenant_id = self._check_enable_multi_tenant_id(tenant_id)
        primary_key = [("document_id", document_id), ("tenant_id", tenant_id)]
        row = Row(primary_key)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.delete_row(self._table_name, row, condition)

    def delete_document_by_tenant(self, tenant_id: str) -> None:
        next_token = None
        while True:
            response = self.search_documents(
                tenant_id=tenant_id,
                limit=1000,
                meta_data_to_get=["document_id", "tenant_id"],
                next_token=next_token,
            )
            document_hits, next_token = (response.hits, response.next_token)
            documents = [doc.document for doc in document_hits]
            TablestoreHelper.batch_delete(tablestore_client=self._client, table_name=self._table_name, iterator=iter(documents))
            if next_token is None:
                break

    def delete_all_documents(self) -> None:
        iterator = self.get_all_documents()
        TablestoreHelper.batch_delete(self._client, self._table_name, iterator)

    def get_document(self, document_id: str, tenant_id: Optional[str] = None) -> Optional[Document]:
        tenant_id = self._check_enable_multi_tenant_id(tenant_id)
        primary_key = [("document_id", document_id), ("tenant_id", tenant_id)]
        _, row, _ = self._client.get_row(self._table_name, primary_key, None, None, 1)
        document = TablestoreHelper.row_to_document(row, self._text_field, self._embedding_field)
        return document

    def get_documents(self, document_id_list: List[str], tenant_id: Optional[str] = None) -> List[Optional[Document]]:
        if len(document_id_list) is None:
            return []
        tenant_id = self._check_enable_multi_tenant_id(tenant_id)
        documents = []
        total = len(document_id_list)
        batch_size = 100
        for start in range(0, total, batch_size):
            end = start + batch_size
            current_batch = document_id_list[start:end]
            rows_to_get = []
            for document_id in current_batch:
                primary_key = [('document_id', document_id), ('tenant_id', tenant_id)]
                rows_to_get.append(primary_key)
            request = BatchGetRowRequest()
            request.add(TableInBatchGetRowItem(self._table_name, rows_to_get, None, None, 1))
            result = self._client.batch_get_row(request)
            table_result = result.get_result_by_table(self._table_name)
            for i in range(len(table_result)):
                item = table_result[i]
                if item.is_ok:
                    document = TablestoreHelper.row_to_document(item.row, self._text_field, self._embedding_field)
                    documents.append(document)
                else:
                    raise ValueError(f"read failed for row:{rows_to_get[i]}, error code: {item.error_code}, error message: {item.error_message}")
        return documents

    def get_all_documents(self) -> Iterator[Document]:
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._table_name,
            translate_function=lambda row: TablestoreHelper.row_to_document(row, self._text_field, self._embedding_field),
            inclusive_start_primary_key=[
                ("document_id", tablestore.INF_MIN),
                ("tenant_id", tablestore.INF_MIN),
            ],
            exclusive_end_primary_key=[
                ("document_id", tablestore.INF_MAX),
                ("tenant_id", tablestore.INF_MAX),
            ],
            order=Order.ASC,
        )
        return iterator

    @validate_call
    def search_documents(self,
                         tenant_id: Optional[Union[List[str], str]] = None,
                         metadata_filter: Optional[Filter] = None,
                         limit: Optional[int] = Field(default=100, le=1000, ge=1),
                         next_token: Optional[str] = None,
                         meta_data_to_get: Optional[List[str]] = None,
                         **kwargs: Any,
                         ) -> Response[DocumentHit]:
        if meta_data_to_get is None:
            meta_data_to_get = self._default_meta_data_to_get
        metadata_filter = self._wrap_tenant_id_filter(tenant_id=tenant_id, metadata_filter=metadata_filter)
        ots_query, need_score_sort = TablestoreHelper.paser_search_index_filters(metadata_filter=metadata_filter)
        sort = tablestore.Sort(sorters=[tablestore.ScoreSort(sort_order=tablestore.SortOrder.DESC)]) if need_score_sort else None
        if next_token:
            next_token = base64.b64decode(next_token.encode('utf-8'))
        search_query = tablestore.SearchQuery(
            query=ots_query, limit=limit, get_total_count=False, sort=sort, next_token=next_token
        )
        routing_keys = kwargs.get("routing_keys")
        if routing_keys is None:
            routing_keys = self._build_routing_keys(tenant_id=tenant_id)
        try:
            search_response = self._client.search(
                table_name=self._table_name,
                index_name=self._search_index_name,
                search_query=search_query,
                columns_to_get=tablestore.ColumnsToGet(
                    return_type=tablestore.ColumnReturnType.SPECIFIED,
                    column_names=meta_data_to_get,
                ),
                routing_keys=routing_keys,
            )
        except tablestore.OTSClientError as e:
            logger.exception("tablestore search document failed with client error:%s", e)
            raise e
        except tablestore.OTSServiceError as e:
            logger.exception(
                "tablestore search document failed with error:%s, http_status:%d, error_code:%s, error_message:%s, request_id:%s",
                e,
                e.get_http_status(),
                e.get_error_code(),
                e.get_error_message(),
                e.get_request_id(),
            )
            raise e
        hits, next_token = TablestoreHelper.search_response_to_document(search_response=search_response, text_field=self._text_field, embedding_field=self._embedding_field)
        logger.info(f"tablestore search document index successfully. request_id:[{search_response.request_id}], metadata_filter:[{metadata_filter}], limit:[{limit}], next_token:[{next_token}]")
        response: Response[DocumentHit] = Response(hits=hits, next_token=next_token)
        return response

    @validate_call
    def full_text_search(self,
                         query: str,
                         tenant_id: Optional[Union[List[str], str]] = None,
                         metadata_filter: Optional[Filter] = None,
                         limit: Optional[int] = Field(default=100, le=1000, ge=1),
                         next_token: Optional[str] = None,
                         meta_data_to_get: Optional[List[str]] = None,
                         **kwargs: Any,
                         ) -> Response[DocumentHit]:
        text_filter = Filters.text_match(self._text_field, query)
        if metadata_filter is not None:
            metadata_filter = Filters.logical_and([text_filter, metadata_filter])
        else:
            metadata_filter = text_filter
        return self.search_documents(
            tenant_id=tenant_id,
            metadata_filter=metadata_filter,
            limit=limit,
            next_token=next_token,
            meta_data_to_get=meta_data_to_get,
        )

    @validate_call
    def vector_search(self,
                      query_vector: List[float],
                      top_k: Optional[int] = 10,
                      tenant_id: Optional[Union[List[str], str]] = None,
                      metadata_filter: Optional[Filter] = None,
                      limit: Optional[int] = Field(default=None, le=1000, ge=1),
                      next_token: Optional[str] = None,
                      meta_data_to_get: Optional[List[str]] = None,
                      **kwargs: Any,
                      ) -> Response[DocumentHit]:
        if limit is None:
            limit = top_k
        metadata_filter = self._wrap_tenant_id_filter(tenant_id=tenant_id, metadata_filter=metadata_filter)
        vector_filter = Filters.vector_query(
            vector_field=self._embedding_field,
            query_vector=query_vector,
            top_k=top_k,
            metadata_filter=metadata_filter,
        )
        routing_keys = self._build_routing_keys(tenant_id=tenant_id)
        return self.search_documents(
            tenant_id=None,
            metadata_filter=vector_filter,
            limit=limit,
            next_token=next_token,
            meta_data_to_get=meta_data_to_get,
            routing_keys=routing_keys,
        )

    def _create_table(self) -> None:
        """ 
        Create table
        """
        primary_key = [
            ("document_id", MetaType.STRING),
            ("tenant_id", MetaType.STRING),
        ]
        TablestoreHelper.create_table(
            self._client,
            self._table_name,
            primary_key,
        )

    def _create_search_index(self):
        """Create search index if not exist."""
        self._search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("document_id", tablestore.FieldType.KEYWORD),
            self._search_index_schema
        )
        self._search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("tenant_id", tablestore.FieldType.KEYWORD),
            self._search_index_schema
        )
        self._search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema(self._embedding_field,
                                   tablestore.FieldType.VECTOR,
                                   vector_options=tablestore.VectorOptions(
                                       data_type=tablestore.VectorDataType.VD_FLOAT_32,
                                       dimension=self._vector_dimension,
                                       metric_type=self._vector_metric_type,
                                   )),
            self._search_index_schema
        )
        self._search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema(self._text_field, tablestore.FieldType.TEXT, analyzer=tablestore.AnalyzerType.MAXWORD),
            self._search_index_schema
        )
        routing_fields = []
        if self._enable_multi_tenant:
            routing_fields = ['tenant_id']
        TablestoreHelper.create_search_index_if_not_exist(
            tablestore_client=self._client,
            table_name=self._table_name,
            index_name=self._search_index_name,
            index_schemas=self._search_index_schema,
            routing_fields=routing_fields,
        )

    def _check_enable_multi_tenant(self, document: Optional[Document] = None):
        if document is None:
            return
        if not self._enable_multi_tenant:
            document.tenant_id = DOCUMENT_DEFAULT_TENANT_ID
            return
        else:
            if document.tenant_id == DOCUMENT_DEFAULT_TENANT_ID or document.tenant_id is None:
                raise ValueError(f"the multi-tenant capability is enabled, but the 'tenant_id' is not set, document detail is:{document}")

    def _check_enable_multi_tenant_id(self, tenant_id: str) -> str:
        if not self._enable_multi_tenant:
            return DOCUMENT_DEFAULT_TENANT_ID
        else:
            if tenant_id == DOCUMENT_DEFAULT_TENANT_ID or tenant_id is None:
                raise ValueError(f"the multi-tenant capability is enabled, but the 'tenant_id' is not set")
        return tenant_id

    def _check_vector_dimension(self, document: Optional[Document] = None) -> None:
        if document is None:
            return
        if document.embedding is None:
            return
        if len(document.embedding) != self._vector_dimension:
            raise ValueError(f"document's embedding embedding length:{len(document.embedding)} is not the same as the knowledge store dimension:{self._vector_dimension}")

    def _delete_table(self) -> None:
        TablestoreHelper.delete_table(self._client, self._table_name)

    def _wrap_tenant_id_filter(self, tenant_id: Optional[Union[List[str], str]] = None, metadata_filter: Optional[Filter] = None) -> Optional[Filter]:
        if self._enable_multi_tenant:
            if tenant_id is None:
                return metadata_filter
            if isinstance(tenant_id, str):
                if tenant_id == DOCUMENT_DEFAULT_TENANT_ID:
                    return metadata_filter
                else:
                    if metadata_filter is None:
                        return Filters.eq("tenant_id", tenant_id)
                    else:
                        return Filters.logical_and([Filters.eq("tenant_id", tenant_id), metadata_filter])
            elif isinstance(tenant_id, List) or isinstance(tenant_id, list):
                if len(tenant_id) == 0:
                    return metadata_filter
                else:
                    if metadata_filter is None:
                        return Filters.In("tenant_id", tenant_id)
                    else:
                        return Filters.logical_and([Filters.In("tenant_id", tenant_id), metadata_filter])
        return metadata_filter

    def _build_routing_keys(self, tenant_id: Optional[Union[List[str], str]] = None) -> Optional[List[List[Tuple[str, str]]]]:
        if self._enable_multi_tenant:
            if tenant_id is None:
                return None
            if isinstance(tenant_id, str):
                return [[('tenant_id', tenant_id)]]
            elif isinstance(tenant_id, List) or isinstance(tenant_id, list):
                if len(tenant_id) == 0:
                    return None
                else:
                    routing_keys = []
                    for _tenant_id in tenant_id:
                        routing_keys.append([('tenant_id', _tenant_id)])
                    return routing_keys
        else:
            return None
