import base64
import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import tablestore
from pydantic import Field, validate_call
from tablestore import Row

from tablestore_for_agent_memory.base.base_memory_store import (
    BaseMemoryStore,
    Message,
    Session,
)
from tablestore_for_agent_memory.base.common import MetaType, Order, microseconds_timestamp
from tablestore_for_agent_memory.base.filter import Filter
from tablestore_for_agent_memory.util.tablestore_helper import TablestoreHelper

logger = logging.getLogger(__name__)


# noinspection DuplicatedCode
class MemoryStore(BaseMemoryStore):

    def __init__(
            self,
            tablestore_client: tablestore.OTSClient,
            session_table_name: Optional[str] = "session",
            session_secondary_index_name: Optional[str] = "session_secondary_index",
            session_secondary_index_meta: Optional[Dict[str, MetaType]] = None,
            session_search_index_name: Optional[str] = "session_search_index_name",
            session_search_index_schema: Optional[List[tablestore.FieldSchema]] = None,
            message_table_name: Optional[str] = "message",
            message_search_index_name: Optional[str] = "message_search_index",
            message_secondary_index_name: Optional[str] = "message_secondary_index",
            message_search_index_schema: Optional[List[tablestore.FieldSchema]] = None,
            **kwargs: Any,
    ):
        super().__init__()
        self._session_table_name = session_table_name
        self._session_secondary_index_name = session_secondary_index_name
        self._session_secondary_index_meta = session_secondary_index_meta
        self._session_secondary_index_meta["update_time"] = MetaType.INTEGER
        self._session_search_index_name = session_search_index_name
        self._session_search_index_schema = session_search_index_schema
        self._message_table_name = message_table_name
        self._message_secondary_index_name = message_secondary_index_name
        self._message_search_index_name = message_search_index_name
        self._message_search_index_schema = message_search_index_schema
        self._client = tablestore_client

    def init_table(self) -> None:
        """
        初始化表
        """
        self._create_session_table()
        self._create_session_secondary_index()
        self._create_message_table()
        self._create_message_secondary_index()
        time.sleep(1)

    def init_search_index(self) -> None:
        """
        初始化多元索引
        """
        self._create_session_search_index()
        self._create_message_search_index()

    def put_session(self, session: Session) -> None:
        primary_key = [("user_id", session.user_id), ("session_id", session.session_id)]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(session.metadata)
        attribute_columns.append(("update_time", session.update_time))
        row = tablestore.Row(primary_key, attribute_columns)
        self._client.put_row(self._session_table_name, row)

    def update_session(self, session: Session) -> None:
        primary_key = [("user_id", session.user_id), ("session_id", session.session_id)]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(session.metadata)
        attribute_columns.append(("update_time", session.update_time))
        update_of_attribute_columns = {
            "put": attribute_columns,
        }
        row = tablestore.Row(primary_key, update_of_attribute_columns)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.update_row(self._session_table_name, row, condition)

    def delete_session(self, user_id: str, session_id: str) -> None:
        primary_key = [("user_id", user_id), ("session_id", session_id)]
        row = Row(primary_key)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.delete_row(self._session_table_name, row, condition)

    def delete_sessions(self, user_id: str) -> None:
        iterator = self.list_sessions(user_id=user_id)
        TablestoreHelper.batch_delete(self._client, self._session_table_name, iterator)

    def delete_all_sessions(self) -> None:
        iterator = self.list_all_sessions()
        TablestoreHelper.batch_delete(self._client, self._session_table_name, iterator)

    def get_session(self, user_id: str, session_id: str) -> Optional[Session]:
        primary_key = [("user_id", user_id), ("session_id", session_id)]
        _, row, _ = self._client.get_row(self._session_table_name, primary_key, None, None, 1)
        session = TablestoreHelper.row_to_session(row)
        return session

    def delete_session_and_messages(self, user_id: str, session_id: str) -> None:
        self.delete_session(user_id=user_id, session_id=session_id)
        self.delete_messages(session_id=session_id)

    def list_all_sessions(self) -> Iterator[Session]:
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_table_name,
            translate_function=TablestoreHelper.row_to_session,
            inclusive_start_primary_key=[
                ("user_id", tablestore.INF_MIN),
                ("session_id", tablestore.INF_MIN),
            ],
            exclusive_end_primary_key=[
                ("user_id", tablestore.INF_MAX),
                ("session_id", tablestore.INF_MAX),
            ],
            order=Order.ASC,
        )
        return iterator

    @validate_call
    def list_sessions(
            self,
            user_id: str,
            metadata_filter: Optional[Filter] = None,
            max_count: Optional[int] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> Iterator[Session]:
        batch_size = self._config_batch_size(batch_size, max_count, metadata_filter)
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_table_name,
            translate_function=TablestoreHelper.row_to_session,
            inclusive_start_primary_key=[
                ("user_id", user_id),
                ("session_id", tablestore.INF_MIN),
            ],
            exclusive_end_primary_key=[
                ("user_id", user_id),
                ("session_id", tablestore.INF_MAX),
            ],
            metadata_filter=metadata_filter,
            order=Order.ASC,
            batch_size=batch_size,
            max_count=max_count,
        )
        return iterator

    @validate_call
    def list_recent_sessions(
            self,
            user_id: str,
            inclusive_start_update_time: Optional[int] = None,
            inclusive_end_update_time: Optional[int] = None,
            metadata_filter: Optional[Filter] = None,
            max_count: Optional[int] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> Iterator[Session]:
        batch_size = self._config_batch_size(batch_size, max_count, metadata_filter)
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_secondary_index_name,
            translate_function=TablestoreHelper.row_to_session,
            inclusive_start_primary_key=[
                ("user_id", user_id),
                (
                    "update_time",
                    tablestore.INF_MAX if inclusive_start_update_time is None else inclusive_start_update_time,
                ),

                ("session_id", tablestore.INF_MAX),
            ],
            exclusive_end_primary_key=[
                ("user_id", user_id),
                (
                    "update_time",
                    tablestore.INF_MIN if inclusive_end_update_time is None else inclusive_end_update_time,
                ),
                ("session_id", tablestore.INF_MIN),

            ],
            metadata_filter=metadata_filter,
            order=Order.DESC,
            batch_size=batch_size,
            max_count=max_count,
        )
        return iterator

    @validate_call
    def list_recent_sessions_paginated(
            self,
            user_id: str,
            page_size: int = 100,
            next_token: Optional[str] = None,
            inclusive_start_update_time: Optional[int] = None,
            inclusive_end_update_time: Optional[int] = None,
            metadata_filter: Optional[Filter] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> (List[Session], Optional[str]):
        batch_size = self._config_batch_size(batch_size, page_size, metadata_filter)
        if next_token is None:
            inclusive_start_primary_key = [
                ("user_id", user_id),
                (
                    "update_time",
                    tablestore.INF_MAX if inclusive_start_update_time is None else inclusive_start_update_time,
                ),

                ("session_id", tablestore.INF_MAX),
            ]
        else:
            inclusive_start_primary_key = TablestoreHelper.decode_next_primary_key_token(next_token)
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_secondary_index_name,
            translate_function=TablestoreHelper.row_to_session,
            inclusive_start_primary_key=inclusive_start_primary_key,
            exclusive_end_primary_key=[
                ("user_id", user_id),
                (
                    "update_time",
                    tablestore.INF_MIN if inclusive_end_update_time is None else inclusive_end_update_time,
                ),
                ("session_id", tablestore.INF_MIN),

            ],
            metadata_filter=metadata_filter,
            order=Order.DESC,
            batch_size=batch_size,
            max_count=page_size,
        )
        sessions = list(iterator)
        next_primary_key = iterator.next_start_primary_key()
        next_token = None if next_primary_key is None else TablestoreHelper.encode_next_primary_key_token(next_primary_key)
        return sessions, next_token

    @validate_call
    def search_sessions(self,
                        metadata_filter: Optional[Filter] = None,
                        limit: Optional[int] = Field(default=100, le=100, ge=1),
                        next_token: Optional[str] = None
                        ) -> (List[Session], Optional[str]):
        ots_query, need_score_sort = TablestoreHelper.paser_search_index_filters(metadata_filter=metadata_filter)
        sort = tablestore.Sort(sorters=[tablestore.ScoreSort(sort_order=tablestore.SortOrder.DESC)]) if need_score_sort else None
        if next_token:
            next_token = base64.b64decode(next_token.encode('utf-8'))
        search_query = tablestore.SearchQuery(
            ots_query, limit=limit, get_total_count=False, sort=sort, next_token=next_token
        )
        try:
            search_response = self._client.search(
                table_name=self._session_table_name,
                index_name=self._session_search_index_name,
                search_query=search_query,
                columns_to_get=tablestore.ColumnsToGet(
                    return_type=tablestore.ColumnReturnType.ALL
                ),
            )
        except tablestore.OTSClientError as e:
            logger.exception("tablestore search session failed with client error:%s", e)
            raise e
        except tablestore.OTSServiceError as e:
            logger.exception(
                "tablestore search session failed with error:%s, http_status:%d, error_code:%s, error_message:%s, request_id:%s",
                e,
                e.get_http_status(),
                e.get_error_code(),
                e.get_error_message(),
                e.get_request_id(),
            )
            raise e
        sessions, next_token = TablestoreHelper.search_response_to_sessions(search_response=search_response)
        logger.info(f"tablestore search session index successfully. request_id:[{search_response.request_id}], metadata_filter:[{metadata_filter}], limit:[{limit}], next_token:[{next_token}]")
        return sessions, next_token

    def put_message(self, message: Message) -> None:
        if message.create_time is None:
            message.create_time = microseconds_timestamp()
        primary_key = [
            ("session_id", message.session_id),
            ("create_time", message.create_time),
            ("message_id", message.message_id),
        ]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(message.metadata)
        if message.content:
            attribute_columns.append(("content", message.content))
        row = tablestore.Row(primary_key, attribute_columns)
        self._client.put_row(self._message_table_name, row)

    def delete_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> None:
        if not create_time:
            create_time = self._get_message_create_time_from_secondary_index(session_id, message_id)
            if not create_time:
                return None
        primary_key = [
            ("session_id", session_id),
            ("create_time", create_time),
            ("message_id", message_id),
        ]
        row = Row(primary_key)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.delete_row(self._message_table_name, row, condition)

    def delete_messages(self, session_id: str) -> None:
        iterator = self.get_messages(session_id=session_id)
        TablestoreHelper.batch_delete(self._client, self._message_table_name, iterator)

    def delete_all_messages(self) -> None:
        iterator = self.get_all_messages()
        TablestoreHelper.batch_delete(self._client, self._message_table_name, iterator)

    def update_message(self, message: Message) -> None:
        if not message.create_time:
            create_time = self._get_message_create_time_from_secondary_index(message.session_id, message.message_id)
            if create_time is not None:
                message.create_time = create_time
            else:
                message.create_time = microseconds_timestamp()
        primary_key = [
            ("session_id", message.session_id),
            ("create_time", message.create_time),
            ("message_id", message.message_id),
        ]
        attribute_columns = TablestoreHelper.meta_data_to_ots_columns(message.metadata)
        if message.content:
            attribute_columns.append(("content", message.content))
        update_of_attribute_columns = {
            "put": attribute_columns,
        }
        row = tablestore.Row(primary_key, update_of_attribute_columns)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        self._client.update_row(self._message_table_name, row, condition)

    def get_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> Optional[Message]:
        if not create_time:
            create_time = self._get_message_create_time_from_secondary_index(session_id, message_id)
            if not create_time:
                return None
        primary_key = [
            ("session_id", session_id),
            ("create_time", create_time),
            ("message_id", message_id),
        ]
        _, row, _ = self._client.get_row(self._message_table_name, primary_key, None, None, 1)
        message = TablestoreHelper.row_to_message(row)
        return message

    def get_all_messages(self) -> Iterator[Message]:
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_table_name,
            translate_function=TablestoreHelper.row_to_message,
            inclusive_start_primary_key=[
                ("session_id", tablestore.INF_MIN),
                ("create_time", tablestore.INF_MIN),
                ("message_id", tablestore.INF_MIN),
            ],
            exclusive_end_primary_key=[
                ("session_id", tablestore.INF_MAX),
                ("create_time", tablestore.INF_MAX),
                ("message_id", tablestore.INF_MAX),
            ],
            order=Order.ASC,
        )
        return iterator

    @validate_call
    def get_messages(
            self,
            session_id: str,
            inclusive_start_create_time: Optional[int] = None,
            inclusive_end_create_time: Optional[int] = None,
            order: Optional[Order] = None,
            metadata_filter: Optional[Filter] = None,
            max_count: Optional[int] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> Iterator[Message]:
        batch_size = self._config_batch_size(batch_size, max_count, metadata_filter)
        if inclusive_start_create_time is not None or inclusive_end_create_time is not None:
            if order is None:
                raise ValueError(f"order is required when inclusive_start_create_time or inclusive_end_create_time is specified")
        else:
            order = Order.DESC
        if order == order.ASC:
            const_min = tablestore.INF_MIN
            const_max = tablestore.INF_MAX
        else:
            const_min = tablestore.INF_MAX
            const_max = tablestore.INF_MIN

        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_table_name,
            translate_function=TablestoreHelper.row_to_message,
            inclusive_start_primary_key=[
                ("session_id", session_id),
                (
                    "create_time",
                    const_min if inclusive_start_create_time is None else inclusive_start_create_time,
                ),
                ("message_id", const_min),
            ],
            exclusive_end_primary_key=[
                ("session_id", session_id),
                (
                    "create_time",
                    const_max if inclusive_end_create_time is None else inclusive_end_create_time,
                ),
                ("message_id", const_max),
            ],
            metadata_filter=metadata_filter,
            order=order,
            batch_size=batch_size,
            max_count=max_count,
        )
        return iterator

    @validate_call
    def search_messages(self,
                        metadata_filter: Optional[Filter] = None,
                        limit: Optional[int] = Field(default=100, le=100, ge=1),
                        next_token: Optional[str] = None
                        ) -> (List[Message], Optional[str]):
        ots_query, need_score_sort = TablestoreHelper.paser_search_index_filters(metadata_filter=metadata_filter)
        sort = tablestore.Sort(sorters=[tablestore.ScoreSort(sort_order=tablestore.SortOrder.DESC)]) if need_score_sort else None
        if next_token:
            next_token = base64.b64decode(next_token.encode('utf-8'))
        search_query = tablestore.SearchQuery(
            ots_query, limit=limit, get_total_count=False, sort=sort, next_token=next_token
        )
        try:
            search_response = self._client.search(
                table_name=self._message_table_name,
                index_name=self._message_search_index_name,
                search_query=search_query,
                columns_to_get=tablestore.ColumnsToGet(
                    return_type=tablestore.ColumnReturnType.ALL
                ),
            )
        except tablestore.OTSClientError as e:
            logger.exception("tablestore search message failed with client error:%s", e)
            raise e
        except tablestore.OTSServiceError as e:
            logger.exception(
                "tablestore search message failed with error:%s, http_status:%d, error_code:%s, error_message:%s, request_id:%s",
                e,
                e.get_http_status(),
                e.get_error_code(),
                e.get_error_message(),
                e.get_request_id(),
            )
            raise e
        messages, next_token = TablestoreHelper.search_response_to_message(search_response=search_response)
        logger.info(f"tablestore search message index successfully. request_id:[{search_response.request_id}], metadata_filter:[{metadata_filter}], limit:[{limit}], next_token:[{next_token}]")
        return messages, next_token

    def _create_session_table(self) -> None:
        """ 
        创建 Session 表
        """
        primary_key_for_session_table = [
            ("user_id", MetaType.STRING),
            ("session_id", MetaType.STRING),
        ]
        defined_columns = [
            (key, self._session_secondary_index_meta[key]) for key in self._session_secondary_index_meta
        ]
        TablestoreHelper.create_table(
            self._client,
            self._session_table_name,
            primary_key_for_session_table,
            defined_columns,
        )

    def _create_session_secondary_index(self) -> None:
        """
        创建 Session 表的二级索引，方便根据 update_time 展示最近活跃的 Sessions
        """
        primary_key_for_session_secondary_index = [
            "user_id",
            "update_time",
            "session_id",
        ]
        defined_columns = [
            (key, self._session_secondary_index_meta[key]) for key in self._session_secondary_index_meta
        ]
        session_defined_columns_for_secondary_index = []
        for defined_column in defined_columns:
            if defined_column[0] != "update_time":
                session_defined_columns_for_secondary_index.append(defined_column[0])
        TablestoreHelper.create_secondary_index(
            self._client,
            self._session_table_name,
            self._session_secondary_index_name,
            primary_key_for_session_secondary_index,
            session_defined_columns_for_secondary_index,
        )

    def _create_session_search_index(self):
        """Create session search index if not exist."""
        if self._session_search_index_schema is None:
            logger.warning("skip create session search index because session_search_index_schema is empty")
            return
        self._session_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("user_id", tablestore.FieldType.KEYWORD),
            self._session_search_index_schema
        )
        self._session_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("session_id", tablestore.FieldType.KEYWORD),
            self._session_search_index_schema
        )
        self._session_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("update_time", tablestore.FieldType.LONG),
            self._session_search_index_schema
        )
        TablestoreHelper.create_search_index_if_not_exist(
            tablestore_client=self._client,
            table_name=self._session_table_name,
            index_name=self._session_search_index_name,
            index_schemas=self._session_search_index_schema,
        )

    def _create_message_search_index(self):
        """Create message search index if not exist."""
        if self._message_search_index_schema is None:
            logger.warning("skip create message search index because message_search_index_schema is empty")
            return
        self._message_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("session_id", tablestore.FieldType.KEYWORD),
            self._message_search_index_schema
        )
        self._message_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("message_id", tablestore.FieldType.KEYWORD),
            self._message_search_index_schema
        )
        self._message_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("create_time", tablestore.FieldType.LONG),
            self._message_search_index_schema
        )
        self._message_search_index_schema = TablestoreHelper.add_schema(
            tablestore.FieldSchema("content", tablestore.FieldType.TEXT, analyzer=tablestore.AnalyzerType.MAXWORD),
            self._message_search_index_schema
        )
        TablestoreHelper.create_search_index_if_not_exist(
            tablestore_client=self._client,
            table_name=self._message_table_name,
            index_name=self._message_search_index_name,
            index_schemas=self._message_search_index_schema,
        )

    def _create_message_table(self) -> None:
        """
        创建 Message 表
        """
        primary_key_for_message_table = [
            ("session_id", MetaType.STRING),
            ("create_time", MetaType.INTEGER),
            ("message_id", MetaType.STRING),
        ]
        TablestoreHelper.create_table(self._client, self._message_table_name, primary_key_for_message_table)

    def _create_message_secondary_index(self) -> None:
        """
        创建 Message 表的二级索引，方便获取 create_time 主键
        """
        primary_key_for_message_secondary_index = [
            "session_id",
            "message_id",
            "create_time",
        ]
        TablestoreHelper.create_secondary_index(
            self._client,
            self._message_table_name,
            self._message_secondary_index_name,
            primary_key_for_message_secondary_index,
            [],
        )

    def _delete_table(self) -> None:
        TablestoreHelper.delete_table(self._client, self._session_table_name)
        TablestoreHelper.delete_table(self._client, self._message_table_name)

    def _get_message_create_time_from_secondary_index(self, session_id: str, message_id: str) -> Optional[int]:
        iterator = TablestoreHelper.GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_secondary_index_name,
            translate_function=TablestoreHelper.row_to_message_create_time,
            inclusive_start_primary_key=[
                ("session_id", session_id),
                ("message_id", message_id),
                ("create_time", tablestore.INF_MIN),
            ],
            exclusive_end_primary_key=[
                ("session_id", session_id),
                ("message_id", message_id),
                ("create_time", tablestore.INF_MAX),
            ],
            order=Order.ASC,
        )
        create_time_list = list(iterator)
        return create_time_list[0] if len(create_time_list) != 0 else None

    @staticmethod
    def _config_batch_size(batch_size: Optional[int], max_count: Optional[int], metadata_filter: Optional[Filter]) -> Optional[int]:
        if batch_size is None and max_count is not None:
            if metadata_filter is None:
                return min(5000, max_count)
            else:
                return min(5000, int(max_count * 1.3))
        return batch_size
