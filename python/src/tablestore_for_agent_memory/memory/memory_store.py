import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import tablestore
from pydantic import Field, validate_call
from tablestore import Row

from tablestore_for_agent_memory.base.common import MetaType, Order, microseconds_timestamp
from tablestore_for_agent_memory.base.filter import Filter
from tablestore_for_agent_memory.base.base_memory_store import (
    BaseMemoryStore,
    Message,
    Session,
)
from tablestore_for_agent_memory.util.ots import (
    GetRangeIterator,
    create_secondary_index,
    create_table,
    delete_table,
    meta_data_to_ots_columns,
    row_to_message,
    row_to_session, row_to_message_create_time, batch_delete,
)

logger = logging.getLogger(__name__)


class MemoryStore(BaseMemoryStore):

    def __init__(
            self,
            endpoint: Optional[str] = None,
            instance_name: Optional[str] = None,
            access_key_id: Optional[str] = None,
            access_key_secret: Optional[str] = None,
            session_table_name: Optional[str] = "session",
            session_secondary_index_name: Optional[str] = "session_secondary_index",
            session_secondary_index_meta: Optional[Dict[str, MetaType]] = None,
            message_table_name: Optional[str] = "message",
            message_search_index_name: Optional[str] = "message_search_index",
            message_secondary_index_name: Optional[str] = "message_secondary_index",
            **kwargs: Any,
    ):
        super().__init__()
        self._session_table_name = session_table_name
        self._session_secondary_index_name = session_secondary_index_name
        self._session_secondary_index_meta = session_secondary_index_meta
        self._session_secondary_index_meta["update_time"] = MetaType.INTEGER
        self._message_table_name = message_table_name
        self._message_search_index_name = message_search_index_name
        self._message_secondary_index_name = message_secondary_index_name
        self._client = tablestore.OTSClient(
            endpoint,
            access_key_id,
            access_key_secret,
            instance_name,
            retry_policy=tablestore.WriteRetryPolicy(),
            **kwargs,
        )

    def init_table(self) -> None:
        """
        初始化表
        """
        self._create_session_table()
        self._create_session_secondary_index()
        self._create_message_table()
        self._create_message_secondary_index()
        time.sleep(1)

    def put_session(self, session: Session) -> None:
        primary_key = [("user_id", session.user_id), ("session_id", session.session_id)]
        attribute_columns = meta_data_to_ots_columns(session.metadata)
        attribute_columns.append(("update_time", session.update_time))
        row = tablestore.Row(primary_key, attribute_columns)
        self._client.put_row(self._session_table_name, row)

    def update_session(self, session: Session) -> None:
        primary_key = [("user_id", session.user_id), ("session_id", session.session_id)]
        attribute_columns = meta_data_to_ots_columns(session.metadata)
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
        batch_delete(self._client, self._session_table_name, iterator)

    def delete_all_sessions(self) -> None:
        iterator = self.list_all_sessions()
        batch_delete(self._client, self._session_table_name, iterator)

    def get_session(self, user_id: str, session_id: str) -> Optional[Session]:
        primary_key = [("user_id", user_id), ("session_id", session_id)]
        _, row, _ = self._client.get_row(self._session_table_name, primary_key, None, None, 1)
        session = row_to_session(row)
        return session

    def list_all_sessions(self) -> Iterator[Session]:
        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_table_name,
            translate_function=row_to_session,
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
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
    ) -> Iterator[Session]:
        batch_size = self._config_batch_size(batch_size,max_count,metadata_filter)
        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_table_name,
            translate_function=row_to_session,
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
            inclusive_end_update_time: Optional[int] = None,
            metadata_filter: Optional[Filter] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
    ) -> Iterator[Session]:
        batch_size = self._config_batch_size(batch_size, max_count, metadata_filter)
        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._session_secondary_index_name,
            translate_function=row_to_session,
            inclusive_start_primary_key=[
                ("user_id", user_id),
                ("update_time", tablestore.INF_MAX),
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

    def search_sessions(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Session]:
        pass

    def put_message(self, message: Message) -> None:
        if message.create_time is None:
            message.create_time = microseconds_timestamp()
        primary_key = [
            ("session_id", message.session_id),
            ("create_time", message.create_time),
            ("message_id", message.message_id),
        ]
        attribute_columns = meta_data_to_ots_columns(message.metadata)
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
        batch_delete(self._client, self._message_table_name, iterator)

    def delete_all_messages(self) -> None:
        iterator = self.get_all_messages()
        batch_delete(self._client, self._message_table_name, iterator)

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
        attribute_columns = meta_data_to_ots_columns(message.metadata)
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
        message = row_to_message(row)
        return message

    def get_all_messages(self) -> Iterator[Message]:
        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_table_name,
            translate_function=row_to_message,
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
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
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

        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_table_name,
            translate_function=row_to_message,
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

    def search_messages(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Message]:
        pass

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
        create_table(
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
        create_secondary_index(
            self._client,
            self._session_table_name,
            self._session_secondary_index_name,
            primary_key_for_session_secondary_index,
            session_defined_columns_for_secondary_index,
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
        create_table(self._client, self._message_table_name, primary_key_for_message_table)

    def _create_message_secondary_index(self) -> None:
        """
        创建 Message 表的二级索引，方便获取 create_time 主键
        """
        primary_key_for_message_secondary_index = [
            "session_id",
            "message_id",
            "create_time",
        ]
        create_secondary_index(
            self._client,
            self._message_table_name,
            self._message_secondary_index_name,
            primary_key_for_message_secondary_index,
            [],
        )

    def _delete_table(self) -> None:
        delete_table(self._client, self._session_table_name)
        delete_table(self._client, self._message_table_name)

    def _get_message_create_time_from_secondary_index(self, session_id: str, message_id: str) -> Optional[int]:
        iterator = GetRangeIterator(
            tablestore_client=self._client,
            table_name=self._message_secondary_index_name,
            translate_function=row_to_message_create_time,
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
    def _config_batch_size(batch_size:Optional[int], max_count:Optional[int], metadata_filter:Optional[Filter]) -> Optional[int]:
        if batch_size is None and max_count is not None:
            if metadata_filter is None:
                return min(5000, max_count)
            else:
                return min(5000, int(max_count * 1.3))
        return batch_size