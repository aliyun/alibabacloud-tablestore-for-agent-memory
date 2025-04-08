from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Union

from pydantic import BaseModel, Field, validate_call

from tablestore_for_agent_memory.base.common import MetaType, Order, microseconds_timestamp
from tablestore_for_agent_memory.base.filter import Filter


@dataclass
class Session(ABC):
    user_id: str

    session_id: str

    update_time: int = microseconds_timestamp()

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)


@dataclass
class Message(ABC):
    session_id: str

    message_id: str

    create_time: Optional[int] = field(default=None)

    content: Optional[str] = field(default=None)

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)


class BaseMemoryStore(BaseModel, ABC):

    @abstractmethod
    def put_session(self, session: Session) -> None:
        """
        写入一条Session会话
        :param session:  会话内容
        """
        pass

    @abstractmethod
    def update_session(self, session: Session) -> None:
        """
        更新一条Session会话
        :param session:  会话内容
        """
        pass

    @abstractmethod
    def delete_session(self, user_id: str, session_id: str) -> None:
        """
        删除一条Session会话
        :param user_id: 用户id
        :param session_id: 会话id
        """
        pass

    @abstractmethod
    def delete_sessions(self, user_id: str) -> None:
        """
        删除一个用户的所有Session会话
        :param user_id: 用户id
        """
        pass

    @abstractmethod
    def delete_all_sessions(self) -> None:
        """
        删除所有用户的所有Session会话（注意：高危）
        """
        pass

    @abstractmethod
    def get_session(self, user_id: str, session_id: str) -> Optional[Session]:
        """
        查出一个会话的详细内容
        :param user_id: 用户id
        :param session_id: 会话id
        """
        pass

    @abstractmethod
    def list_all_sessions(self) -> Iterator[Session]:
        """
        列出所有用户的所有会话。
        """
        pass

    def delete_session_and_messages(self, user_id: str, session_id: str) -> None:
        """
        删除一个session和其对应的所有会话。
        """
        pass

    @abstractmethod
    @validate_call
    def list_sessions(
            self,
            user_id: str,
            metadata_filter: Optional[Filter] = None,
            max_count: Optional[int] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> Iterator[Session]:
        """
        列出一个用户的所有会话。
        :param user_id: 用户id，必传参数。
        :param metadata_filter: metadata过滤条件。
        :param batch_size: 内部批量获取参数。
        :param max_count: Iterator中最大的个数。
        """
        pass

    @abstractmethod
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
        """
        列出最近的所有会话信息，根据Session会话更新时间排序。
        :param user_id: 用户id，必传参数。
        :param inclusive_start_update_time: 起始时间.
        :param inclusive_end_update_time: 结束时间.
        :param metadata_filter: metadata过滤条件。
        :param batch_size: 内部批量获取参数。
        :param max_count: Iterator中最大的个数。
        """
        pass
    
    @abstractmethod
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
        """
        使用连续翻页方式，列出最近的所有会话信息，根据Session会话更新时间排序。
        :rtype: (Session信息, 下一次访问的token)
        :param user_id: 用户id，必传参数。
        :param page_size: 返回的Session会话个数
        :param next_token: 下次翻页的token
        :param inclusive_start_update_time: 起始时间.
        :param inclusive_end_update_time: 结束时间.
        :param metadata_filter: metadata过滤条件。
        :param batch_size: 内部批量获取参数。
        """
        pass

    @abstractmethod
    def search_sessions(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Session]:
        """
        搜索Session.
        :param metadata_filter: metadata过滤条件。
        :param limit: 单次返回行数.
        """
        pass

    @abstractmethod
    def put_message(self, message: Message) -> None:
        """
        写入一条Message消息.
        :param message: Message消息
        """
        pass

    @abstractmethod
    def delete_message(self, session_id: str, message_id: str) -> None:
        """
        删除一条Message消息.
        :param session_id: Session会话ID
        :param message_id: Message消息ID
        """
        pass

    @abstractmethod
    def delete_messages(self, session_id: str) -> None:
        """
        删除一个Session会话的所有Message消息.
        :param session_id: Session会话ID
        """
        pass

    @abstractmethod
    def delete_all_messages(self) -> None:
        """
        删除所有Session会话的所有消息.
        """
        pass

    @abstractmethod
    def update_message(self, message: Message) -> None:
        """
        更新一条Message消息.
        :param message: Message消息
        """
        pass

    @abstractmethod
    def get_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> Optional[Message]:
        """
        查询一条Message消息
        :param session_id: Session会话ID
        :param message_id: Message消息ID
        :param create_time: 创建时间. (可选参数，设置该参数能提高查询性能)
        """
        pass

    @abstractmethod
    def get_all_messages(self) -> Iterator[Message]:
        """
        获取所有Session会话的所有消息.
        """
        pass

    @abstractmethod
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
        """
        返回一个Session会话的所有消息。可以根据参数条件进行过滤.
        :param session_id: Session会话ID
        :param inclusive_start_create_time: 起始时间
        :param inclusive_end_create_time: 结束时间
        :param order: 按照创建时间正序还是逆序查询数据
        :param metadata_filter: metadata过滤条件。
        :param max_count: Iterator中最大的个数。
        :param batch_size: 内部批量获取参数。
        """
        pass

    @abstractmethod
    def search_messages(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Message]:
        pass
