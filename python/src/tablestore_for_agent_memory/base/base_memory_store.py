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
        pass

    @abstractmethod
    def update_session(self, session: Session) -> None:
        pass

    @abstractmethod
    def delete_session(self, user_id: str, session_id: str) -> None:
        pass

    @abstractmethod
    def delete_sessions(self, user_id: str) -> None:
        pass

    @abstractmethod
    def delete_all_sessions(self) -> None:
        pass

    @abstractmethod
    def get_session(self, user_id: str, session_id: str) -> Optional[Session]:
        pass

    @abstractmethod
    def list_all_sessions(self) -> Iterator[Session]:
        pass

    @abstractmethod
    @validate_call
    def list_sessions(
            self,
            user_id: str,
            metadata_filter: Optional[Filter] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
    ) -> Iterator[Session]:
        pass

    @abstractmethod
    @validate_call
    def list_recent_sessions(
            self,
            user_id: str,
            inclusive_end_update_time: Optional[int] = None,
            metadata_filter: Optional[Filter] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
    ) -> Iterator[Session]:
        pass

    @abstractmethod
    def search_sessions(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Session]:
        pass

    @abstractmethod
    def put_message(self, message: Message) -> None:
        pass

    @abstractmethod
    def delete_message(self, session_id: str, message_id: str) -> None:
        pass

    @abstractmethod
    def delete_messages(self, session_id: str) -> None:
        pass

    @abstractmethod
    def delete_all_messages(self) -> None:
        pass

    @abstractmethod
    def update_message(self, message: Message) -> None:
        pass

    @abstractmethod
    def get_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> Optional[Message]:
        pass

    @abstractmethod
    def get_all_messages(self) -> Iterator[Message]:
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
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
            max_count: Optional[int] = None,
    ) -> Iterator[Message]:
        pass

    @abstractmethod
    def search_messages(self, metadata_filter: Filter, limit: Optional[int] = 100) -> Iterator[Message]:
        pass
