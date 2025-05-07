from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Union

from pydantic import BaseModel, Field, validate_call

from tablestore_for_agent_memory.base.common import Order, microseconds_timestamp, Response
from tablestore_for_agent_memory.base.filter import Filter


@dataclass
class Session(ABC):
    user_id: str
    """
    User id
    """

    session_id: str
    """
    Session id
    """

    update_time: int = microseconds_timestamp()
    """
    Update time of the session. This needs to be updated each time a Message is written.
    """

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)
    """
    Metadata for the session
    """


@dataclass
class Message(ABC):
    session_id: str
    """
    Session id.
    """

    message_id: str
    """
    Message id. Combined with session id, it uniquely identifies a row of data.
    """

    create_time: Optional[int] = field(default=None)
    """
    Creation time of the message. Once created, this time cannot be modified.
    """

    content: Optional[str] = field(default=None)
    """
    Content of the message
    """

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)
    """
    Metadata for the message
    """


class BaseMemoryStore(BaseModel, ABC):

    @abstractmethod
    def put_session(self, session: Session) -> None:
        """
        Write a Session into storage
        :param session:  Session content
        """
        pass

    @abstractmethod
    def update_session(self, session: Session) -> None:
        """
        Update a Session in storage
        :param session:  Session content
        """
        pass

    @abstractmethod
    def delete_session(self, user_id: str, session_id: str) -> None:
        """
        Delete a Session from storage
        :param user_id: User id
        :param session_id: Session id
        """
        pass

    @abstractmethod
    def delete_sessions(self, user_id: str) -> None:
        """
        Delete all Sessions of a user
        :param user_id: User id
        """
        pass

    @abstractmethod
    def delete_all_sessions(self) -> None:
        """
        Delete all Sessions of all users (Note: High risk operation)
        """
        pass

    @abstractmethod
    def get_session(self, user_id: str, session_id: str) -> Optional[Session]:
        """
        Retrieve detailed information of a session
        :param user_id: User id
        :param session_id: Session id
        """
        pass

    @abstractmethod
    def list_all_sessions(self) -> Iterator[Session]:
        """
        List all sessions of all users.
        """
        pass

    def delete_session_and_messages(self, user_id: str, session_id: str) -> None:
        """
        Delete a session and its corresponding messages.
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
        List all sessions for a user.
        :param user_id: User id, required parameter.
        :param metadata_filter: Metadata filter condition.
        :param batch_size: Internal batch retrieval parameter.
        :param max_count: Maximum number in the Iterator.
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
        List recent session information sorted by session update time.
        :param user_id: User ID, required parameter.
        :param inclusive_start_update_time: Start time.
        :param inclusive_end_update_time: End time.
        :param metadata_filter: Metadata filtering conditions.
        :param batch_size: Internal batch retrieval parameter.
        :param max_count: Maximum count in the Iterator.
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
    ) -> Response[Session]:
        """
        List all recent session information using continuous pagination, sorted by session update time.
        :param user_id: User ID, required parameter.
        :param page_size: Number of Sessions returned.
        :param next_token: Token for the next pagination.
        :param inclusive_start_update_time: Start time.
        :param inclusive_end_update_time: End time.
        :param metadata_filter: Metadata filtering conditions.
        :param batch_size: Internal batch retrieval parameter.
        :rtype: (List of sessions, token for the next access)
        """
        pass

    @validate_call
    @abstractmethod
    def search_sessions(self,
                        metadata_filter: Optional[Filter] = None,
                        limit: Optional[int] = Field(default=100, le=100, ge=1),
                        next_token: Optional[str] = None
                        ) -> Response[Session]:
        """
        Search for Sessions.
        :param metadata_filter: Metadata filter conditions.
        :param limit: Number of rows returned per call.
        :param next_token: Token for the next pagination.
        :rtype: (List of sessions, token for the next access)
        """
        pass

    @abstractmethod
    def put_message(self, message: Message) -> None:
        """
        Write a Message.
        :param message: Message object
        """
        pass

    @abstractmethod
    def delete_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> None:
        """
        Delete a Message.
        :param session_id: Session ID
        :param message_id: Message ID
        :param create_time: Creation time. (Optional parameter, setting this can improve query performance)
        """
        pass

    @abstractmethod
    def delete_messages(self, session_id: str) -> None:
        """
        Delete all messages from a Session.
        :param session_id: Session ID
        """
        pass

    @abstractmethod
    def delete_all_messages(self) -> None:
        """
        Delete all messages from all Sessions.
        """
        pass

    @abstractmethod
    def update_message(self, message: Message) -> None:
        """
        Update a Message.
        :param message: Message object
        """
        pass

    @abstractmethod
    def get_message(self, session_id: str, message_id: str, create_time: Optional[int] = None) -> Optional[Message]:
        """
        Query a Message.
        :param session_id: Session ID
        :param message_id: Message ID
        :param create_time: Creation time. (Optional parameter, setting this can improve query performance)
        """
        pass

    @abstractmethod
    def list_all_messages(self) -> Iterator[Message]:
        """
        Get all messages from all sessions.
        """
        pass

    @abstractmethod
    @validate_call
    def list_messages(
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
        Return all messages for a Session. Filtering can be done based on parameters.
        :param session_id: Session ID
        :param inclusive_start_create_time: Start time
        :param inclusive_end_create_time: End time
        :param order: Order data by creation time in ascending or descending order
        :param metadata_filter: Metadata filtering condition.
        :param max_count: Maximum number in the Iterator.
        :param batch_size: Internal batch retrieval parameter.
        """
        pass

    @abstractmethod
    @validate_call
    def list_messages_paginated(
            self,
            session_id: str,
            page_size: int = 100,
            next_token: Optional[str] = None,
            inclusive_start_create_time: Optional[int] = None,
            inclusive_end_create_time: Optional[int] = None,
            order: Optional[Order] = None,
            metadata_filter: Optional[Filter] = None,
            batch_size: Optional[int] = Field(default=None, le=5000, ge=1),
    ) -> Response[Session]:
        """
        List all messages using continuous pagination. Filtering can be done based on parameters.
        :param session_id: Session ID
        :param page_size: Number of Messages returned.
        :param next_token: Token for the next pagination.
        :param inclusive_start_create_time: Start time
        :param inclusive_end_create_time: End time
        :param order: Order data by creation time in ascending or descending order
        :param metadata_filter: Metadata filtering condition.
        :param batch_size: Internal batch retrieval parameter.
        :rtype: (List of messages, token for the next access)
        """
        pass

    @validate_call
    @abstractmethod
    def search_messages(self,
                        metadata_filter: Optional[Filter] = None,
                        limit: Optional[int] = Field(default=100, le=100, ge=1),
                        next_token: Optional[str] = None
                        ) -> Response[Message]:
        """
        Search for Messages.
        :param metadata_filter: Metadata filter conditions.
        :param limit: Number of rows returned per call.
        :param next_token: Token for pagination in the next call.
        :rtype: (List of messages, token for next access)
        """
        pass
