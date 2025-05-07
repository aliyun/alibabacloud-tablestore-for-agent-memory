from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import Dict, Iterator, List, Union, Any
from typing import Optional

from pydantic import BaseModel, Field, validate_call

from tablestore_for_agent_memory.base.common import Response
from tablestore_for_agent_memory.base.filter import Filter

DOCUMENT_DEFAULT_TENANT_ID: str = "__default"


@dataclass
class Document(ABC):
    """
    Document
    """

    document_id: str
    """
    Document ID
    """

    tenant_id: Optional[str] = DOCUMENT_DEFAULT_TENANT_ID
    """
    Tenant ID: Tenant ID in multi-tenant scenarios. It can be unused if not involving multi-tenancy issues.
    In multi-tenant scenarios, it can be used for tenants such as knowledge bases, users, organizations, etc., depending on specific business contexts. Generally speaking, using user IDs or knowledge base IDs as tenant IDs is common practice.
    """

    text: Optional[str] = None
    """
    Text content of the document
    """

    embedding: Optional[List[float]] = None
    """
    Vector content of the document after Embedding
    """

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)
    """
    Metadata information of the document
    """


@dataclass
class DocumentHit(ABC):
    """
    Search result
    """

    document: Document
    """
    Document
    """

    score: Optional[float] = None
    """
    Score
    """


class BaseKnowledgeStore(BaseModel, ABC):

    @abstractmethod
    def put_document(self, document: Document) -> None:
        """
        Write a Document document
        :param document: document
        """
        pass

    @abstractmethod
    def update_document(self, document: Document) -> None:
        """
        Update a Document document
        :param document: document
        """
        pass

    @abstractmethod
    def delete_document(self, document_id: str, tenant_id: Optional[str] = None) -> None:
        """
        Delete a Document document
        :param document_id: document id
        :param tenant_id: tenant id
        """
        pass

    @abstractmethod
    def delete_document_by_tenant(self, tenant_id: str) -> None:
        """
        Delete all Document documents under a certain tenant. If multi-tenancy capability is not used, delete all documents.
        :param tenant_id: tenant id
        """
        pass

    @abstractmethod
    def delete_all_documents(self) -> None:
        """
        Delete all documents in the entire table.
        """
        pass

    @abstractmethod
    def get_document(self, document_id: str, tenant_id: Optional[str] = None) -> Optional[Document]:
        """
        Query details of a single Document document
        :param document_id: document id
        :param tenant_id: tenant id
        """
        pass

    @abstractmethod
    def get_documents(self, document_id_list: List[str], tenant_id: Optional[str] = None) -> List[Optional[Document]]:
        """
        Batch query details of multiple Document documents
        :param document_id_list: list of document ids.
        :param tenant_id: tenant id (if multi-tenancy capability is used, this interface can only batch-query n documents from a single tenant)
        """
        pass

    @abstractmethod
    def get_all_documents(self) -> Iterator[Document]:
        """
        Get all documents in the entire table.
        """
        pass

    @validate_call
    @abstractmethod
    def search_documents(self,
                         tenant_id: Optional[Union[List[str], str]] = None,
                         metadata_filter: Optional[Filter] = None,
                         limit: Optional[int] = Field(default=100, le=1000, ge=1),
                         next_token: Optional[str] = None,
                         meta_data_to_get: Optional[List[str]] = None,
                         **kwargs: Any,
                         ) -> Response[DocumentHit]:
        """
        Search Documents.
        :param tenant_id: tenant id.
        :param metadata_filter: metadata filter condition.
        :param limit: number of rows returned per call.
        :param next_token: token for next pagination.
        :param meta_data_to_get: fields of meta_data to return. By default, only specified meta fields during index creation are returned; specify additional fields here if needed.
        """
        pass

    @validate_call
    @abstractmethod
    def full_text_search(self,
                         query: str,
                         tenant_id: Optional[Union[List[str], str]] = None,
                         metadata_filter: Optional[Filter] = None,
                         limit: Optional[int] = Field(default=100, le=1000, ge=1),
                         next_token: Optional[str] = None,
                         meta_data_to_get: Optional[List[str]] = None,
                         **kwargs: Any,
                         ) -> Response[DocumentHit]:
        """
        Perform a full-text search on the text content of Document.
        :param query: text content to be queried input by the user
        :param tenant_id: tenant id.
        :param metadata_filter: metadata filter condition.
        :param limit: number of rows returned per call.
        :param next_token: token for next pagination
        :param meta_data_to_get: fields of meta_data to return. By default, only specified meta fields during index creation are returned; specify additional fields here if needed.
        :rtype: (list of documents, token for next access)
        """
        pass

    @validate_call
    @abstractmethod
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
        """
        Perform a vector search on the Embedding vector content of Document.
        :param query_vector: vector content to be queried input by the user
        :param top_k: number of top results to return
        :param tenant_id: tenant id.
        :param metadata_filter: metadata filter condition.
        :param limit: number of rows returned per call.
        :param next_token: token for next pagination
        :param meta_data_to_get: fields of meta_data to return. By default, only specified meta fields during index creation are returned; specify additional fields here if needed.
        :rtype: (list of documents, token for next access)
        """
        pass
