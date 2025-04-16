from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import Dict, Iterator, List, Union
from typing import Optional

from pydantic import BaseModel, Field, validate_call

from tablestore_for_agent_memory.base.filter import Filter

DOCUMENT_DEFAULT_TENANT_ID: str = "__default"


@dataclass
class Document(ABC):
    """
    文档
    """

    document_id: str
    """
    文档id
    """

    tenant_id: Optional[str] = DOCUMENT_DEFAULT_TENANT_ID
    """
    tenant_id: 多租户场景下的租户id，如果不涉及多租户问题，可以不使用。
    在多租户场景中可以使用该字段，租户可以是知识库、用户、组织等，具体可以参考业务场景。通常来说使用用户id或者知识库id当做租户id有通用性。
    """

    text_content: Optional[str] = None
    """
    文档的文本内容
    """

    vector_content: Optional[List[float]] = None
    """
    文档经过Embedding后的向量内容
    """

    metadata: Optional[Dict[str, Union[int, float, str, bool, bytearray]]] = field(default_factory=dict)
    """
    文档的元数据信息
    """


@dataclass
class DocumentHit(ABC):
    """
    搜索的结果
    """

    document: Document
    """
    文档
    """

    score: Optional[float] = None
    """
    分数
    """


class BaseKnowledgeStore(BaseModel, ABC):

    @abstractmethod
    def put_document(self, document: Document) -> None:
        """
        写入一个Document文档
        :param document: 文档
        """
        pass

    @abstractmethod
    def update_document(self, document: Document) -> None:
        """
        更新一个Document文档
        :param document:  文档
        """
        pass

    @abstractmethod
    def delete_document(self, document_id: str, tenant_id: Optional[str] = None) -> None:
        """
        删除一个Document文档
        :param document_id: 文档id
        :param tenant_id: 租户id
        """
        pass

    @abstractmethod
    def delete_document_by_tenant(self, tenant_id: str) -> None:
        """
        删除某一个租户下的全部Document文档。如果没有使用多租户能力，则删除全部文档。
        :param tenant_id: 租户id
        """
        pass

    @abstractmethod
    def delete_all_documents(self) -> None:
        """
        删除整个表里的所有文档。
        """
        pass

    @abstractmethod
    def get_document(self, document_id: str, tenant_id: Optional[str] = None) -> Optional[Document]:
        """
        查询单个Document文档详情
        :param document_id: 文档id
        :param tenant_id: 租户id
        """
        pass

    @abstractmethod
    def get_documents(self, document_id_list: List[str], tenant_id: Optional[str] = None) -> List[Optional[Document]]:
        """
        批量查询单个Document文档详情
        :param document_id_list: 文档id列表。
        :param tenant_id: 租户id（如果用户使用了多租户能力，该接口仅能批量查询单个租户的n个文档）
        """
        pass

    @abstractmethod
    def get_all_documents(self) -> Iterator[Document]:
        """
        获取整个表里的所有文档。
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
                         ) -> (List[DocumentHit], Optional[str]):
        """
        搜索 Document.
        :param tenant_id: 租户id。
        :param metadata_filter: metadata过滤条件。
        :param limit: 单次返回行数.
        :param next_token: 下次翻页的token.
        :param meta_data_to_get: 需要返回的meta_data字段。默认仅返回创建索引时候指定的meta字段，如果需要返回额外字段，请在该参数中指定。
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
                         ) -> (List[DocumentHit], Optional[str]):
        """
        通过全文检索查询Document的内容(查询content字段).
        :param query: 用户输入的待查询的文本内容
        :param tenant_id: 租户id。
        :param metadata_filter: metadata过滤条件。
        :param limit: 单次返回行数.
        :param next_token: 下次翻页的token
        :param meta_data_to_get: 需要返回的meta_data字段。默认仅返回创建索引时候指定的meta字段，如果需要返回额外字段，请在该参数中指定。
        :rtype: (文档列表, 下一次访问的token)
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
                      ) -> (List[DocumentHit], Optional[str]):
        """
        通过向量查询Document的内容(查询vector字段).
        :param query_vector: 用户输入的待查询的向量内容
        :param top_k: 查询返回topK
        :param tenant_id: 租户id。
        :param metadata_filter: metadata过滤条件。
        :param limit: 单次返回行数.
        :param next_token: 下次翻页的token
        :param meta_data_to_get: 需要返回的meta_data字段。默认仅返回创建索引时候指定的meta字段，如果需要返回额外字段，请在该参数中指定。
        :rtype: (文档列表, 下一次访问的token)
        """
        pass
