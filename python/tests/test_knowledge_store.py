import copy
import logging
import random

import pytest
import tablestore
from faker import Faker

from tablestore_for_agent_memory.base.base_knowledge_store import Document
from tablestore_for_agent_memory.base.filter import Filters
from tablestore_for_agent_memory.knowledge.knowledge_store import KnowledgeStore
from tablestore_for_agent_memory.util.tablestore_helper import TablestoreHelper
from tests.conftest import MockEmbedding

fk_data = Faker(locale="zh_CN")
logger = logging.getLogger(__name__)


@pytest.fixture
def single_knowledge_store(tablestore_client: tablestore.OTSClient, embedding_model: MockEmbedding):
    search_index_schema = [
        tablestore.FieldSchema("user_id", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_string", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_long", tablestore.FieldType.LONG),
        tablestore.FieldSchema("meta_double", tablestore.FieldType.DOUBLE),
        tablestore.FieldSchema("meta_boolean", tablestore.FieldType.BOOLEAN),
    ]
    logger.info(f"vector_dimension:{embedding_model.embed_dimension}")

    knowledge_store = KnowledgeStore(
        tablestore_client=tablestore_client,
        vector_dimension=embedding_model.embed_dimension,
        table_name="single_knowledge_store",
        enable_multi_tenant=False,
        search_index_schema=search_index_schema,
    )
    return knowledge_store


@pytest.fixture
def multi_tenant_knowledge_store(tablestore_client: tablestore.OTSClient, embedding_model: MockEmbedding):
    search_index_schema = [
        tablestore.FieldSchema("user_id", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_string", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_long", tablestore.FieldType.LONG),
        tablestore.FieldSchema("meta_double", tablestore.FieldType.DOUBLE),
        tablestore.FieldSchema("meta_boolean", tablestore.FieldType.BOOLEAN),
    ]
    logger.info(f"vector_dimension:{embedding_model.embed_dimension}")

    knowledge_store = KnowledgeStore(
        tablestore_client=tablestore_client,
        vector_dimension=embedding_model.embed_dimension,
        table_name="multi_tenant_knowledge_store",
        enable_multi_tenant=True,
        search_index_schema=search_index_schema,
    )
    return knowledge_store


def random_document(embedding_model: MockEmbedding, tenant_id=fk_data.user_name()) -> Document:
    document = Document(document_id=fk_data.uuid4(), tenant_id=tenant_id)
    document.text = " ".join(
        random.choices(
            ["abc", "def", "ghi", "abcd", "adef", "abcgh", "apple", "banana", "cherry"], k=random.randint(1, 10)
        )
    )
    document.embedding = embedding_model.embedding(document.text)
    document.metadata["meta_string"] = fk_data.name_female()
    document.metadata["meta_long"] = random.randint(0, 88888888)
    document.metadata["meta_double"] = random.uniform(1.0, 2.0)
    document.metadata["meta_boolean"] = random.choice([False, True])
    document.metadata["meta_bytes"] = bytearray(fk_data.city_name(), encoding="utf8")
    return document


def test_embedding_model_dimension(embedding_model: MockEmbedding):
    assert embedding_model.embed_dimension == 128
    embedding = embedding_model.embedding("123")
    assert len(embedding) == 128


def test_knowledge_store(multi_tenant_knowledge_store: KnowledgeStore, embedding_model: MockEmbedding):
    knowledge_store = multi_tenant_knowledge_store
    knowledge_store._delete_table()
    knowledge_store.init_table()
    knowledge_store.delete_all_documents()

    document = Document(document_id="1", tenant_id="user_id_1")
    document.text = "123"
    document.embedding = embedding_model.embedding(document.text)
    document.metadata["meta_string"] = "123"
    document.metadata["meta_bool"] = True
    document.metadata["meta_double"] = 123.456
    document.metadata["meta_long"] = 123456
    document.metadata["meta_bytes"] = bytearray("China", encoding="utf8")

    knowledge_store.put_document(document)

    documents = list(knowledge_store.get_all_documents())
    assert len(documents) == 1
    document_get = knowledge_store.get_document(document_id="1", tenant_id="user_id_1")
    assert document.document_id == document_get.document_id
    assert document.tenant_id == document_get.tenant_id
    assert document.text == document_get.text
    assert document.embedding == document_get.embedding
    assert document.metadata["meta_string"] == document_get.metadata["meta_string"]
    assert "123" == document_get.metadata["meta_string"]
    assert document.metadata["meta_bool"] == document_get.metadata["meta_bool"]
    assert document.metadata["meta_double"] == document_get.metadata["meta_double"]
    assert 123.456 == document_get.metadata["meta_double"]
    assert document.metadata["meta_long"] == document_get.metadata["meta_long"]
    assert 123456 == document_get.metadata["meta_long"]
    assert document.metadata["meta_bytes"] == document_get.metadata["meta_bytes"]
    assert bytearray("China", encoding="utf8") == document_get.metadata["meta_bytes"]

    document_updated = copy.deepcopy(document)
    document_updated.embedding = None
    document_updated.text = None
    document_updated.metadata["meta_string"] = "456"
    knowledge_store.put_document(document_updated)
    document_updated_get = knowledge_store.get_document(document_id="1", tenant_id="user_id_1")
    assert "456" == document_updated_get.metadata["meta_string"]

    knowledge_store.delete_document(document_id="1", tenant_id="user_id_1")

    document_updated_after_deleted = knowledge_store.get_document(document_id="1", tenant_id="user_id_1")
    assert document_updated_after_deleted is None

    total_docs = random.randint(50, 200)
    id_list = []
    for i in range(total_docs):
        document.document_id = str(i)
        knowledge_store.put_document(document)
        id_list.append(str(i))
    documents = list(knowledge_store.get_all_documents())
    assert len(documents) == total_docs
    documents_by_batch_get = knowledge_store.get_documents(document_id_list=id_list, tenant_id="user_id_1")
    assert len(documents_by_batch_get) == total_docs
    assert id_list == [doc.document_id for doc in documents_by_batch_get]

    documents_by_batch_get_404 = knowledge_store.get_documents(document_id_list=id_list, tenant_id="user_id_404")
    assert len(documents_by_batch_get_404) == total_docs
    assert all(doc is None for doc in documents_by_batch_get_404)

    knowledge_store.delete_all_documents()
    documents = list(knowledge_store.get_all_documents())
    assert len(documents) == 0


def test_single_knowledge_search(single_knowledge_store: KnowledgeStore, embedding_model: MockEmbedding):
    knowledge_store = single_knowledge_store
    knowledge_store._delete_table()
    knowledge_store.init_table()

    total_document_count = random.randint(50, 99)
    user_id_1_count = 0
    user_id_meta_boolean_true_count = 0
    user_id_meta_double_gt_half_1_count = 0
    user_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    for _ in range(total_document_count):
        document = random_document(embedding_model=embedding_model)
        user_id = random.choice(["1", "2"])
        document.metadata["user_id"] = user_id
        knowledge_store.put_document(document)
        if user_id == "1":
            user_id_1_count += 1
            if document.metadata["meta_boolean"]:
                user_id_meta_boolean_true_count += 1
            if document.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_count += 1
            if document.metadata["meta_boolean"] and document.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_and_meta_bool_true_count += 1

    documents = list(knowledge_store.get_all_documents())
    assert len(documents) == total_document_count

    TablestoreHelper.wait_search_index_ready(
        tablestore_client=knowledge_store._client,
        table_name=knowledge_store._table_name,
        index_name=knowledge_store._search_index_name,
        total_count=total_document_count,
    )

    response = knowledge_store.search_documents(limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == total_document_count
    print(len(documents))
    assert next_token is None, next_token
    response = knowledge_store.search_documents(metadata_filter=Filters.all(), limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert next_token is None, next_token
    assert len(documents) == total_document_count
    hit = documents[0]
    print("hit", hit)
    document = hit.document
    assert document.document_id is not None
    assert document.tenant_id is not None
    assert document.text is not None
    assert document.metadata is not None
    assert "meta_boolean" in document.metadata

    response = knowledge_store.search_documents(metadata_filter=Filters.eq("user_id", "1"), limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == user_id_1_count
    response = knowledge_store.search_documents(metadata_filter=Filters.eq("user_id", "1"), limit=3)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == 3
    assert all(
        hit.document.metadata["user_id"] == "1" for hit in documents
    ), f"Not all elements assert true, detail is:{[hit.document.metadata['user_id'] for hit in documents]}"
    assert next_token is not None
    print(next_token)
    while True:
        response = knowledge_store.search_documents(
            metadata_filter=Filters.eq("user_id", "1"), limit=3, next_token=next_token
        )
        sub_docs, next_token = (response.hits, response.next_token)
        documents.extend(sub_docs)
        if next_token is None:
            break
    print(len(documents))
    assert len(documents) == user_id_1_count

    response = knowledge_store.search_documents(
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5)])
    )

    documents, next_token = (response.hits, response.next_token)
    print("user_id_meta_double_gt_half_1_count", user_id_meta_double_gt_half_1_count)
    assert len(documents) == user_id_meta_double_gt_half_1_count

    response = knowledge_store.search_documents(
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.eq("meta_boolean", True)])
    )
    documents, next_token = (response.hits, response.next_token)
    print("user_id_meta_boolean_true_count", user_id_meta_boolean_true_count)
    assert len(documents) == user_id_meta_boolean_true_count

    response = knowledge_store.search_documents(
        metadata_filter=Filters.logical_and(
            [Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]
        )
    )
    documents, next_token = (response.hits, response.next_token)
    print(
        "user_id_meta_double_gt_half_1_and_meta_bool_true_count",
        user_id_meta_double_gt_half_1_and_meta_bool_true_count,
    )
    assert len(documents) == user_id_meta_double_gt_half_1_and_meta_bool_true_count

    document_hits = []
    next_token = None
    while True:
        response = knowledge_store.full_text_search(
            query="abc",
            metadata_filter=Filters.eq("user_id", "1"),
            limit=2,
            next_token=next_token,
            meta_data_to_get=["meta_boolean", "meta_string", "text", "user_id"],
        )
        sub_documents, next_token = (response.hits, response.next_token)
        document_hits.extend(sub_documents)
        if next_token is None:
            break
    print("full_text_search", len(document_hits))
    assert all(
        hit.document.metadata["user_id"] == "1"
        and hit.document.text is not None
        and "meta_boolean" in hit.document.metadata
        and "meta_string" in hit.document.metadata
        for hit in document_hits
    ), f"Not all elements assert true, detail is:{[document for document in document_hits]}"
    assert all(
        "abc" in hit.document.text for hit in document_hits
    ), f"Not all elements assert true, detail is:{[hit.document.text for hit in document_hits]}"

    document_hits = []
    next_token = None
    while True:
        response = knowledge_store.vector_search(
            query_vector=embedding_model.embedding("abc"),
            top_k=15,
            metadata_filter=Filters.eq("user_id", "1"),
            limit=2,
            next_token=next_token,
            meta_data_to_get=["meta_boolean", "meta_string", "text", "user_id"],
        )
        sub_documents, next_token = (response.hits, response.next_token)
        document_hits.extend(sub_documents)
        if next_token is None:
            break
    print("vector_search", len(document_hits))
    assert all(
        hit.document.metadata["user_id"] == "1"
        and hit.document.text is not None
        and "meta_boolean" in hit.document.metadata
        and "meta_string" in hit.document.metadata
        for hit in document_hits
    ), f"Not all elements assert true, detail is:{[document for document in document_hits]}"


def test_multi_tenant_knowledge_search(multi_tenant_knowledge_store: KnowledgeStore, embedding_model: MockEmbedding):
    knowledge_store = multi_tenant_knowledge_store
    knowledge_store._delete_table()
    knowledge_store.init_table()

    total_document_count = random.randint(50, 99)
    user_id_1_count = 0
    user_id_meta_boolean_true_count = 0
    user_id_meta_double_gt_half_1_count = 0
    user_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    for _ in range(total_document_count):
        user_id = random.choice(["1", "2"])
        document = random_document(embedding_model=embedding_model, tenant_id=user_id)
        document.metadata["user_id"] = user_id
        knowledge_store.put_document(document)
        if user_id == "1":
            user_id_1_count += 1
            if document.metadata["meta_boolean"]:
                user_id_meta_boolean_true_count += 1
            if document.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_count += 1
            if document.metadata["meta_boolean"] and document.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_and_meta_bool_true_count += 1

    documents = list(knowledge_store.get_all_documents())
    assert len(documents) == total_document_count

    TablestoreHelper.wait_search_index_ready(
        tablestore_client=knowledge_store._client,
        table_name=knowledge_store._table_name,
        index_name=knowledge_store._search_index_name,
        total_count=total_document_count,
    )

    response = knowledge_store.search_documents(limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == total_document_count
    print(len(documents))
    assert next_token is None, next_token

    response = knowledge_store.search_documents(limit=100, tenant_id="1")
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == user_id_1_count
    print(len(documents))
    assert next_token is None, next_token

    response = knowledge_store.search_documents(metadata_filter=Filters.all(), limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert next_token is None, next_token
    assert len(documents) == total_document_count
    hit = documents[0]
    print("hit", hit)
    document = hit.document
    assert document.document_id is not None
    assert document.tenant_id is not None
    assert document.text is not None
    assert document.metadata is not None
    assert "meta_boolean" in document.metadata

    response = knowledge_store.search_documents(metadata_filter=Filters.all(), limit=100, tenant_id="1")
    documents, next_token = (response.hits, response.next_token)
    assert next_token is None, next_token
    assert len(documents) == user_id_1_count

    response = knowledge_store.search_documents(tenant_id="1", limit=100)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == user_id_1_count
    response = knowledge_store.search_documents(tenant_id="1", limit=3)
    documents, next_token = (response.hits, response.next_token)
    assert len(documents) == 3
    assert all(
        hit.document.metadata["user_id"] == "1" for hit in documents
    ), f"Not all elements assert true, detail is:{[hit.document.metadata['user_id'] for hit in documents]}"
    assert next_token is not None
    print(next_token)
    while True:
        response = knowledge_store.search_documents(tenant_id="1", limit=3, next_token=next_token)
        sub_docs, next_token = (response.hits, response.next_token)
        documents.extend(sub_docs)
        if next_token is None:
            break
    print(len(documents))
    assert len(documents) == user_id_1_count

    response = knowledge_store.search_documents(
        tenant_id="1", metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5)])
    )
    documents, next_token = (response.hits, response.next_token)
    print("user_id_meta_double_gt_half_1_count", user_id_meta_double_gt_half_1_count)
    assert len(documents) == user_id_meta_double_gt_half_1_count

    response = knowledge_store.search_documents(
        tenant_id="1",
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.eq("meta_boolean", True)]),
    )
    documents, next_token = (response.hits, response.next_token)
    print("user_id_meta_boolean_true_count", user_id_meta_boolean_true_count)
    assert len(documents) == user_id_meta_boolean_true_count

    response = knowledge_store.search_documents(
        tenant_id="1",
        metadata_filter=Filters.logical_and(
            [Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]
        ),
    )
    documents, next_token = (response.hits, response.next_token)
    print(
        "user_id_meta_double_gt_half_1_and_meta_bool_true_count",
        user_id_meta_double_gt_half_1_and_meta_bool_true_count,
    )
    assert len(documents) == user_id_meta_double_gt_half_1_and_meta_bool_true_count

    response = knowledge_store.search_documents(limit=10)
    document_hits, next_token = (response.hits, response.next_token)
    assert all(
        document_hit.score is None for document_hit in document_hits
    ), f"Not all elements assert true, detail is:{[document for document in document_hits]}"

    document_hits = []
    next_token = None
    while True:
        response = knowledge_store.full_text_search(
            query="abc",
            tenant_id="1",
            limit=2,
            next_token=next_token,
            meta_data_to_get=["meta_boolean", "meta_string", "text", "user_id"],
        )
        sub_documents, next_token = (response.hits, response.next_token)
        document_hits.extend(sub_documents)
        if next_token is None:
            break
    print("full_text_search", len(document_hits))
    assert all(
        document_hit.document.metadata["user_id"] == "1"
        and 0 < document_hit.score < 10
        and document_hit.document.text is not None
        and "meta_boolean" in document_hit.document.metadata
        and "meta_string" in document_hit.document.metadata
        for document_hit in document_hits
    ), f"Not all elements assert true, detail is:{[document for document in document_hits]}"
    assert all(
        "abc" in hit.document.text for hit in document_hits
    ), f"Not all elements assert true, detail is:{[hit.document.text for hit in document_hits]}"

    document_hits = []
    next_token = None
    while True:
        response = knowledge_store.vector_search(
            query_vector=embedding_model.embedding("abc"),
            top_k=15,
            tenant_id="1",
            limit=2,
            next_token=next_token,
            meta_data_to_get=["meta_boolean", "meta_string", "text", "user_id"],
        )
        sub_documents, next_token = (response.hits, response.next_token)
        document_hits.extend(sub_documents)
        if next_token is None:
            break
    print("vector_search", len(document_hits))
    assert all(
        hit.document.metadata["user_id"] == "1"
        and 0 < hit.score < 10
        and hit.document.text is not None
        and "meta_boolean" in hit.document.metadata
        and "meta_string" in hit.document.metadata
        for hit in document_hits
    ), f"Not all elements assert true, detail is:{[document for document in document_hits]}"

    knowledge_store.delete_document_by_tenant(tenant_id="1")
    documents = list(knowledge_store.get_all_documents())
    assert all(
        document.tenant_id != "1" for document in documents
    ), f"Not all elements assert true, detail is:{[document for document in documents]}"


def test_multi_tenant_knowledge_check_tenant(
    multi_tenant_knowledge_store: KnowledgeStore, embedding_model: MockEmbedding
):
    knowledge_store = multi_tenant_knowledge_store
    knowledge_store._delete_table()
    knowledge_store.init_table()
    document_without_tenant_id = random_document(embedding_model=embedding_model)
    document_with_tenant_id = random_document(embedding_model=embedding_model)
    check_tenant_msg = "the 'tenant_id' is not set"
    document_without_tenant_id.tenant_id = None

    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.put_document(document_without_tenant_id)
    knowledge_store.put_document(document_with_tenant_id)

    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.update_document(document_without_tenant_id)
    knowledge_store.update_document(document_with_tenant_id)

    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.delete_document(document_id="id")
    knowledge_store.delete_document(document_id="id", tenant_id="1")

    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.get_document(document_id="id")
    knowledge_store.get_document(document_id="id", tenant_id="1")

    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.get_documents(document_id_list=["123"])
    knowledge_store.get_documents(document_id_list=["123"], tenant_id="1")

    document_with_embedding_error = random_document(embedding_model=embedding_model)
    document_with_embedding_error.embedding.append(0.5)
    document_with_none_embedding = random_document(embedding_model=embedding_model)
    document_with_none_embedding.embedding = None

    check_tenant_msg = "is not the same as the knowledge store dimension"
    knowledge_store.put_document(document_with_none_embedding)
    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.put_document(document_with_embedding_error)

    knowledge_store.update_document(document_with_none_embedding)
    with pytest.raises(ValueError, match=check_tenant_msg):
        knowledge_store.update_document(document_with_embedding_error)


def test_index_routing(multi_tenant_knowledge_store: KnowledgeStore, single_knowledge_store: KnowledgeStore):
    single_knowledge_store._delete_table()
    single_knowledge_store.init_table()
    print(single_knowledge_store._table_name)
    index_meta2, sync_stat = single_knowledge_store._client.describe_search_index(
        "single_knowledge_store", single_knowledge_store._search_index_name
    )
    print(index_meta2.index_setting)
    assert len(index_meta2.index_setting.routing_fields) == 0

    multi_tenant_knowledge_store._delete_table()
    multi_tenant_knowledge_store.init_table()
    index_meta, sync_stat = multi_tenant_knowledge_store._client.describe_search_index(
        multi_tenant_knowledge_store._table_name, multi_tenant_knowledge_store._search_index_name
    )
    print(index_meta.index_setting)
    assert len(index_meta.index_setting.routing_fields) == 1
    assert index_meta.index_setting.routing_fields[0] == "tenant_id"
