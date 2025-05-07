package com.aliyun.openservices.tablestore.agent.knowledge;

import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.aliyun.openservices.tablestore.agent.BaseTest;
import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.DocumentHit;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import com.aliyun.openservices.tablestore.agent.util.CollectionUtil;
import com.aliyun.openservices.tablestore.agent.util.FakeEmbedding;
import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class KnowledgeStoreTest extends BaseTest {

    KnowledgeStoreImpl multiTenantStore;
    KnowledgeStoreImpl singleKnowledgeStore;

    FakeEmbedding fakeEmbedding = new FakeEmbedding(16);

    @BeforeEach
    void setUp() {

        List<FieldSchema> extraMetaDataIndexSchema = Arrays.asList(
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );

        multiTenantStore = KnowledgeStoreImpl.builder()
            .client(client)
            .metadataSchema(extraMetaDataIndexSchema)
            .tableName("multiTenantKnowledgeStore")
            .textField("text_1")
            .embeddingField("embedding_1")
            .embeddingDimension(16)
            .enableMultiTenant(true)
            .build();

        singleKnowledgeStore = KnowledgeStoreImpl.builder()
            .client(client)
            .metadataSchema(extraMetaDataIndexSchema)
            .tableName("simpleTenantKnowledgeStore")
            .textField("text_1")
            .embeddingField("embedding_1")
            .embeddingDimension(16)
            .enableMultiTenant(false)
            .build();
    }

    Document randomDocument(String tenantId) {
        String documentId = UUID.randomUUID().toString();
        Document document = tenantId == null ? new Document(documentId) : new Document(documentId, tenantId);
        String text = randomFrom(Arrays.asList("abc", "def", "ghi", "abcd", "abcdef", "abcgh"));
        document.setText(text);
        document.setEmbedding(fakeEmbedding.embed(text));
        String name = faker.name().fullName();
        document.getMetadata().put("meta_example_string", name);
        document.getMetadata().put("meta_example_text", text);
        document.getMetadata().put("meta_example_long", faker.number().numberBetween(0, Long.MAX_VALUE));
        document.getMetadata().put("meta_example_double", faker.number().randomDouble(2, 0, 1));
        document.getMetadata().put("meta_example_boolean", faker.bool().bool());
        document.getMetadata().put("meta_example_bytes", name.getBytes(StandardCharsets.UTF_8));
        return document;
    }

    @Test
    void testSimpleMultiTenantStore() {
        KnowledgeStoreImpl store = multiTenantStore;
        store.deleteTableAndIndex();
        store.initTable();
        store.deleteAllDocuments();

        Document document = randomDocument("1");
        Assertions.assertEquals("1", document.getTenantId());
        store.putDocument(document);
        Document documentPut = store.getDocument(document.getDocumentId(), document.getTenantId());
        assertEquals(documentPut, document);
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_string"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_text"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_long"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_double"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_boolean"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_bytes"));

        Document documentToUpdate = new Document(document);
        Assertions.assertEquals(documentToUpdate, document);
        documentToUpdate.getMetadata().put("meta_example_string", "updated");
        store.updateDocument(documentToUpdate);
        Document documentUpdated = store.getDocument(document.getDocumentId(), document.getTenantId());
        assertEquals(documentToUpdate, documentUpdated);

        {
            store.putDocument(document);
            store.deleteDocument(document.getDocumentId());
            Document documentDeleted = store.getDocument(document.getDocumentId(), document.getTenantId());
            Assertions.assertNull(documentDeleted);
        }
        {
            store.putDocument(document);
            store.deleteDocument(document.getDocumentId(), "1");
            Document documentDeleted = store.getDocument(document.getDocumentId(), document.getTenantId());
            Assertions.assertNull(documentDeleted);
        }

        int total = ThreadLocalRandom.current().nextInt(20, 30);
        List<String> user1DocIds = new ArrayList<>();
        List<String> user2DocIds = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            Document documentForBatch = randomDocument(randomFrom(Arrays.asList("1", "2")));
            if (documentForBatch.getTenantId().equals("1")) {
                user1DocIds.add(documentForBatch.getDocumentId());
            }
            if (documentForBatch.getTenantId().equals("2")) {
                user2DocIds.add(documentForBatch.getDocumentId());
            }
            store.putDocument(documentForBatch);
        }
        log.info("total:{}, user1DocIds:{}, user2DocIds:{}", total, user1DocIds.size(), user2DocIds.size());
        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllDocuments()).size());

        Assertions.assertThrows(IllegalArgumentException.class, () -> store.getDocuments(user1DocIds));
        {
            List<Document> documents = store.getDocuments(user1DocIds, "1");
            Assertions.assertEquals(documents.size(), user1DocIds.size());
            for (int i = 0; i < documents.size(); i++) {
                Document document1 = documents.get(i);
                Assertions.assertEquals("1", document1.getTenantId());
                Assertions.assertEquals(user1DocIds.get(i), document1.getDocumentId());
            }
        }
        {
            List<Document> documents = store.getDocuments(user2DocIds, "2");
            Assertions.assertEquals(documents.size(), user2DocIds.size());
            for (int i = 0; i < documents.size(); i++) {
                Document document1 = documents.get(i);
                Assertions.assertEquals("2", document1.getTenantId());
                Assertions.assertEquals(user2DocIds.get(i), document1.getDocumentId());
            }
        }
        {
            List<String> ids = new ArrayList<>(user2DocIds);
            ids.add("not_exist");
            List<Document> documents = store.getDocuments(ids, "2");
            Assertions.assertEquals(documents.size(), ids.size());
            Assertions.assertNull(documents.get(ids.size() - 1));
        }

        TablestoreHelper.waitSearchIndexReady(client, store.getTableName(), store.getSearchIndexName(), total);
        store.deleteDocumentByTenant("1");
        Assertions.assertEquals(total - user1DocIds.size(), CollectionUtil.toList(store.listAllDocuments()).size());

        store.deleteAllDocuments();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllDocuments()).size());
        commonTestSearch(store);
    }

    @Test
    void testSimpleSingleTenantStore() {
        KnowledgeStoreImpl store = singleKnowledgeStore;
        store.deleteTableAndIndex();
        store.initTable();
        store.deleteAllDocuments();

        Document document = randomDocument(null);
        Assertions.assertEquals(Document.DOCUMENT_DEFAULT_TENANT_ID, document.getTenantId());
        store.putDocument(document);
        Document documentPut = store.getDocument(document.getDocumentId(), document.getTenantId());
        assertEquals(documentPut, document);
        documentPut = store.getDocument(document.getDocumentId());
        assertEquals(documentPut, document);
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_string"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_text"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_long"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_double"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_boolean"));
        Assertions.assertTrue(documentPut.getMetadata().containsKey("meta_example_bytes"));

        Document documentToUpdate = new Document(document);
        Assertions.assertEquals(documentToUpdate, document);
        documentToUpdate.getMetadata().put("meta_example_string", "updated");
        store.updateDocument(documentToUpdate);
        Document documentUpdated = store.getDocument(document.getDocumentId(), document.getTenantId());
        assertEquals(documentToUpdate, documentUpdated);

        log.info("exception:", Assertions.assertThrows(IllegalArgumentException.class, () -> store.deleteDocument(document.getDocumentId(), "123")));
        store.deleteDocument(document.getDocumentId());
        Document documentDeleted = store.getDocument(document.getDocumentId(), document.getTenantId());
        Assertions.assertNull(documentDeleted);

        int total = ThreadLocalRandom.current().nextInt(20, 30);
        List<String> user1DocIds = new ArrayList<>();
        List<String> user2DocIds = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            Document documentForBatch = randomDocument(null);
            String userId = randomFrom(Arrays.asList("1", "2"));
            documentForBatch.getMetadata().put("userId", userId);
            if (userId.equals("1")) {
                user1DocIds.add(documentForBatch.getDocumentId());
            }
            if (userId.equals("2")) {
                user2DocIds.add(documentForBatch.getDocumentId());
            }
            store.putDocument(documentForBatch);
        }
        log.info("total:{}, user1DocIds:{}, user2DocIds:{}", total, user1DocIds.size(), user2DocIds.size());
        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllDocuments()).size());
        log.info("exception:", Assertions.assertThrows(IllegalArgumentException.class, () -> store.getDocuments(user1DocIds, "1")));

        {
            List<Document> documents = store.getDocuments(user1DocIds);
            Assertions.assertEquals(documents.size(), user1DocIds.size());
            for (int i = 0; i < documents.size(); i++) {
                Document document1 = documents.get(i);
                Assertions.assertEquals("1", document1.getMetadata().getString("userId"));
                Assertions.assertEquals(user1DocIds.get(i), document1.getDocumentId());
            }
        }
        {
            List<Document> documents = store.getDocuments(user2DocIds);
            Assertions.assertEquals(documents.size(), user2DocIds.size());
            for (int i = 0; i < documents.size(); i++) {
                Document document1 = documents.get(i);
                Assertions.assertEquals("2", document1.getMetadata().getString("userId"));
                Assertions.assertEquals(user2DocIds.get(i), document1.getDocumentId());
            }
        }
        {
            List<String> ids = new ArrayList<>(user2DocIds);
            ids.add("not_exist");
            List<Document> documents = store.getDocuments(ids);
            Assertions.assertEquals(documents.size(), ids.size());
            Assertions.assertNull(documents.get(ids.size() - 1));
        }

        TablestoreHelper.waitSearchIndexReady(client, store.getTableName(), store.getSearchIndexName(), total);
        Assertions.assertThrows(IllegalArgumentException.class, () -> store.deleteDocumentByTenant("1"));

        store.deleteAllDocuments();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllDocuments()).size());

        commonTestSearch(store);
    }

    void commonTestSearch(KnowledgeStoreImpl store) {
        store.deleteAllDocuments();
        Boolean enableMultiTenant = store.getEnableMultiTenant();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllDocuments()).size());

        int total = 80;
        for (int i = 0; i < total; i++) {
            Document documentForBatch = randomDocument(enableMultiTenant ? randomFrom(Arrays.asList("1", "2")) : null);
            store.putDocument(documentForBatch);
        }
        TablestoreHelper.waitSearchIndexReady(client, store.getTableName(), store.getSearchIndexName(), total);

        {
            {
                Response<DocumentHit> response = store.searchDocuments(KnowledgeSearchRequest.builder().limit(100).build());
                Assertions.assertNull(response.getNextToken());
                Assertions.assertEquals(total, response.getHits().size());
                Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
            }
            {
                if (enableMultiTenant) {
                    {
                        Response<DocumentHit> response = store.searchDocuments(KnowledgeSearchRequest.builder().tenantId("1").limit(100).build());
                        Assertions.assertNull(response.getNextToken());
                        Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
                        Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getTenantId().equals("1")));
                    }
                    {
                        Response<DocumentHit> response = store.searchDocuments(KnowledgeSearchRequest.builder().tenantId("1").tenantId("2").limit(100).build());
                        Assertions.assertNull(response.getNextToken());
                        Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
                        Assertions.assertEquals(total, response.getHits().size());
                    }
                }
            }
        }

        {
            Response<DocumentHit> response = store.searchDocuments(
                KnowledgeSearchRequest.builder().limit(3).metadataFilter(Filters.eq("meta_example_boolean", true)).build()
            );
            Assertions.assertNotNull(response.getNextToken());
            Assertions.assertTrue(response.getHits().size() <= 3);
            Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
            Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
        }

        {
            Response<DocumentHit> response = store.searchDocuments(
                KnowledgeSearchRequest.builder().limit(3).metadataFilter(Filters.not(Filters.eq("meta_example_boolean", true))).build()
            );
            Assertions.assertNotNull(response.getNextToken());
            Assertions.assertTrue(response.getHits().size() <= 3);
            Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
            Assertions.assertFalse(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
        }
        {
            Response<DocumentHit> response = store.searchDocuments(
                KnowledgeSearchRequest.builder().limit(100).metadataFilter(Filters.not(Filters.eq("meta_example_boolean", true))).build()
            );
            int metaTrue = response.getHits().size();
            String nextToken = null;
            List<DocumentHit> hits = new ArrayList<>();
            while (true) {
                response = store.searchDocuments(
                    KnowledgeSearchRequest.builder().limit(3).metadataFilter(Filters.not(Filters.eq("meta_example_boolean", true))).nextToken(nextToken).build()
                );
                hits.addAll(response.getHits());
                nextToken = response.getNextToken();
                if (nextToken == null) {
                    break;
                }
            }
            Assertions.assertEquals(metaTrue, hits.size());
            Assertions.assertFalse(hits.stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
        }

        {
            Response<DocumentHit> response = store.searchDocuments(
                KnowledgeSearchRequest.builder()
                    .limit(100)
                    .metadataFilter(Filters.and(Filters.gt("meta_example_double", 0.5), Filters.not(Filters.eq("meta_example_boolean", true))))
                    .build()
            );
            Assertions.assertNull(response.getNextToken());
            if (!response.getHits().isEmpty()) {
                Assertions.assertEquals(Double.NaN, response.getHits().get(0).getScore());
                Assertions.assertFalse(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
                Assertions.assertTrue(
                    response.getHits().stream().allMatch(hit -> Objects.requireNonNull(hit.getDocument().getMetadata().getDouble("meta_example_double")) > 0.5)
                );
            }
        }

        {
            Response<DocumentHit> response = store.fullTextSearch("abc", null, 100, Filters.eq("meta_example_boolean", true), null, null);
            Assertions.assertNull(response.getNextToken());
            if (!response.getHits().isEmpty()) {
                Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
                DocumentHit documentHit = response.getHits().get(0);
                Document document = documentHit.getDocument();
                Assertions.assertTrue(document.getText().contains("abc"));
                Assertions.assertNull(document.getEmbedding());
                Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                Assertions.assertNotNull(document.getMetadata().get("meta_example_long"));
                Assertions.assertNotNull(document.getMetadata().get("meta_example_string"));
                Assertions.assertNotNull(document.getMetadata().get("meta_example_text"));
            }
        }
        {
            Response<DocumentHit> response = store.fullTextSearch(
                "abc",
                null,
                100,
                Filters.eq("meta_example_boolean", true),
                null,
                Arrays.asList("meta_example_boolean", "meta_example_double")
            );
            Assertions.assertNull(response.getNextToken());
            if (!response.getHits().isEmpty()) {
                Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
                DocumentHit documentHit = response.getHits().get(0);
                Document document = documentHit.getDocument();
                Assertions.assertNull(document.getText());
                Assertions.assertNull(document.getEmbedding());
                Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                Assertions.assertNull(document.getMetadata().get("meta_example_long"));
                Assertions.assertNull(document.getMetadata().get("meta_example_string"));
            }
        }
        {
            Response<DocumentHit> response = store.fullTextSearch("abc", null, 100, null, null, Collections.singletonList("meta_example_boolean"));
            Assertions.assertNull(response.getNextToken());
            if (!response.getHits().isEmpty()) {
                Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                DocumentHit documentHit = response.getHits().get(0);
                Document document = documentHit.getDocument();
                Assertions.assertNull(document.getText());
                Assertions.assertNull(document.getEmbedding());
                Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                Assertions.assertNull(document.getMetadata().get("meta_example_double"));
                Assertions.assertNull(document.getMetadata().get("meta_example_long"));
                Assertions.assertNull(document.getMetadata().get("meta_example_string"));
            }
        }
        {
            float[] query = fakeEmbedding.embed("abc");
            Response<DocumentHit> response = store.vectorSearch(query, 25, null, null, null, Collections.singletonList("meta_example_boolean"));
            if (!response.getHits().isEmpty()) {
                Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                DocumentHit documentHit = response.getHits().get(0);
                Document document = documentHit.getDocument();
                Assertions.assertNull(document.getText());
                Assertions.assertNull(document.getEmbedding());
                Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                Assertions.assertNull(document.getMetadata().get("meta_example_double"));
                Assertions.assertNull(document.getMetadata().get("meta_example_long"));
                Assertions.assertNull(document.getMetadata().get("meta_example_string"));
            }
        }
        {
            {
                float[] query = fakeEmbedding.embed("abc");
                Response<DocumentHit> response = store.vectorSearch(query, 1000, null, null, null, null);
                if (!response.getHits().isEmpty()) {
                    Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                    Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                    DocumentHit documentHit = response.getHits().get(0);
                    Document document = documentHit.getDocument();
                    Assertions.assertNotNull(document.getText());
                    Assertions.assertNull(document.getEmbedding());
                    Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                    Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                    Assertions.assertNotNull(document.getMetadata().get("meta_example_long"));
                    Assertions.assertNotNull(document.getMetadata().get("meta_example_string"));
                }
            }
            if (enableMultiTenant) {
                {
                    float[] query = fakeEmbedding.embed("abc");
                    Response<DocumentHit> response = store.vectorSearch(query, 1000, null, Collections.singleton("1"), null, null);
                    if (!response.getHits().isEmpty()) {
                        Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getTenantId().equals("1")));
                        Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                        Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                        DocumentHit documentHit = response.getHits().get(0);
                        Document document = documentHit.getDocument();
                        Assertions.assertNotNull(document.getText());
                        Assertions.assertNull(document.getEmbedding());
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_long"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_string"));
                    }
                }
                {
                    float[] query = fakeEmbedding.embed("abc");
                    Response<DocumentHit> response = store.vectorSearch(
                        query,
                        1000,
                        null,
                        Collections.singleton("1"),
                        Filters.eq("meta_example_boolean", true),
                        null
                    );
                    if (!response.getHits().isEmpty()) {
                        Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getTenantId().equals("1")));
                        Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
                        Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                        Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                        DocumentHit documentHit = response.getHits().get(0);
                        Document document = documentHit.getDocument();
                        Assertions.assertNotNull(document.getText());
                        Assertions.assertNull(document.getEmbedding());
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_long"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_string"));
                    }
                }
                {
                    float[] query = fakeEmbedding.embed("abc");
                    Set<String> tenantIds = new HashSet<>();
                    tenantIds.add("1");
                    tenantIds.add("2");
                    Response<DocumentHit> response = store.vectorSearch(
                        query,
                        1000,
                        null,
                        tenantIds,
                        Filters.or(Filters.eq("meta_example_boolean", true)),
                        null
                    );
                    if (!response.getHits().isEmpty()) {
                        Assertions.assertTrue(
                            response.getHits()
                                .stream()
                                .allMatch(hit -> hit.getDocument().getTenantId().equals("1") || hit.getDocument().getTenantId().equals("2"))
                        );
                        Assertions.assertTrue(response.getHits().stream().allMatch(hit -> hit.getDocument().getMetadata().getBoolean("meta_example_boolean")));
                        Assertions.assertNotEquals(Double.NaN, response.getHits().get(0).getScore());
                        Assertions.assertTrue(response.getHits().get(0).getScore() > 0);
                        DocumentHit documentHit = response.getHits().get(0);
                        Document document = documentHit.getDocument();
                        Assertions.assertNotNull(document.getText());
                        Assertions.assertNull(document.getEmbedding());
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_boolean"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_double"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_long"));
                        Assertions.assertNotNull(document.getMetadata().get("meta_example_string"));
                    }
                }

            }
        }
    }

}
