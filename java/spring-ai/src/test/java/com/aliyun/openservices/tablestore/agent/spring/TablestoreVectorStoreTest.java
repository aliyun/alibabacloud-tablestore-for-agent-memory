package com.aliyun.openservices.tablestore.agent.spring;

import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.aliyun.openservices.tablestore.agent.knowledge.KnowledgeStoreImpl;
import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.document.Document;
import org.springframework.ai.test.vectorstore.BaseVectorStoreTests;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;

class TablestoreVectorStoreTest extends BaseVectorStoreTests {

    private static TablestoreVectorStore store;
    private static KnowledgeStoreImpl innerStore;

    @BeforeAll
    static void beforeAllSetUp() throws Exception {
        List<FieldSchema> extraMetaDataIndexSchema = Arrays.asList(
            new FieldSchema("country", FieldType.KEYWORD),
            new FieldSchema("year", FieldType.LONG),
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );
        innerStore = KnowledgeStoreImpl.builder()
            .client(TestEnv.getClient())
            .metadataSchema(extraMetaDataIndexSchema)
            .tableName("spring_ai_multiTenantKnowledgeStore")
            .textField("text_1")
            .embeddingField("embedding_1")
            .embeddingDimension(384)
            .enableMultiTenant(true)
            .build();
        innerStore.deleteTableAndIndex();
        store = TablestoreVectorStore.builder(innerStore, new TestEmbedding()).initializeTable(true).build();
        store.afterPropertiesSet();
        TablestoreHelper.waitSearchIndexIncPhrase(innerStore.getClient(), innerStore.getTableName(), innerStore.getSearchIndexName());

        Awaitility.setDefaultPollInterval(3, TimeUnit.SECONDS);
        Awaitility.setDefaultPollDelay(Duration.ZERO);
        Awaitility.setDefaultTimeout(Duration.ofMinutes(1));
    }

    @BeforeEach
    void cleanDatabase() {
        innerStore.deleteAllDocuments();
    }

    @Override
    protected void executeTest(Consumer<VectorStore> testFunction) {
        testFunction.accept(store);
    }

    @Override
    protected Document createDocument(String country, Integer year) {
        Document document = super.createDocument(country, year);
        document.getMetadata().put(com.aliyun.openservices.tablestore.agent.model.Document.DOCUMENT_TENANT_ID, "user1");
        document.getMetadata().put("meta_example_boolean", ThreadLocalRandom.current().nextBoolean());
        document.getMetadata().put("meta_example_double", ThreadLocalRandom.current().nextDouble());
        return document;
    }

    @Test
    void testFilterWithTenantId() {
        List<Document> documents = setupTestDocuments(store);
        TablestoreHelper.waitSearchIndexReady(innerStore.getClient(), innerStore.getTableName(), innerStore.getSearchIndexName(), documents.size());
        {
            List<Document> results = store.similaritySearch(
                SearchRequest.builder()
                    .filterExpression(
                        "(country == 'BG' || meta_example_double < 134.34) && tenant_id == 'user3' && meta_example_boolean == true &&tenant_id IN ['user1', 'user2']"
                    )
                    .query("The World")
                    .topK(5)
                    .similarityThresholdAll()
                    .build()
            );
            Assertions.assertThat(results).isEmpty();
        }
        {
            List<Document> results = store.similaritySearch(
                SearchRequest.builder().filterExpression("tenant_id == 'user1'").query("The World").topK(5).similarityThresholdAll().build()
            );
            Assertions.assertThat(results).hasSizeGreaterThanOrEqualTo(3);
        }
    }
}
