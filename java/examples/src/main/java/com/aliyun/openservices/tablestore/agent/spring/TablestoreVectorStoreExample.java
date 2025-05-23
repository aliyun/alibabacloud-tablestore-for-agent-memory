package com.aliyun.openservices.tablestore.agent.spring;

import com.aliyun.openservices.tablestore.agent.knowledge.EmbeddingService;
import com.aliyun.openservices.tablestore.agent.knowledge.KnowledgeStoreImpl;
import com.aliyun.openservices.tablestore.agent.knowledge.KnowledgeStoreInitExample;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;

public class TablestoreVectorStoreExample {

    /**
     * 因为 Spring AI Vector Store 兼容版基于 `Knowledge Store` 实现，所以需要先初始化 KnowledgeStore
     *
     * @param knowledgeStoreImpl 如何初始化 KnowledgeStore 请参考 {@link KnowledgeStoreInitExample}
     */
    public void example(KnowledgeStoreImpl knowledgeStoreImpl) throws Exception {

        /*
         * 创建一个Embedding模型供后续使用。 这里以本地比较火的 Embedding 模型 “BAAI/bge-base-zh-v1.5”为例, 维度768，使用djl可直接本地基于cpu跑起来。
         * 模型细节：https://modelscope.cn/models/BAAI/bge-base-zh-v1.5
         */
        String modelName = "ai.djl.huggingface.rust/BAAI/bge-base-zh-v1.5/0.0.1/bge-base-zh-v1.5";
        EmbeddingService embeddingService = new EmbeddingService(modelName); // 进程结束可以将其close

        TablestoreVectorStore store = TablestoreVectorStore.builder(knowledgeStoreImpl, embeddingService)
            .initializeTable(true) // 首次使用可将该参数设置为 true，进行初始化表。后续不需要设置。
            .build();

        /*
         * 初始化表.spring内部会自动初始化表，如果非spring场景，可以人工调用。或者直接使用 knowledgeStoreImpl.initTable()完成初始化。
         */
        store.afterPropertiesSet();

        /*
         * 声明文档
         */
        Map<String, Object> metadata = new HashMap();
        metadata.put("city", "hangzhou");
        metadata.put("year", 2005);
        // 因 KnowledgeStoreImpl 内部有多租户优化，而spring不支持多租户优化，这里我们将多租户设置到metadata中
        metadata.put(com.aliyun.openservices.tablestore.agent.model.Document.DOCUMENT_TENANT_ID, "租户id_user小明");
        Document document = new Document("文档id_001", "The World is Big and Salvation Lurks Around the Corner", metadata);

        // 添加文档
        store.add(List.of(document));

        // 删除文档
        store.delete(List.of("文档id_001"));

        /*
         * 搜索文档。
         *
         * 为了兼容KnowledgeStoreImpl的多租户能力，请在查询条件里设置: tenant_id == 'user3' 或者 tenant_id IN ['user1', 'user2']
         *
         * <p> tenant_id 的字段名是固定的，来自常量 {@link com.aliyun.openservices.tablestore.agent.model.Document.DOCUMENT_TENANT_ID}</p>
         */
        String filterExpression = " (city == 'hangzhou' || year < 2025) && tenant_id == 'user3' ";
        List<Document> results = store.similaritySearch(
            SearchRequest.builder().filterExpression(filterExpression).query("The World").topK(5).similarityThreshold(0.0f).build()
        );
        for (Document result : results) {
            String docId = result.getId();
            String docText = result.getText();
            Map<String, Object> docMetadata = result.getMetadata();
            Double score = result.getScore();
        }
    }
}
