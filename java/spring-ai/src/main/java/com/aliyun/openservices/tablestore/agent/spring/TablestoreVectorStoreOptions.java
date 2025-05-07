package com.aliyun.openservices.tablestore.agent.spring;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.aliyun.openservices.tablestore.agent.knowledge.KnowledgeStoreImpl;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Tablestore vector store options. {@link KnowledgeStoreImpl}
 */
@Builder
@Getter
public class TablestoreVectorStoreOptions {

    private final SyncClient client;
    @Builder.Default
    @NonNull
    private final String tableName = "knowledge";
    @Builder.Default
    @NonNull
    private final String searchIndexName = "knowledge_search_index_name";
    @Builder.Default
    @NonNull
    private final List<FieldSchema> metadataSchema = Collections.emptyList();
    @Builder.Default
    @NonNull
    private final String textField = "text";
    @Builder.Default
    @NonNull
    private final String embeddingField = "embedding";
    @Builder.Default
    @NonNull
    private final VectorMetricType embeddingMetricType = VectorMetricType.COSINE;
    @NonNull
    private final Integer embeddingDimension;
    @NonNull
    private final Boolean enableMultiTenant;
}
