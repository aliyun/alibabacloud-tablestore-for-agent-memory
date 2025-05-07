package com.aliyun.openservices.tablestore.agent.memory;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.aliyun.openservices.tablestore.agent.model.MetaType;
import com.aliyun.openservices.tablestore.agent.util.Pair;
import java.util.Arrays;
import java.util.List;

public class MemoryStoreInitExample {

    public void example() {
        /*
         * 初始化TableStore客户端
         */
        String endPoint = "your endPoint";
        String instanceName = "your instanceName";
        String accessKeyId = "your accessKeyId";
        String accessKeySecret = "your accessKeySecret";
        SyncClient client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);

        /*
         * 初始化MemoryStore
         */

        // 定义哪些meta字段定义到二级索引中，这样使用最近二级索引展示最近的会话信息时候可以带上这些字段信息
        List<Pair<String, MetaType>> sessionSecondaryIndexMeta = Arrays.asList(
            Pair.of("meta_example_string", MetaType.STRING),
            Pair.of("meta_example_text", MetaType.STRING),
            Pair.of("meta_example_long", MetaType.INTEGER),
            Pair.of("meta_example_double", MetaType.DOUBLE),
            Pair.of("meta_example_boolean", MetaType.BOOLEAN),
            Pair.of("meta_example_bytes", MetaType.BINARY)
        );

        // 定义“Session会话”哪些meta字段定义到多元索引中，这样使用多元索引可以搜索这些字段信息。
        List<FieldSchema> sessionSearchIndexSchema = Arrays.asList(
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );

        // 定义“Message消息”的哪些meta字段定义到多元索引中，这样使用多元索引可以搜索这些字段。
        List<FieldSchema> messageSearchIndexSchema = Arrays.asList(
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );

        // 如需自定义其它参数，可以自己通过builder选择自己需要的
        MemoryStore store = MemoryStoreImpl.builder()
            .client(client)
            .sessionSecondaryIndexMeta(sessionSecondaryIndexMeta)
            .sessionSearchIndexSchema(sessionSearchIndexSchema)
            .messageSearchIndexSchema(messageSearchIndexSchema)
            .build();

        // 初始化核心能力(表和二级索引)，能完成Memory场景的核心能力(包括对Session会话和Message消息的Meta信息的Filter过滤能力)
        store.initTable();

        // 初始化多元索引(可选能力，初始化后才能使用多元索引Search搜索，比如全文检索历史消息等)
        store.initSearchIndex();

        // 删除表和索引(方便测试期间使用)
        store.deleteTableAndIndex();
    }
}
