package com.aliyun.openservices.tablestore.agent.knowledge;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowExistenceExpectation;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.vector.VectorDataType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;
import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.DocumentHit;
import com.aliyun.openservices.tablestore.agent.model.MetaType;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.VectorQuery;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.model.sort.ScoreSort;
import com.aliyun.openservices.tablestore.agent.util.CollectionUtil;
import com.aliyun.openservices.tablestore.agent.util.Exceptions;
import com.aliyun.openservices.tablestore.agent.util.Pair;
import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import com.aliyun.openservices.tablestore.agent.util.Triple;
import com.aliyun.openservices.tablestore.agent.util.ValidationUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
@Getter
public class KnowledgeStoreImpl implements KnowledgeStore {

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

    @Override
    public void putDocument(Document document) {
        ValidationUtils.ensureNotNull(document, "document");
        ValidationUtils.ensureNotNull(document.getDocumentId(), "documentId");
        checkDimension(document);
        checkEnableMultiTenant(document);

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(document.getDocumentId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(document.getTenantId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(document.getMetadata());
        if (document.getText() != null) {
            columns.add(new Column(textField, ColumnValue.fromString(document.getText())));
        }
        if (document.getEmbedding() != null) {
            columns.add(new Column(embeddingField, ColumnValue.fromString(TablestoreHelper.encodeEmbedding(document.getEmbedding()))));
        }
        rowPutChange.addColumns(columns);
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            if (log.isDebugEnabled()) {
                log.debug("put document:{}", document);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("put document:%s failed", document), e);
        }
    }

    @Override
    public void updateDocument(Document document) {
        ValidationUtils.ensureNotNull(document, "document");
        ValidationUtils.ensureNotNull(document.getDocumentId(), "documentId");
        checkDimension(document);
        checkEnableMultiTenant(document);

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(document.getDocumentId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(document.getTenantId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowUpdateChange change = new RowUpdateChange(tableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(document.getMetadata());
        if (document.getText() != null) {
            columns.add(new Column(textField, ColumnValue.fromString(document.getText())));
        }
        if (document.getEmbedding() != null) {
            columns.add(new Column(embeddingField, ColumnValue.fromString(TablestoreHelper.encodeEmbedding(document.getEmbedding()))));
        }
        change.put(columns);
        try {
            client.updateRow(new UpdateRowRequest(change));
            if (log.isDebugEnabled()) {
                log.debug("update document:{}", document);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("update document:%s failed", document), e);
        }
    }

    @Override
    public void deleteDocument(String documentId, String tenantId) {
        ValidationUtils.ensureNotNull(documentId, "documentId");
        if (enableMultiTenant && (tenantId == null || Document.DOCUMENT_DEFAULT_TENANT_ID.equals(tenantId))) {
            List<String> tenantIds = getTenantIds(documentId);
            for (String getTenantId : tenantIds) {
                innerDelete(documentId, getTenantId);
            }
            return;
        }
        innerDelete(documentId, tenantId);
    }

    private void innerDelete(String documentId, String tenantId) {
        tenantId = checkEnableMultiTenantId(tenantId);
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(documentId));
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(tenantId));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowDeleteChange rowDeleteChange = new RowDeleteChange(tableName, primaryKey);
        rowDeleteChange.setCondition(new Condition(RowExistenceExpectation.IGNORE));

        try {
            client.deleteRow(new DeleteRowRequest(rowDeleteChange));
            if (log.isDebugEnabled()) {
                log.debug("delete session, documentId:{}, tenantId:{}", documentId, tenantId);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("delete session failed, documentId:%s, tenantId:%s", documentId, tenantId), e);
        }
    }

    @Override
    public void deleteDocument(String documentId) {
        deleteDocument(documentId, null);
    }

    @Override
    public void deleteDocumentByTenant(String tenantId) {
        HashSet<String> tenantIds = new HashSet<>();
        if (tenantId != null) {
            tenantIds.add(tenantId);
        }
        deleteDocument(tenantIds, null);
    }

    @Override
    public void deleteDocument(Set<String> tenantIds, Filter metadataFilter) {
        log.info("delete document, tenantIds:{}, metadataFilter:{}", tenantIds, metadataFilter);
        String nextToken = null;
        while (true) {
            KnowledgeSearchRequest knowledgeSearchRequest = KnowledgeSearchRequest.builder()
                .tenantIds(tenantIds)
                .metadataFilter(metadataFilter)
                .limit(1000)
                .nextToken(nextToken)
                .columnsToGet(Arrays.asList(Document.DOCUMENT_DOCUMENT_ID, Document.DOCUMENT_TENANT_ID))
                .build();
            final Response<DocumentHit> response = searchDocuments(knowledgeSearchRequest);
            List<DocumentHit> hits = response.getHits();
            TablestoreHelper.batchDelete(client, tableName, hits.iterator());
            nextToken = response.getNextToken();
            if (nextToken == null) {
                break;
            }
        }
    }

    @Override
    public void deleteAllDocuments() {
        log.info("delete all documents");
        Iterator<Document> documents = listAllDocuments();
        TablestoreHelper.batchDelete(client, tableName, documents);
    }

    @Override
    public Document getDocument(String documentId, String tenantId) {
        ValidationUtils.ensureNotNull(documentId, "documentId");
        tenantId = checkEnableMultiTenantId(tenantId);

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(documentId));
        primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(tenantId));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        criteria.setMaxVersions(1);

        try {
            GetRowResponse response = client.getRow(new GetRowRequest(criteria));
            Row row = response.getRow();
            Document document = TablestoreHelper.rowToDocument(row, textField, embeddingField);
            if (log.isDebugEnabled()) {
                log.debug("get document:{}", document);
            }
            return document;
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("get document failed, documentId:%s, tenantId:%s ", documentId, tenantId), e);
        }
    }

    @Override
    public Document getDocument(String documentId) {
        String tenantId = checkEnableMultiTenantId(null);
        return getDocument(documentId, tenantId);
    }

    @Override
    public List<Document> getDocuments(List<String> documentIdList, String tenantId) {
        log.info("get documents, documentIdList:{}, tenantId:{}", documentIdList, tenantId);
        String newTenantId = checkEnableMultiTenantId(tenantId);
        List<PrimaryKey> pkList = new ArrayList<>(documentIdList.size());
        for (String docId : documentIdList) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(docId));
            primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(newTenantId));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            pkList.add(primaryKey);
        }

        List<Document> documents = TablestoreHelper.batchGetDocuments(client, tableName, pkList, textField, embeddingField);
        if (documents.size() != documentIdList.size()) {
            throw Exceptions.runtime(
                "get documents failed, documentIdList[%s]:%s, tenantId:%s, documents:%s",
                documentIdList.size(),
                documentIdList,
                newTenantId,
                documents.size()
            );
        }
        return documents;
    }

    @Override
    public List<Document> getDocuments(List<String> documentIdList) {
        return getDocuments(documentIdList, null);
    }

    @Override
    public Iterator<Document> listAllDocuments() {
        log.info("list all documents");
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.INF_MIN)
            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.INF_MAX)
            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.INF_MAX)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(
            client,
            tableName,
            row -> TablestoreHelper.rowToDocument(row, textField, embeddingField),
            start,
            end,
            null,
            Order.ASC,
            -1L,
            -1,
            null
        );
    }

    @Override
    public Response<DocumentHit> searchDocuments(KnowledgeSearchRequest searchRequest) {
        if (log.isDebugEnabled()) {
            log.debug("before search documents:{}", searchRequest);
        }
        ValidationUtils.ensureNotNull(searchRequest, "KnowledgeSearchRequest");
        Filter filter = wrapTenantIds(searchRequest.getTenantIds(), searchRequest.getMetadataFilter());
        Query query = TablestoreHelper.parserSearchFilters(filter);
        Sort otsSort = TablestoreHelper.toOtsSort(searchRequest.getSorts());
        byte[] nextToken = null;
        if (searchRequest.getNextToken() != null) {
            nextToken = Base64.getDecoder().decode(searchRequest.getNextToken());
        }
        SearchQuery searchQuery = SearchQuery.newBuilder()
            .query(query)
            .getTotalCount(false)
            .limit(searchRequest.getLimit())
            .offset(0)
            .sort(otsSort)
            .token(nextToken)
            .build();

        SearchRequest otsSearchRequest = new SearchRequest(tableName, searchIndexName, searchQuery);

        List<PrimaryKey> routingValues = getRoutingValues(searchRequest);
        if (!routingValues.isEmpty()) {
            otsSearchRequest.setRoutingValues(routingValues);
        }
        otsSearchRequest.setColumnsToGet(toColumnsToGet(searchRequest.getColumnsToGet()));

        try {
            SearchResponse searchResponse = client.search(otsSearchRequest);
            log.info("search documents:{}, request_id:{}", searchRequest, searchResponse.getRequestId());
            Triple<List<Document>, String, List<Double>> triple = TablestoreHelper.parserSearchResponse(
                searchResponse,
                r -> TablestoreHelper.rowToDocument(r, textField, embeddingField)
            );
            List<Document> documents = triple.getLeft();
            String nextTokenStr = triple.getMiddle();
            List<Double> scores = triple.getRight();
            List<DocumentHit> documentHits = new ArrayList<>(documents.size());
            for (int i = 0; i < documents.size(); i++) {
                documentHits.add(new DocumentHit(documents.get(i), scores.get(i)));
            }
            return new Response<>(documentHits, nextTokenStr);
        } catch (TableStoreException e) {
            throw Exceptions.runtimeThrowable(String.format("search documents failed, request_id:%s, query:[%s]", e.getRequestId(), searchRequest), e);
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("search documents failed, query:[%s]", searchRequest), e);
        }
    }

    @Override
    public Response<DocumentHit> fullTextSearch(
        String query,
        Set<String> tenantIds,
        int limit,
        Filter metadataFilter,
        String nextToken,
        List<String> columnsToGet
    ) {
        Filter textMatch = Filters.textMatch(textField, query);
        if (metadataFilter != null) {
            metadataFilter = Filters.and(textMatch, metadataFilter);
        } else {
            metadataFilter = textMatch;
        }
        KnowledgeSearchRequest knowledgeSearchRequest = KnowledgeSearchRequest.builder()
            .tenantIds(tenantIds)
            .metadataFilter(metadataFilter)
            .limit(limit)
            .nextToken(nextToken)
            .columnsToGet(columnsToGet)
            .sorts(Collections.singletonList(ScoreSort.builder().order(Order.DESC).build()))
            .build();
        return searchDocuments(knowledgeSearchRequest);
    }

    @Override
    public Response<DocumentHit> vectorSearch(
        float[] queryVector,
        int topK,
        Float minScore,
        Set<String> tenantIds,
        Filter metadataFilter,
        List<String> columnsToGet
    ) {
        return vectorSearch(queryVector, topK, minScore, tenantIds, metadataFilter, columnsToGet, null);
    }

    public Response<DocumentHit> vectorSearch(
        float[] queryVector,
        int topK,
        Float minScore,
        Set<String> tenantIds,
        Filter metadataFilter,
        List<String> columnsToGet,
        Map<String, Object> varArgs
    ) {
        Filter filter;
        if (varArgs != null && varArgs.containsKey(FLAG_SKIP_WRAP_TENANT_IDS)) {
            filter = metadataFilter;
        } else {
            filter = wrapTenantIds(tenantIds, metadataFilter);
        }
        VectorQuery vectorQuery = Filters.vectorQuery(embeddingField, queryVector).setTopK(topK).setFilter(filter).setMinScore(minScore);
        KnowledgeSearchRequest knowledgeSearchRequest = KnowledgeSearchRequest.builder()
            .tenantIds(null)
            .metadataFilter(vectorQuery)
            .limit(topK)
            .nextToken(null)
            .columnsToGet(columnsToGet)
            .sorts(Collections.singletonList(ScoreSort.builder().order(Order.DESC).build()))
            .varArg(FLAG_ROUTING_VALUES, buildRouting(tenantIds))
            .build();
        return searchDocuments(knowledgeSearchRequest);
    }

    @Override
    public boolean enableMultiTenant() {
        return enableMultiTenant;
    }

    @Override
    public void initTable() {
        TablestoreHelper.createTableIfNotExist(
            client,
            tableName,
            Arrays.asList(Pair.of(Document.DOCUMENT_DOCUMENT_ID, MetaType.STRING), Pair.of(Document.DOCUMENT_TENANT_ID, MetaType.STRING)),
            Collections.emptyList()
        );

        List<FieldSchema> messageSchemas = new ArrayList<>(metadataSchema);
        TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(Document.DOCUMENT_DOCUMENT_ID, FieldType.KEYWORD));
        TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(Document.DOCUMENT_TENANT_ID, FieldType.KEYWORD));
        TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(textField, FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        TablestoreHelper.addSchemaIfNotExist(
            messageSchemas,
            new FieldSchema(embeddingField, FieldType.VECTOR).setVectorOptions(
                new VectorOptions(VectorDataType.FLOAT_32, embeddingDimension, embeddingMetricType)
            )
        );
        List<String> routing = new ArrayList<>();
        if (enableMultiTenant) {
            routing.add(Document.DOCUMENT_TENANT_ID);
        }
        TablestoreHelper.createSearchIndexIfNotExist(client, tableName, searchIndexName, messageSchemas, routing);
    }

    @Override
    public void deleteTableAndIndex() {
        TablestoreHelper.deleteTable(client, tableName);
    }

    private void checkDimension(Document document) {
        if (document == null || document.getEmbedding() == null) {
            return;
        }
        int actualDimension = document.getEmbedding().length;
        if (embeddingDimension != actualDimension) {
            throw Exceptions.illegalArgument(
                "document's embedding embedding length:%s is not the same as the knowledge store dimension:%s, document id:%s",
                embeddingDimension,
                actualDimension,
                document.getDocumentId()
            );
        }
    }

    private void checkEnableMultiTenant(Document document) {
        if (document == null) {
            return;
        }
        checkEnableMultiTenantId(document.getTenantId());
    }

    private String checkEnableMultiTenantId(String tenantId) {
        if (!enableMultiTenant) {
            if (tenantId == null) {
                return Document.DOCUMENT_DEFAULT_TENANT_ID;
            }
            if (!Document.DOCUMENT_DEFAULT_TENANT_ID.equals(tenantId)) {
                throw Exceptions.illegalArgument("the multi-tenant capability is not enabled, but the 'tenant_id' is set");
            }
        } else {
            if (Document.DOCUMENT_DEFAULT_TENANT_ID.equals(tenantId) || tenantId == null) {
                throw Exceptions.illegalArgument("the multi-tenant capability is enabled, but the 'tenant_id' is not set");
            }
        }
        return tenantId;
    }

    private SearchRequest.ColumnsToGet toColumnsToGet(List<String> columnsToGet) {
        if (columnsToGet == null || columnsToGet.isEmpty()) {
            SearchRequest.ColumnsToGet otsColumnsToGet = new SearchRequest.ColumnsToGet();
            List<String> defaultColumnsToGet = getDefaultColumnsToGet();
            otsColumnsToGet.setColumns(defaultColumnsToGet);
            return otsColumnsToGet;
        } else {
            SearchRequest.ColumnsToGet otsColumnsToGet = new SearchRequest.ColumnsToGet();
            otsColumnsToGet.setColumns(columnsToGet);
            return otsColumnsToGet;
        }
    }

    private List<String> getDefaultColumnsToGet() {
        List<String> defaultColumnsToGet = new ArrayList<>();
        for (FieldSchema fieldSchema : metadataSchema) {
            if (fieldSchema.getFieldType().equals(FieldType.VECTOR) || fieldSchema.getFieldName().equals(embeddingField)) {
                continue;
            }
            defaultColumnsToGet.add(fieldSchema.getFieldName());
        }
        defaultColumnsToGet.add(Document.DOCUMENT_DOCUMENT_ID);
        defaultColumnsToGet.add(Document.DOCUMENT_TENANT_ID);
        defaultColumnsToGet.add(textField);
        return defaultColumnsToGet;
    }

    private Filter wrapTenantIds(Set<String> tenantIds, Filter metadataFilter) {
        if (enableMultiTenant) {
            if (tenantIds == null || tenantIds.isEmpty()) {
                return metadataFilter;
            }
            if (tenantIds.size() == 1) {
                if (metadataFilter == null) {
                    return Filters.eq(Document.DOCUMENT_TENANT_ID, tenantIds.iterator().next());
                }
                return Filters.and(Filters.eq(Document.DOCUMENT_TENANT_ID, tenantIds.iterator().next()), metadataFilter);
            } else {
                if (metadataFilter == null) {
                    return Filters.in(Document.DOCUMENT_TENANT_ID, new ArrayList<>(tenantIds));
                }
                return Filters.and(Filters.in(Document.DOCUMENT_TENANT_ID, new ArrayList<>(tenantIds)), metadataFilter);
            }
        } else {
            if (tenantIds == null || tenantIds.isEmpty()) {
                return metadataFilter;
            }
            throw Exceptions.illegalArgument("the multi-tenant capability is not enabled, but the 'tenant id' is set");
        }
    }

    private List<PrimaryKey> buildRouting(Set<String> tenantIds) {
        if (!enableMultiTenant) {
            if (tenantIds == null || tenantIds.isEmpty()) {
                return Collections.emptyList();
            }
            throw Exceptions.illegalArgument("the multi-tenant capability is not enabled, but the 'tenant id' is set");
        }
        if (tenantIds == null || tenantIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<PrimaryKey> routing = new ArrayList<>(tenantIds.size());
        for (String tid : tenantIds) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(tid));
            routing.add(primaryKeyBuilder.build());
        }
        return routing;
    }

    private List<PrimaryKey> getRoutingValues(KnowledgeSearchRequest searchRequest) {
        if (searchRequest == null) {
            return Collections.emptyList();
        }
        if (searchRequest.getVarArgs() != null && searchRequest.getVarArgs().containsKey(FLAG_ROUTING_VALUES)) {
            // noinspection unchecked
            return (List<PrimaryKey>) searchRequest.getVarArgs().get(FLAG_ROUTING_VALUES);
        }

        return buildRouting(searchRequest.getTenantIds());
    }

    private List<String> getTenantIds(String documentId) {
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(documentId))
            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(documentId))
            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.INF_MAX)
            .build();
        TablestoreHelper.GetRangeIterator<Document> iterator = new TablestoreHelper.GetRangeIterator<>(
            client,
            tableName,
            row -> TablestoreHelper.rowToDocument(row, textField, embeddingField),
            start,
            end,
            null,
            Order.ASC,
            -1L,
            -1,
            Arrays.asList(Document.DOCUMENT_TENANT_ID, Document.DOCUMENT_DOCUMENT_ID)
        );
        List<Document> documents = CollectionUtil.toList(iterator);
        List<String> tenantIds = new ArrayList<>();
        for (Document document : documents) {
            if (document != null && document.getTenantId() != null) {
                tenantIds.add(document.getTenantId());
            }
        }
        if (tenantIds.size() > 1) {
            log.warn("document id:{} has more than one tenant id:{}", documentId, tenantIds);
        }
        return tenantIds;
    }

    public static final String FLAG_ROUTING_VALUES = "_flag_routing_values";
    public static final String FLAG_SKIP_WRAP_TENANT_IDS = "_flag_skip_wrap_tenant_ids";
}
