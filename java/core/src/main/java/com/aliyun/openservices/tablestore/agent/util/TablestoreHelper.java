package com.aliyun.openservices.tablestore.agent.util;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateIndexRequest;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DefinedColumnSchema;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.IndexUpdateMode;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.IndexSetting;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.SearchHit;
import com.alicloud.openservices.tablestore.model.search.SearchIndexInfo;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.SyncStat;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.KnnVectorQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.DocumentHit;
import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.MetaType;
import com.aliyun.openservices.tablestore.agent.model.Metadata;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.AbstractConditionFilter;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.And;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.Not;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.Or;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.AbstractOperationFilter;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Eq;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Exists;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Gt;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Gte;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.In;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Lt;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Lte;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.NotEq;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.NotIn;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.TextMatch;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.TextMatchPhrase;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.VectorQuery;
import com.aliyun.openservices.tablestore.agent.model.sort.FieldSort;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.model.sort.ScoreSort;
import com.aliyun.openservices.tablestore.agent.model.sort.Sort;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TablestoreHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void createTableIfNotExist(
        SyncClient client,
        String tableName,
        List<Pair<String, MetaType>> primaryKeys,
        List<Pair<String, MetaType>> definedColumns
    ) {
        ListTableResponse listTableResponse = client.listTable();
        for (String name : listTableResponse.getTableNames()) {
            if (name.equals(tableName)) {
                log.warn("tablestore table:[{}] already exists", tableName);
                return;
            }
        }
        TableMeta tableMeta = new TableMeta(tableName);
        for (Pair<String, MetaType> pk : primaryKeys) {
            String name = pk.getKey();
            MetaType type = pk.getValue();
            switch (type) {
                case STRING:
                    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(name, PrimaryKeyType.STRING));
                    break;
                case INTEGER:
                    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(name, PrimaryKeyType.INTEGER));
                    break;
                case BINARY:
                    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(name, PrimaryKeyType.BINARY));
                    break;
                default:
                    throw Exceptions.illegalArgument("unsupported primary key name:%s type:%s", name, type);
            }
        }
        if (definedColumns != null) {
            for (Pair<String, MetaType> definedColumn : definedColumns) {
                String name = definedColumn.getKey();
                MetaType type = definedColumn.getValue();
                switch (type) {
                    case STRING:
                        tableMeta.addDefinedColumn(new DefinedColumnSchema(name, DefinedColumnType.STRING));
                        break;
                    case INTEGER:
                        tableMeta.addDefinedColumn(new DefinedColumnSchema(name, DefinedColumnType.INTEGER));
                        break;
                    case DOUBLE:
                        tableMeta.addDefinedColumn(new DefinedColumnSchema(name, DefinedColumnType.DOUBLE));
                        break;
                    case BOOLEAN:
                        tableMeta.addDefinedColumn(new DefinedColumnSchema(name, DefinedColumnType.BOOLEAN));
                        break;
                    case BINARY:
                        tableMeta.addDefinedColumn(new DefinedColumnSchema(name, DefinedColumnType.BINARY));
                        break;
                    default:
                        throw Exceptions.illegalArgument("unsupported defined column name:%s type:%s", name, type);
                }
            }
        }
        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        log.info("tablestore create table:[{}] successfully.", tableName);
        try {
            client.createTable(request);
            Thread.sleep(1000);
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("tablestore create table:[%s] failed", tableName), e);
        }
    }

    public static void createSecondaryIndexIfNotExist(
        SyncClient client,
        String tableName,
        String secondaryIndexName,
        List<String> primaryKeyNames,
        List<String> definedColumnNames,
        IndexType indexType
    ) {
        DescribeTableResponse describeTableResponse = client.describeTable(new DescribeTableRequest(tableName));
        List<IndexMeta> indexMetas = describeTableResponse.getIndexMeta();
        for (IndexMeta indexMeta : indexMetas) {
            if (indexMeta.getIndexName().equals(secondaryIndexName)) {
                log.warn("tablestore secondary index:[{}] already exists", secondaryIndexName);
                return;
            }
        }
        boolean includeBaseData = false;
        IndexMeta indexMeta = new IndexMeta(secondaryIndexName);
        indexMeta.setIndexUpdateMode(IndexType.IT_GLOBAL_INDEX.equals(indexType) ? IndexUpdateMode.IUM_ASYNC_INDEX : IndexUpdateMode.IUM_SYNC_INDEX);
        indexMeta.setIndexType(indexType);
        for (String primaryKeyName : primaryKeyNames) {
            indexMeta.addPrimaryKeyColumn(primaryKeyName);
        }
        for (String columnName : definedColumnNames) {
            indexMeta.addDefinedColumn(columnName);
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(tableName, indexMeta, includeBaseData);
        client.createIndex(createIndexRequest);
        log.info("tablestore create secondary index:[{}] successfully.", secondaryIndexName);
    }

    public static void deleteTable(SyncClient client, String tableName) {
        List<String> tableNames = client.listTable().getTableNames();
        if (!tableNames.contains(tableName)) {
            return;
        }
        ListSearchIndexRequest listSearchIndexRequest = new ListSearchIndexRequest();
        listSearchIndexRequest.setTableName(tableName);
        ListSearchIndexResponse listSearchIndexResponse = client.listSearchIndex(listSearchIndexRequest);
        for (SearchIndexInfo indexInfo : listSearchIndexResponse.getIndexInfos()) {
            DeleteSearchIndexRequest deleteSearchIndexRequest = new DeleteSearchIndexRequest();
            deleteSearchIndexRequest.setTableName(indexInfo.getTableName());
            deleteSearchIndexRequest.setIndexName(indexInfo.getIndexName());
            client.deleteSearchIndex(deleteSearchIndexRequest);
            log.info("tablestore delete search index:[{}] successfully.", indexInfo.getIndexName());
        }

        try {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            client.deleteTable(request);
        } catch (TableStoreException e) {
            if ("OTSObjectNotExist".equals(e.getErrorCode()) && e.getMessage().contains("does not exist")) {
                log.warn("tablestore table:[{}] not found", tableName);
            } else {
                throw Exceptions.runtimeThrowable(String.format("tablestore delete table:[%s] failed", tableName), e);
            }
        }
    }

    public static List<Column> metadataToColumns(Metadata metadata) {
        if (metadata == null) {
            return new ArrayList<>();
        }
        Map<String, Object> metadataMap = metadata.toMap();
        if (metadataMap == null || metadataMap.isEmpty()) {
            return new ArrayList<>();
        }
        List<Column> columns = new ArrayList<>(metadataMap.size());
        for (Map.Entry<String, Object> entry : metadataMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            columns.add(new Column(key, toColumnValue(value)));
        }
        return columns;
    }

    public static ColumnValue toColumnValue(Object value) {
        if (value instanceof Float) {
            return ColumnValue.fromDouble((Float) value);
        } else if (value instanceof Long) {
            return ColumnValue.fromLong((Long) value);
        } else if (value instanceof Short) {
            return ColumnValue.fromLong((Short) value);
        } else if (value instanceof Integer) {
            return ColumnValue.fromLong(((Integer) value).longValue());
        } else if (value instanceof Double) {
            return ColumnValue.fromDouble((Double) value);
        } else if (value instanceof String) {
            return ColumnValue.fromString((String) value);
        } else if (value instanceof Boolean) {
            return ColumnValue.fromBoolean((Boolean) value);
        } else if (value instanceof byte[]) {
            return ColumnValue.fromBinary((byte[]) value);
        } else {
            throw Exceptions.illegalArgument("unsupported value[%s] type:[%s]", value, value.getClass());
        }
    }

    public static Session rowToSession(Row row) {
        if (row == null) {
            return null;
        }
        PrimaryKey primaryKey = row.getPrimaryKey();
        String userId = primaryKey.getPrimaryKeyColumn(0).getValue().asString();
        Reference<Long> updateTime = new Reference<>();
        String sessionId;
        if (primaryKey.size() == 2) {
            sessionId = primaryKey.getPrimaryKeyColumn(1).getValue().asString();
        } else {
            updateTime.set(primaryKey.getPrimaryKeyColumn(1).getValue().asLong());
            sessionId = primaryKey.getPrimaryKeyColumn(2).getValue().asString();
        }
        Column[] columns = row.getColumns();
        Metadata metadata = columnsToMetadata(columns, (column -> {
            String name = column.getName();
            ColumnValue value = column.getValue();
            if (Session.SESSION_UPDATE_TIME.equals(name)) {
                updateTime.set(value.asLong());
                return true;
            }
            return false;
        }));
        Session session = new Session(userId, sessionId, updateTime.get());
        session.setMetadata(metadata);
        return session;
    }

    public static Document rowToDocument(Row row, String textField, String embeddingField) {
        if (row == null) {
            return null;
        }
        PrimaryKey primaryKey = row.getPrimaryKey();
        String documentId = primaryKey.getPrimaryKeyColumn(0).getValue().asString();
        String tenantId = primaryKey.getPrimaryKeyColumn(1).getValue().asString();
        Column[] columns = row.getColumns();
        Reference<String> text = new Reference<>();
        Reference<float[]> embedding = new Reference<>();
        Metadata metadata = columnsToMetadata(columns, (column -> {
            String name = column.getName();
            ColumnValue value = column.getValue();
            if (textField.equals(name)) {
                text.set(value.asString());
                return true;
            }
            if (embeddingField.equals(name)) {
                embedding.set(TablestoreHelper.decodeEmbedding(value.asString()));
                return true;
            }
            return false;
        }));
        Document document = new Document(documentId, tenantId);
        document.setText(text.get());
        document.setEmbedding(embedding.get());
        document.setMetadata(metadata);
        return document;
    }

    public static Message rowToMessage(Row row) {
        if (row == null) {
            return null;
        }
        PrimaryKey primaryKey = row.getPrimaryKey();
        String sessionId = primaryKey.getPrimaryKeyColumn(0).getValue().asString();
        Reference<String> content = new Reference<>();
        String messageId;
        long createTime;
        PrimaryKeyColumn primaryKey1 = primaryKey.getPrimaryKeyColumn(1);
        if (primaryKey1.getValue().getType().equals(PrimaryKeyType.STRING)) {
            messageId = primaryKey1.getValue().asString();
            createTime = primaryKey.getPrimaryKeyColumn(2).getValue().asLong();
        } else {
            createTime = primaryKey1.getValue().asLong();
            messageId = primaryKey.getPrimaryKeyColumn(2).getValue().asString();
        }
        Column[] columns = row.getColumns();
        Metadata metadata = columnsToMetadata(columns, (column -> {
            String name = column.getName();
            ColumnValue value = column.getValue();
            if (Message.MESSAGE_CONTENT.equals(name)) {
                content.set(value.asString());
                return true;
            }
            return false;
        }));
        Message message = new Message(sessionId, messageId, createTime);
        message.setContent(content.get());
        message.setMetadata(metadata);
        return message;
    }

    public static Metadata columnsToMetadata(Column[] columns, Function<Column, Boolean> specialColumnConsumer) {
        Metadata metadata = new Metadata();
        for (Column column : columns) {
            if (Boolean.TRUE.equals(specialColumnConsumer.apply(column))) {
                continue;
            }
            String name = column.getName();
            ColumnValue value = column.getValue();
            switch (value.getType()) {
                case DOUBLE:
                    metadata.put(name, value.asDouble());
                    break;
                case INTEGER:
                    metadata.put(name, value.asLong());
                    break;
                case STRING:
                    metadata.put(name, value.asString());
                    break;
                case BINARY:
                    metadata.put(name, value.asBinary());
                    break;
                case BOOLEAN:
                    metadata.put(name, value.asBoolean());
                    break;
                default:
                    throw Exceptions.illegalArgument("unsupported tablestore column name:%s type:%s", name, value.getType());
            }
        }
        return metadata;
    }

    public static class GetRangeIterator<E> implements Iterator<E> {
        private final SyncClient client;
        private final String tableName;
        private final Function<Row, E> translateFunction;
        private PrimaryKey inclusiveStartPrimaryKey;
        private final PrimaryKey exclusiveEndPrimaryKey;
        private final ColumnValueFilter metadataFilter;
        private final Direction direction;
        private final long iteratorMaxCount;
        private final int batchSize;
        private final List<String> columnToGet;

        private long count;
        private LinkedList<Row> rowsBufferList;

        public GetRangeIterator(
            SyncClient client,
            String tableName,
            Function<Row, E> translateFunction,
            PrimaryKey inclusiveStartPrimaryKey,
            PrimaryKey exclusiveEndPrimaryKey,
            Filter metadataFilter,
            Order order,
            Long iteratorMaxCount,
            Integer batchSize,
            List<String> columnToGet
        ) {
            this.client = client;
            this.tableName = tableName;
            this.translateFunction = translateFunction;
            this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
            this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
            this.metadataFilter = TablestoreHelper.parserTableFilters(metadataFilter);
            this.direction = Order.DESC.equals(order) ? Direction.BACKWARD : Direction.FORWARD;
            this.iteratorMaxCount = iteratorMaxCount == null ? -1 : iteratorMaxCount;
            this.batchSize = configBatchSize(batchSize, iteratorMaxCount, metadataFilter);
            this.columnToGet = columnToGet == null ? new ArrayList<>() : columnToGet;
            this.count = 0;
            this.rowsBufferList = new LinkedList<>();
            fetchNextBatch();
        }

        private static int configBatchSize(Integer batchSize, Long iteratorMaxCount, Filter metadataFilter) {
            if ((batchSize == null || batchSize == -1) && (iteratorMaxCount != null && iteratorMaxCount > 0)) {
                if (metadataFilter == null) {
                    return Math.max(Math.min(5000, iteratorMaxCount.intValue()), 1);
                } else {
                    return Math.max(Math.min(5000, (int) (iteratorMaxCount.intValue() * 1.3)), 1);
                }
            }
            return (batchSize == null || batchSize <= 0) ? 5000 : batchSize;
        }

        @Override
        public boolean hasNext() {
            if (iteratorMaxCount > 0 && count >= iteratorMaxCount) {
                return false;
            }
            if (bufferHasData()) {
                return true;
            }
            if (hasNextBatch()) {
                fetchNextBatch();
            }
            return bufferHasData();
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Row row = rowsBufferList.pop();
            count++;
            return translateFunction.apply(row);
        }

        private void fetchNextBatch() {
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(this.tableName);
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            rangeRowQueryCriteria.setMaxVersions(1);
            rangeRowQueryCriteria.setLimit(batchSize);
            rangeRowQueryCriteria.setDirection(direction);
            rangeRowQueryCriteria.addColumnsToGet(columnToGet);
            if (metadataFilter != null) {
                rangeRowQueryCriteria.setFilter(metadataFilter);
            }
            GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
            GetRangeResponse rangeResponse = client.getRange(getRangeRequest);
            inclusiveStartPrimaryKey = rangeResponse.getNextStartPrimaryKey();
            rowsBufferList = new LinkedList<>(rangeResponse.getRows());
        }

        private boolean bufferHasData() {
            return !rowsBufferList.isEmpty();
        }

        private boolean hasNextBatch() {
            return inclusiveStartPrimaryKey != null;
        }

        public PrimaryKey nextStartPrimaryKey() {
            if (rowsBufferList != null && !rowsBufferList.isEmpty()) {
                return rowsBufferList.peek().getPrimaryKey();
            } else {
                return inclusiveStartPrimaryKey;
            }
        }
    }

    public static <T> void batchDelete(SyncClient client, String tableName, Iterator<T> iterator) {
        List<RowChange> rowChanges = new ArrayList<>();
        while (iterator.hasNext()) {
            T item = iterator.next();
            if (item instanceof Session) {
                Session session = (Session) item;
                rowChanges.add(
                    new RowDeleteChange(
                        tableName,
                        PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(session.getUserId()))
                            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString(session.getSessionId()))
                            .build()
                    )
                );
            } else if (item instanceof Message) {
                Message message = (Message) item;
                rowChanges.add(
                    new RowDeleteChange(
                        tableName,
                        PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(message.getSessionId()))
                            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.fromLong(message.getCreateTime()))
                            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(message.getMessageId()))
                            .build()
                    )
                );
            } else if (item instanceof Document) {
                Document document = (Document) item;
                rowChanges.add(
                    new RowDeleteChange(
                        tableName,
                        PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(document.getDocumentId()))
                            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(document.getTenantId()))
                            .build()
                    )
                );
            } else if (item instanceof DocumentHit) {
                DocumentHit documentHit = (DocumentHit) item;
                Document document = documentHit.getDocument();
                rowChanges.add(
                    new RowDeleteChange(
                        tableName,
                        PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn(Document.DOCUMENT_DOCUMENT_ID, PrimaryKeyValue.fromString(document.getDocumentId()))
                            .addPrimaryKeyColumn(Document.DOCUMENT_TENANT_ID, PrimaryKeyValue.fromString(document.getTenantId()))
                            .build()
                    )
                );
            } else {
                throw Exceptions.illegalArgument("unsupported item type:%s, detail:%s", item.getClass(), item);
            }
            if (rowChanges.size() == 200) {
                batchWrite(client, rowChanges);
                rowChanges.clear();
            }
        }
        if (!rowChanges.isEmpty()) {
            batchWrite(client, rowChanges);
        }
    }

    public static void batchWrite(SyncClient client, List<RowChange> rowChanges) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (RowChange rowChange : rowChanges) {
            batchWriteRowRequest.addRowChange(rowChange);
        }
        List<String> errorDetails = new ArrayList<>();
        try {
            BatchWriteRowResponse batchWriteRowResponse = client.batchWriteRow(batchWriteRowRequest);
            if (!batchWriteRowResponse.isAllSucceed()) {
                for (BatchWriteRowResponse.RowResult rowResult : batchWriteRowResponse.getFailedRows()) {
                    PrimaryKey primaryKey = batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey();
                    errorDetails.add(String.format("failed pk:[%s], failed msg:[%s]", primaryKey.jsonize(), rowResult.getError().getMessage()));
                }
                throw Exceptions.runtime(String.format("batch write failed, error details:%s", errorDetails));
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable("batch write failed", e);
        }
    }

    public static <T> List<T> batchGetRow(SyncClient client, String tableName, List<PrimaryKey> primaryKeys, Function<Row, T> translateFunction) {
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
        for (PrimaryKey rowChange : primaryKeys) {
            multiRowQueryCriteria.addRow(rowChange);
        }
        multiRowQueryCriteria.setMaxVersions(1);

        List<String> errorDetails = new ArrayList<>();
        try {
            BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
            batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

            BatchGetRowResponse batchGetRowResponse = client.batchGetRow(batchGetRowRequest);
            if (!batchGetRowResponse.isAllSucceed()) {
                for (BatchGetRowResponse.RowResult rowResult : batchGetRowResponse.getFailedRows()) {
                    PrimaryKey primaryKey = batchGetRowRequest.getPrimaryKey(rowResult.getTableName(), rowResult.getIndex());
                    errorDetails.add(String.format("failed pk:[%s], failed msg:[%s]", primaryKey.jsonize(), rowResult.getError().getMessage()));
                }
                throw Exceptions.runtime(String.format("batch get row failed, error details:%s", errorDetails));
            }
            return batchGetRowResponse.getSucceedRows().stream().map(r -> translateFunction.apply(r.getRow())).collect(Collectors.toList());
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable("batch get row failed", e);
        }
    }

    public static ColumnValueFilter parserTableFilters(Filter metadataFilter) {
        if (metadataFilter == null) {
            return null;
        }
        if (metadataFilter instanceof AbstractConditionFilter) {
            if (metadataFilter instanceof And) {
                CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
                for (Filter filter : ((And) metadataFilter).getFilters()) {
                    compositeColumnValueFilter.addFilter(parserTableFilters(filter));
                }
                return compositeColumnValueFilter;
            } else if (metadataFilter instanceof Or) {
                CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
                for (Filter filter : ((Or) metadataFilter).getFilters()) {
                    compositeColumnValueFilter.addFilter(parserTableFilters(filter));
                }
                return compositeColumnValueFilter;
            } else if (metadataFilter instanceof Not) {
                CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
                for (Filter filter : ((Not) metadataFilter).getFilters()) {
                    compositeColumnValueFilter.addFilter(parserTableFilters(filter));
                }
                return compositeColumnValueFilter;
            } else {
                throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
            }
        } else if (metadataFilter instanceof AbstractOperationFilter) {
            return parseTableFilter((AbstractOperationFilter) metadataFilter);
        } else {
            throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
        }
    }

    public static SingleColumnValueFilter parseTableFilter(AbstractOperationFilter metadataFilter) {
        if (metadataFilter instanceof Eq) {
            Eq eq = (Eq) metadataFilter;
            return new SingleColumnValueFilter(eq.getKey(), SingleColumnValueFilter.CompareOperator.EQUAL, toColumnValue(eq.getValue()));
        } else if (metadataFilter instanceof Gt) {
            Gt gt = (Gt) metadataFilter;
            return new SingleColumnValueFilter(gt.getKey(), SingleColumnValueFilter.CompareOperator.GREATER_THAN, toColumnValue(gt.getValue()));
        } else if (metadataFilter instanceof Gte) {
            Gte gte = (Gte) metadataFilter;
            return new SingleColumnValueFilter(gte.getKey(), SingleColumnValueFilter.CompareOperator.GREATER_EQUAL, toColumnValue(gte.getValue()));
        } else if (metadataFilter instanceof Lt) {
            Lt lt = (Lt) metadataFilter;
            return new SingleColumnValueFilter(lt.getKey(), SingleColumnValueFilter.CompareOperator.LESS_THAN, toColumnValue(lt.getValue()));
        } else if (metadataFilter instanceof Lte) {
            Lte lte = (Lte) metadataFilter;
            return new SingleColumnValueFilter(lte.getKey(), SingleColumnValueFilter.CompareOperator.LESS_EQUAL, toColumnValue(lte.getValue()));
        } else if (metadataFilter instanceof NotEq) {
            NotEq notEq = (NotEq) metadataFilter;
            return new SingleColumnValueFilter(notEq.getKey(), SingleColumnValueFilter.CompareOperator.NOT_EQUAL, toColumnValue(notEq.getValue()));
        } else {
            throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
        }
    }

    public static Query parserSearchFilters(Filter metadataFilter) {
        if (metadataFilter == null) {
            return QueryBuilders.matchAll().build();
        }
        if (metadataFilter instanceof AbstractConditionFilter) {
            if (metadataFilter instanceof And) {
                BoolQuery.Builder bool = QueryBuilders.bool();
                for (Filter filter : ((And) metadataFilter).getFilters()) {
                    bool.must(parserSearchFilters(filter));
                }
                return bool.build();
            } else if (metadataFilter instanceof Or) {
                BoolQuery.Builder bool = QueryBuilders.bool();
                for (Filter filter : ((Or) metadataFilter).getFilters()) {
                    bool.should(parserSearchFilters(filter));
                }
                return bool.build();
            } else if (metadataFilter instanceof Not) {
                BoolQuery.Builder bool = QueryBuilders.bool();
                for (Filter filter : ((Not) metadataFilter).getFilters()) {
                    bool.mustNot(parserSearchFilters(filter));
                }
                return bool.build();
            } else {
                throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
            }
        } else if (metadataFilter instanceof AbstractOperationFilter) {
            return parseSearchFilter((AbstractOperationFilter) metadataFilter);
        } else {
            throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
        }
    }

    public static Query parseSearchFilter(AbstractOperationFilter metadataFilter) {
        if (metadataFilter instanceof Eq) {
            Eq eq = (Eq) metadataFilter;
            return QueryBuilders.term(eq.getKey(), eq.getValue()).build();
        } else if (metadataFilter instanceof Exists) {
            Exists op = (Exists) metadataFilter;
            return QueryBuilders.exists(op.getKey()).build();
        } else if (metadataFilter instanceof Gt) {
            Gt gt = (Gt) metadataFilter;
            return QueryBuilders.range(gt.getKey()).greaterThan(gt.getValue()).build();
        } else if (metadataFilter instanceof Gte) {
            Gte gte = (Gte) metadataFilter;
            return QueryBuilders.range(gte.getKey()).greaterThanOrEqual(gte.getValue()).build();
        } else if (metadataFilter instanceof In) {
            In in = (In) metadataFilter;
            TermsQuery.Builder terms = QueryBuilders.terms(in.getKey());
            for (Object value : in.getValues()) {
                terms.addTerm(value);
            }
            return terms.build();
        } else if (metadataFilter instanceof Lt) {
            Lt lt = (Lt) metadataFilter;
            return QueryBuilders.range(lt.getKey()).lessThan(lt.getValue()).build();
        } else if (metadataFilter instanceof Lte) {
            Lte lte = (Lte) metadataFilter;
            return QueryBuilders.range(lte.getKey()).lessThanOrEqual(lte.getValue()).build();
        } else if (metadataFilter instanceof NotEq) {
            NotEq notEq = (NotEq) metadataFilter;
            return QueryBuilders.bool().mustNot(QueryBuilders.term(notEq.getKey(), notEq.getValue()).build()).build();
        } else if (metadataFilter instanceof NotIn) {
            NotIn notIn = (NotIn) metadataFilter;
            TermsQuery.Builder terms = QueryBuilders.terms(notIn.getKey());
            for (Object value : notIn.getValues()) {
                terms.addTerm(value);
            }
            return QueryBuilders.bool().mustNot(terms.build()).build();
        } else if (metadataFilter instanceof TextMatch) {
            TextMatch match = (TextMatch) metadataFilter;
            return QueryBuilders.match(match.getKey(), match.getValue()).build();
        } else if (metadataFilter instanceof TextMatchPhrase) {
            TextMatchPhrase matchPhrase = (TextMatchPhrase) metadataFilter;
            return QueryBuilders.matchPhrase(matchPhrase.getKey(), matchPhrase.getValue()).build();
        } else if (metadataFilter instanceof VectorQuery) {
            VectorQuery vectorQuery = (VectorQuery) metadataFilter;
            KnnVectorQuery.Builder builder = QueryBuilders.knnVector(vectorQuery.getKey(), vectorQuery.getTopK(), vectorQuery.getQueryVector());
            if (vectorQuery.getFilter() != null) {
                builder.filter(parserSearchFilters(vectorQuery.getFilter()));
            }
            if (vectorQuery.getMinScore() != null) {
                builder.minScore(vectorQuery.getMinScore());
            }
            return builder.build();
        } else {
            throw Exceptions.illegalArgument("unsupported filter type:%s, filter:%s", metadataFilter.getClass(), metadataFilter);
        }
    }

    public static String encodeNextPrimaryKeyToken(PrimaryKey nextPrimaryKey) {
        List<List<Object>> primaryKeys = new ArrayList<>();
        for (PrimaryKeyColumn primaryKeyColumn : nextPrimaryKey.getPrimaryKeyColumns()) {
            PrimaryKeyValue columnValue = primaryKeyColumn.getValue();
            Object object = null;
            switch (columnValue.getType()) {
                case INTEGER:
                    object = columnValue.asLong();
                    break;
                case STRING:
                    object = columnValue.asString();
                    break;
                default:
                    throw Exceptions.illegalArgument("unsupported tablestore primaryKeyValue type:%s, value:%s", columnValue.getType(), columnValue.toString());
            }
            primaryKeys.add(Arrays.asList(primaryKeyColumn.getName(), object));
        }
        try {
            String sourceToken = MAPPER.writeValueAsString(primaryKeys);
            return Base64.getEncoder().encodeToString(sourceToken.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw Exceptions.runtimeThrowable(String.format("encode nextPrimaryKey token failed, primaryKey:%s", nextPrimaryKey), e);
        }
    }

    public static PrimaryKey decodeNextPrimaryKeyToken(String nextPrimaryKeyToken) {
        byte[] decodeBytes = Base64.getDecoder().decode(nextPrimaryKeyToken);
        String sourceToken = new String(decodeBytes, StandardCharsets.UTF_8);
        List<List<Object>> primaryKeys;
        try {
            primaryKeys = MAPPER.readValue(sourceToken, new TypeReference<List<List<Object>>>() {
            });
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("decode nextPrimaryKey token failed, token:%s", nextPrimaryKeyToken), e);
        }
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (List<Object> primaryKeyList : primaryKeys) {
            if (primaryKeyList.size() != 2) {
                throw Exceptions.illegalArgument("invalid primaryKey size:%s, primaryKey:%s", primaryKeyList.size(), primaryKeyList);
            }
            String name = (String) primaryKeyList.get(0);
            Object object = primaryKeyList.get(1);
            if (object instanceof String) {
                primaryKeyBuilder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromString(object.toString()));
            } else if (object instanceof Number) {
                primaryKeyBuilder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromLong(((Number) object).longValue()));
            } else {
                throw Exceptions.illegalArgument("unsupported primaryKeyValue type:%s, value:%s", object.getClass(), object);
            }
        }
        return primaryKeyBuilder.build();
    }

    public static void addSchemaIfNotExist(List<FieldSchema> schemas, FieldSchema newSchema) {
        if (schemas == null) {
            return;
        }
        for (FieldSchema schema : schemas) {
            if (schema.getFieldName().equals(newSchema.getFieldName())) {
                return;
            }
        }
        schemas.add(newSchema);
    }

    public static void createSearchIndexIfNotExist(
        SyncClient client,
        String tableName,
        String searchIndexName,
        List<FieldSchema> schemas,
        List<String> routingFields
    ) {
        ListSearchIndexRequest listSearchIndexRequest = new ListSearchIndexRequest();
        listSearchIndexRequest.setTableName(tableName);
        ListSearchIndexResponse listSearchIndexResponse = client.listSearchIndex(listSearchIndexRequest);
        for (SearchIndexInfo indexInfo : listSearchIndexResponse.getIndexInfos()) {
            if (indexInfo.getIndexName().equals(searchIndexName)) {
                log.warn("search index already exist, tableName:{}, searchIndexName:{}", tableName, searchIndexName);
                return;
            }
        }
        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(tableName);
        request.setIndexName(searchIndexName);
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(schemas);
        if (routingFields != null && !routingFields.isEmpty()) {
            IndexSetting indexSetting = new IndexSetting();
            indexSetting.setRoutingFields(routingFields);
            indexSchema.setIndexSetting(indexSetting);
        }
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
        log.info("create search index:{}, tableName:{}", searchIndexName, tableName);
    }

    public static com.alicloud.openservices.tablestore.model.search.sort.Sort toOtsSort(List<Sort> sorts) {
        if (sorts == null || sorts.isEmpty()) {
            return null;
        }
        List<com.alicloud.openservices.tablestore.model.search.sort.Sort.Sorter> sorters = new ArrayList<>();
        for (Sort sort : sorts) {
            if (sort instanceof FieldSort) {
                FieldSort fieldSort = (FieldSort) sort;
                sorters.add(
                    new com.alicloud.openservices.tablestore.model.search.sort.FieldSort(
                        fieldSort.getField(),
                        fieldSort.getOrder().equals(Order.ASC) ? SortOrder.ASC : SortOrder.DESC
                    )
                );
            } else if (sort instanceof ScoreSort) {
                ScoreSort s = (ScoreSort) sort;
                SortOrder order = s.getOrder().equals(Order.ASC) ? SortOrder.ASC : SortOrder.DESC;
                com.alicloud.openservices.tablestore.model.search.sort.ScoreSort scoreSort =
                    new com.alicloud.openservices.tablestore.model.search.sort.ScoreSort();
                scoreSort.setOrder(order);
                sorters.add(scoreSort);
            } else {
                throw Exceptions.illegalArgument("unsupported sorter type:%s, sorter:%s", sort.getClass(), sort);
            }
        }
        return new com.alicloud.openservices.tablestore.model.search.sort.Sort(sorters);
    }

    public static List<Document> batchGetDocuments(SyncClient client, String tableName, List<PrimaryKey> pkList, String textField, String embeddingField) {
        List<Document> documents = new ArrayList<>(pkList.size());
        int batchSize = 100;
        int total = pkList.size();
        for (int start = 0; start < total; start += batchSize) {
            int end = Math.min(start + batchSize, total);
            List<PrimaryKey> currentBatch = pkList.subList(start, end);
            List<Document> processed = batchGetRow(client, tableName, currentBatch, r -> TablestoreHelper.rowToDocument(r, textField, embeddingField));
            documents.addAll(processed);
        }
        return documents;
    }

    public static <T> Triple<List<T>, String, List<Double>> parserSearchResponse(SearchResponse searchResponse, Function<Row, T> rowToInstance) {
        List<T> list = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        String nextToken = null;
        if (searchResponse.getNextToken() != null) {
            nextToken = Base64.getEncoder().encodeToString(searchResponse.getNextToken());
        }
        for (SearchHit searchHit : searchResponse.getSearchHits()) {
            Row row = searchHit.getRow();
            T instance = rowToInstance.apply(row);
            list.add(instance);
            scores.add(searchHit.getScore());
        }
        return Triple.of(list, nextToken, scores);
    }

    public static String encodeEmbedding(float[] embedding) {
        try {
            return MAPPER.writeValueAsString(embedding);
        } catch (JsonProcessingException e) {
            throw Exceptions.runtimeThrowable(String.format("encode embedding failed, embedding:%s", Arrays.toString(embedding)), e);
        }
    }

    public static float[] decodeEmbedding(String embedding) {
        try {
            return MAPPER.readValue(embedding, float[].class);
        } catch (JsonProcessingException e) {
            throw Exceptions.runtimeThrowable(String.format("decode embedding failed, embedding:%s", embedding), e);
        }
    }

    public static void waitSearchIndexReady(SyncClient client, String tableName, String indexName, int totalCount) {
        long maxWaitTime = 300 * 1000_0000_000L;// 300s
        long startTime = System.nanoTime();
        while (System.nanoTime() - startTime < maxWaitTime) {
            SearchRequest searchRequest = SearchRequest.newBuilder()
                .tableName(tableName)
                .indexName(indexName)
                .searchQuery(SearchQuery.newBuilder().query(QueryBuilders.matchAll()).limit(0).getTotalCount(true).build())
                .build();
            SearchResponse searchResponse = client.search(searchRequest);
            if (searchResponse.getTotalCount() == totalCount) {
                log.info("search index ready, use: {}s", (System.nanoTime() - startTime) / 1000_000_000L);
                return;
            }
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw Exceptions.runtime("search index not ready");
    }

    public static void waitSearchIndexIncPhrase(SyncClient client, String tableName, String indexName) {
        long maxWaitTime = 300 * 1000_0000_000L;// 300s
        long startTime = System.nanoTime();
        while (System.nanoTime() - startTime < maxWaitTime) {
            DescribeSearchIndexRequest request = new DescribeSearchIndexRequest();
            request.setTableName(tableName);
            request.setIndexName(indexName);
            DescribeSearchIndexResponse searchResponse = client.describeSearchIndex(request);
            SyncStat syncStat = searchResponse.getSyncStat();
            if (syncStat.getSyncPhase().equals(SyncStat.SyncPhase.INCR)
                && Math.abs(syncStat.getCurrentSyncTimestamp() / 1000 / 1000 - System.currentTimeMillis()) <= 10_000) {
                log.info("search index inc phrase ready, use: {}s", (System.nanoTime() - startTime) / 1000_000_000L);
                return;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw Exceptions.runtime("search index not ready");
    }

    public static String maxOrNull(String str, int max, String end) {
        if (str == null) {
            return null;
        }
        if (str.length() <= max) {
            return str;
        }
        return str.substring(0, max) + "......" + end;
    }
}
