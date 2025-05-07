package com.aliyun.openservices.tablestore.agent.memory;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.IndexType;
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
import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.MetaType;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
@Getter
public class MemoryStoreImpl implements MemoryStore {

    @NonNull
    private final SyncClient client;
    @Builder.Default
    @NonNull
    private String sessionTableName = "session";
    @Builder.Default
    @NonNull
    private String sessionSecondaryIndexName = "session_secondary_index";
    @Builder.Default
    @NonNull
    private List<Pair<String, MetaType>> sessionSecondaryIndexMeta = Collections.emptyList();
    @Builder.Default
    @NonNull
    private String sessionSearchIndexName = "session_search_index_name";
    @Builder.Default
    @NonNull
    private List<FieldSchema> sessionSearchIndexSchema = Collections.emptyList();
    @Builder.Default
    @NonNull
    private String messageTableName = "message";
    @Builder.Default
    @NonNull
    private String messageSearchIndexName = "message_search_index";
    @Builder.Default
    @NonNull
    private String messageSecondaryIndexName = "message_secondary_index";
    @Builder.Default
    @NonNull
    private List<FieldSchema> messageSearchIndexSchema = Collections.emptyList();

    @Override
    public void putSession(Session session) {
        ValidationUtils.ensureNotNull(session, "session");
        ValidationUtils.ensureNotNull(session.getUserId(), "userId");
        ValidationUtils.ensureNotNull(session.getSessionId(), "sessionId");
        ValidationUtils.ensureGreaterThanAndEqualZero(session.getUpdateTime(), "updateTime");

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(session.getUserId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString(session.getSessionId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowPutChange rowPutChange = new RowPutChange(sessionTableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(session.getMetadata());
        columns.add(new Column(Session.SESSION_UPDATE_TIME, ColumnValue.fromLong(session.getUpdateTime())));
        rowPutChange.addColumns(columns);
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            if (log.isDebugEnabled()) {
                log.debug("put session:{}", session);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("put session:%s failed", session), e);
        }
    }

    @Override
    public void updateSession(Session session) {
        ValidationUtils.ensureNotNull(session, "session");
        ValidationUtils.ensureNotNull(session.getUserId(), "userId");
        ValidationUtils.ensureNotNull(session.getSessionId(), "sessionId");
        ValidationUtils.ensureGreaterThanAndEqualZero(session.getUpdateTime(), "updateTime");

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(session.getUserId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString(session.getSessionId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowUpdateChange change = new RowUpdateChange(sessionTableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(session.getMetadata());
        columns.add(new Column(Session.SESSION_UPDATE_TIME, ColumnValue.fromLong(session.getUpdateTime())));
        change.put(columns);
        try {
            client.updateRow(new UpdateRowRequest(change));
            if (log.isDebugEnabled()) {
                log.debug("update session:{}", session);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("update session:%s failed", session), e);
        }
    }

    @Override
    public void deleteSession(String userId, String sessionId) {
        ValidationUtils.ensureNotNull(userId, "userId");
        ValidationUtils.ensureNotNull(sessionId, "sessionId");

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId));
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString(sessionId));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowDeleteChange rowDeleteChange = new RowDeleteChange(sessionTableName, primaryKey);
        rowDeleteChange.setCondition(new Condition(RowExistenceExpectation.IGNORE));

        try {
            client.deleteRow(new DeleteRowRequest(rowDeleteChange));
            if (log.isDebugEnabled()) {
                log.debug("delete session, userId:{}, sessionId:{}", userId, sessionId);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("delete session failed, userId:%s, sessionId:%s", userId, sessionId), e);
        }
    }

    @Override
    public void deleteSessions(String userId) {
        ValidationUtils.ensureNotNull(userId, "userId");
        log.info("delete sessions, userId:{}", userId);
        Iterator<Session> iterator = listSessions(userId, null, -1L, 5000);
        TablestoreHelper.batchDelete(client, sessionTableName, iterator);
    }

    @Override
    public void deleteSessionAndMessages(String userId, String sessionId) {
        log.info("delete session and messages, userId:{}, sessionId:{}", userId, sessionId);
        deleteSessions(userId);
        deleteMessages(sessionId);
    }

    @Override
    public void deleteAllSessions() {
        log.info("delete all sessions");
        TablestoreHelper.batchDelete(client, sessionTableName, listAllSessions());
    }

    @Override
    public Session getSession(String userId, String sessionId) {
        ValidationUtils.ensureNotNull(userId, "userId");
        ValidationUtils.ensureNotNull(sessionId, "sessionId");

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId));
        primaryKeyBuilder.addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString(sessionId));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(sessionTableName, primaryKey);
        criteria.setMaxVersions(1);
        try {
            GetRowResponse response = client.getRow(new GetRowRequest(criteria));
            Row row = response.getRow();
            Session session = TablestoreHelper.rowToSession(row);
            if (log.isDebugEnabled()) {
                log.debug("get session:{}", session);
            }
            return session;
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("get session failed, userId:%s, sessionId:%s ", userId, sessionId), e);
        }
    }

    @Override
    public Iterator<Session> listAllSessions() {
        log.info("list all sessions");
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.INF_MIN)
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.INF_MAX)
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MAX)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(client, sessionTableName, TablestoreHelper::rowToSession, start, end, null, Order.ASC, -1L, 5000, null);
    }

    @Override
    public Iterator<Session> listSessions(String userId, Filter metadataFilter, Long maxCount, Integer batchSize) {
        log.info("list sessions, userId:{}, metadataFilter:{}, maxCount:{}, batchSize:{}", userId, metadataFilter, maxCount, batchSize);
        ValidationUtils.ensureNotNull(userId, "userId");
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MAX)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(
            client,
            sessionTableName,
            TablestoreHelper::rowToSession,
            start,
            end,
            metadataFilter,
            Order.ASC,
            maxCount,
            batchSize,
            null
        );
    }

    @Override
    public Iterator<Session> listRecentSessions(
        String userId,
        Filter metadataFilter,
        Long inclusiveStartUpdateTime,
        Long inclusiveEndUpdateTime,
        Long maxCount,
        Integer batchSize
    ) {
        log.info(
            "list recent sessions, userId:{}, metadataFilter:{}, inclusiveStartUpdateTime:{}, inclusiveEndUpdateTime:{}, maxCount:{}, batchSize:{}",
            userId,
            metadataFilter,
            inclusiveStartUpdateTime,
            inclusiveEndUpdateTime,
            maxCount,
            batchSize
        );
        ValidationUtils.ensureNotNull(userId, "userId");
        if (inclusiveStartUpdateTime != null && inclusiveEndUpdateTime != null && inclusiveStartUpdateTime < inclusiveEndUpdateTime) {
            throw Exceptions.illegalArgument(
                "inclusiveStartUpdateTime must be greater than inclusiveEndUpdateTime, because the results are returned in reverse order of update time"
            );
        }
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(
                Session.SESSION_UPDATE_TIME,
                inclusiveStartUpdateTime == null ? PrimaryKeyValue.INF_MAX : PrimaryKeyValue.fromLong(inclusiveStartUpdateTime)
            )
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MAX)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(
                Session.SESSION_UPDATE_TIME,
                inclusiveEndUpdateTime == null ? PrimaryKeyValue.INF_MIN : PrimaryKeyValue.fromLong(inclusiveEndUpdateTime)
            )
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MIN)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(
            client,
            sessionSecondaryIndexName,
            TablestoreHelper::rowToSession,
            start,
            end,
            metadataFilter,
            Order.DESC,
            maxCount,
            batchSize,
            null
        );
    }

    @Override
    public Response<Session> listRecentSessionsPaginated(
        String userId,
        int pageSize,
        Filter metadataFilter,
        Long inclusiveStartUpdateTime,
        Long inclusiveEndUpdateTime,
        String nextToken,
        Integer batchSize
    ) {
        log.info(
            "list recent sessions paginated, userId:{}, pageSize:{}, metadataFilter:{}, inclusiveStartUpdateTime:{}, inclusiveEndUpdateTime:{}, nextToken:{}, batchSize:{}",
            userId,
            pageSize,
            metadataFilter,
            inclusiveStartUpdateTime,
            inclusiveEndUpdateTime,
            nextToken,
            batchSize
        );
        ValidationUtils.ensureNotNull(userId, "userId");
        if (inclusiveStartUpdateTime != null && inclusiveEndUpdateTime != null && inclusiveStartUpdateTime < inclusiveEndUpdateTime) {
            throw Exceptions.illegalArgument(
                "inclusiveStartUpdateTime must be greater than inclusiveEndUpdateTime, because the results are returned in reverse order of update time"
            );
        }
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(
                Session.SESSION_UPDATE_TIME,
                inclusiveStartUpdateTime == null ? PrimaryKeyValue.INF_MAX : PrimaryKeyValue.fromLong(inclusiveStartUpdateTime)
            )
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MAX)
            .build();
        if (nextToken != null) {
            start = TablestoreHelper.decodeNextPrimaryKeyToken(nextToken);
        }
        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString(userId))
            .addPrimaryKeyColumn(
                Session.SESSION_UPDATE_TIME,
                inclusiveEndUpdateTime == null ? PrimaryKeyValue.INF_MIN : PrimaryKeyValue.fromLong(inclusiveEndUpdateTime)
            )
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.INF_MIN)
            .build();
        TablestoreHelper.GetRangeIterator<Session> rangeIterator = new TablestoreHelper.GetRangeIterator<>(
            client,
            sessionSecondaryIndexName,
            TablestoreHelper::rowToSession,
            start,
            end,
            metadataFilter,
            Order.DESC,
            (long) pageSize,
            batchSize,
            null
        );
        List<Session> sessions = CollectionUtil.toList(rangeIterator);
        PrimaryKey nextStartPrimaryKey = rangeIterator.nextStartPrimaryKey();
        String token = nextStartPrimaryKey == null ? null : TablestoreHelper.encodeNextPrimaryKeyToken(nextStartPrimaryKey);
        return new Response<>(sessions, token);
    }

    @Override
    public Response<Session> searchSessions(MemorySearchRequest searchRequest) {
        if (log.isDebugEnabled()) {
            log.debug("before search sessions:{}", searchRequest);
        }
        ValidationUtils.ensureNotNull(searchRequest, "MemorySearchRequest");
        Query query = TablestoreHelper.parserSearchFilters(searchRequest.getMetadataFilter());
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
        SearchRequest otsSearchRequest = SearchRequest.newBuilder()
            .tableName(sessionTableName)
            .indexName(sessionSearchIndexName)
            .searchQuery(searchQuery)
            .returnAllColumns(true)
            .build();
        try {
            SearchResponse searchResponse = client.search(otsSearchRequest);
            log.info("search sessions:{}, request_id:{}", searchRequest, searchResponse.getRequestId());
            Triple<List<Session>, String, List<Double>> triple = TablestoreHelper.parserSearchResponse(searchResponse, TablestoreHelper::rowToSession);
            List<Session> sessions = triple.getLeft();
            String nextTokenStr = triple.getMiddle();
            return new Response<>(sessions, nextTokenStr);
        } catch (TableStoreException e) {
            throw Exceptions.runtimeThrowable(String.format("search sessions failed, request_id:%s, query:[%s]", e.getRequestId(), searchRequest), e);
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("search sessions failed, query:[%s]", searchRequest), e);
        }
    }

    @Override
    public void putMessage(Message message) {
        ValidationUtils.ensureNotNull(message, "message");
        ValidationUtils.ensureNotNull(message.getSessionId(), "sessionId");
        ValidationUtils.ensureNotNull(message.getMessageId(), "messageId");
        ValidationUtils.ensureGreaterThanAndEqualZero(message.getCreateTime(), "createTime");

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(message.getSessionId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.fromLong(message.getCreateTime()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(message.getMessageId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowPutChange rowPutChange = new RowPutChange(messageTableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(message.getMetadata());
        if (message.getContent() != null) {
            columns.add(new Column(Message.MESSAGE_CONTENT, ColumnValue.fromString(message.getContent())));
        }
        rowPutChange.addColumns(columns);
        try {
            client.putRow(new PutRowRequest(rowPutChange));
            if (log.isDebugEnabled()) {
                log.debug("put message:{}", message);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("put message:%s failed", message), e);
        }
    }

    @Override
    public void updateMessage(Message message) {
        ValidationUtils.ensureNotNull(message, "message");
        ValidationUtils.ensureNotNull(message.getSessionId(), "sessionId");
        ValidationUtils.ensureNotNull(message.getMessageId(), "messageId");
        if (message.getCreateTime() == null) {
            Long createTimeFromSecondaryIndex = getMessageCreateTimeFromSecondaryIndex(message.getSessionId(), message.getMessageId());
            if (createTimeFromSecondaryIndex == null) {
                throw Exceptions.illegalArgument(
                    "message is not exist because createTime is null and can't find in secondaryIndex, sessionId:%s, messageId:%s",
                    message.getSessionId(),
                    message.getMessageId()
                );
            }
            message.setCreateTime(createTimeFromSecondaryIndex);
        }
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(message.getSessionId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.fromLong(message.getCreateTime()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(message.getMessageId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        RowUpdateChange change = new RowUpdateChange(messageTableName, primaryKey);
        List<Column> columns = TablestoreHelper.metadataToColumns(message.getMetadata());
        if (message.getContent() != null) {
            columns.add(new Column(Message.MESSAGE_CONTENT, ColumnValue.fromString(message.getContent())));
        }
        change.put(columns);
        try {
            client.updateRow(new UpdateRowRequest(change));
            if (log.isDebugEnabled()) {
                log.debug("update message:{}", message);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("update message:%s failed", message), e);
        }
    }

    @Override
    public void deleteMessage(Message message) {
        ValidationUtils.ensureNotNull(message, "message");
        ValidationUtils.ensureNotNull(message.getSessionId(), "sessionId");
        ValidationUtils.ensureNotNull(message.getMessageId(), "messageId");
        if (message.getCreateTime() == null) {
            Long createTimeFromSecondaryIndex = getMessageCreateTimeFromSecondaryIndex(message.getSessionId(), message.getMessageId());
            if (createTimeFromSecondaryIndex == null) {
                return;
            }
            message.setCreateTime(createTimeFromSecondaryIndex);
        }

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(message.getSessionId()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.fromLong(message.getCreateTime()));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(message.getMessageId()));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowDeleteChange rowDeleteChange = new RowDeleteChange(messageTableName, primaryKey);
        rowDeleteChange.setCondition(new Condition(RowExistenceExpectation.IGNORE));

        try {
            client.deleteRow(new DeleteRowRequest(rowDeleteChange));
            if (log.isDebugEnabled()) {
                log.debug("delete message:{}", message);
            }
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("delete message failed, message:%s", message), e);
        }
    }

    @Override
    public void deleteMessage(String sessionId, String messageId, Long createTime) {
        Message message = new Message(sessionId, messageId, createTime);
        deleteMessage(message);
    }

    @Override
    public void deleteMessages(String sessionId) {
        log.info("delete messages, sessionId:{}", sessionId);
        Iterator<Message> messages = listMessages(sessionId);
        TablestoreHelper.batchDelete(client, messageTableName, messages);
    }

    @Override
    public void deleteAllMessages() {
        log.info("delete all messages");
        Iterator<Message> allMessages = listAllMessages();
        TablestoreHelper.batchDelete(client, messageTableName, allMessages);
    }

    @Override
    public Message getMessage(String sessionId, String messageId, Long createTime) {
        ValidationUtils.ensureNotNull(sessionId, "sessionId");
        ValidationUtils.ensureNotNull(messageId, "messageId");
        if (createTime == null) {
            Long createTimeFromSecondaryIndex = getMessageCreateTimeFromSecondaryIndex(sessionId, messageId);
            if (createTimeFromSecondaryIndex == null) {
                return null;
            }
            createTime = createTimeFromSecondaryIndex;
        }

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.fromLong(createTime));
        primaryKeyBuilder.addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(messageId));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(messageTableName, primaryKey);
        criteria.setMaxVersions(1);
        try {
            GetRowResponse response = client.getRow(new GetRowRequest(criteria));
            Row row = response.getRow();
            Message message = TablestoreHelper.rowToMessage(row);
            if (log.isDebugEnabled()) {
                log.debug("get message:{}", message);
            }
            return message;
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(
                String.format("get message failed, sessionId:%s, createTime:%s, messageId:%s", sessionId, createTime, messageId),
                e
            );
        }
    }

    @Override
    public Iterator<Message> listAllMessages() {
        log.info("list all messages");
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.INF_MIN)
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.INF_MIN)
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.INF_MAX)
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.INF_MAX)
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.INF_MAX)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(client, messageTableName, TablestoreHelper::rowToMessage, start, end, null, Order.ASC, -1L, 5000, null);
    }

    @Override
    public Iterator<Message> listMessages(String sessionId) {
        return listMessages(sessionId, null, null, null, Order.ASC, null, null);
    }

    @Override
    public Iterator<Message> listMessages(
        String sessionId,
        Filter metadataFilter,
        Long inclusiveStartCreateTime,
        Long inclusiveEndCreateTime,
        Order order,
        Long maxCount,
        Integer batchSize
    ) {
        log.info(
            "list messages, sessionId:{}, metadataFilter:{}, inclusiveStartCreateTime:{}, inclusiveEndCreateTime:{}, order:{}, maxCount:{}, batchSize:{}",
            sessionId,
            metadataFilter,
            inclusiveStartCreateTime,
            inclusiveEndCreateTime,
            order,
            maxCount,
            batchSize
        );
        ValidationUtils.ensureNotNull(sessionId, "sessionId");
        if (inclusiveStartCreateTime != null || inclusiveEndCreateTime != null) {
            if (order == null) {
                throw Exceptions.illegalArgument("order is required when inclusiveStartCreateTime or inclusiveEndCreateTime is specified");
            }
        } else {
            order = Order.DESC;
        }
        if (inclusiveStartCreateTime != null && inclusiveEndCreateTime != null) {
            if (Order.DESC.equals(order) && inclusiveStartCreateTime < inclusiveEndCreateTime) {
                throw Exceptions.illegalArgument(
                    "inclusiveStartUpdateTime must be greater than inclusiveEndUpdateTime, because the results are returned in reverse order of update time"
                );
            }

            if (Order.ASC.equals(order) && inclusiveStartCreateTime > inclusiveEndCreateTime) {
                throw Exceptions.illegalArgument(
                    "inclusiveStartUpdateTime must be less than inclusiveEndUpdateTime, because the results are returned in order of update time"
                );
            }
        }
        PrimaryKeyValue constMin;
        PrimaryKeyValue constMax;
        if (Order.ASC.equals(order)) {
            constMin = PrimaryKeyValue.INF_MIN;
            constMax = PrimaryKeyValue.INF_MAX;
        } else {
            constMin = PrimaryKeyValue.INF_MAX;
            constMax = PrimaryKeyValue.INF_MIN;
        }
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, inclusiveStartCreateTime == null ? constMin : PrimaryKeyValue.fromLong(inclusiveStartCreateTime))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, constMin)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, inclusiveEndCreateTime == null ? constMax : PrimaryKeyValue.fromLong(inclusiveEndCreateTime))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, constMax)
            .build();
        return new TablestoreHelper.GetRangeIterator<>(
            client,
            messageTableName,
            TablestoreHelper::rowToMessage,
            start,
            end,
            metadataFilter,
            order,
            maxCount,
            batchSize,
            null
        );
    }

    @Override
    public Response<Message> listMessagesPaginated(
        String sessionId,
        int pageSize,
        Filter metadataFilter,
        Long inclusiveStartCreateTime,
        Long inclusiveEndCreateTime,
        Order order,
        String nextToken,
        Integer batchSize
    ) {
        log.info(
            "list messages paginated, sessionId:{}, pageSize:{}, metadataFilter:{}, inclusiveStartCreateTime:{}, inclusiveEndCreateTime:{}, order:{}, nextToken:{}, batchSize:{}",
            sessionId,
            pageSize,
            metadataFilter,
            inclusiveStartCreateTime,
            inclusiveEndCreateTime,
            order,
            nextToken,
            batchSize
        );
        ValidationUtils.ensureNotNull(sessionId, "sessionId");
        if (inclusiveStartCreateTime != null || inclusiveEndCreateTime != null) {
            if (order == null) {
                throw Exceptions.illegalArgument("order is required when inclusiveStartCreateTime or inclusiveEndCreateTime is specified");
            }
        } else {
            order = Order.DESC;
        }
        if (inclusiveStartCreateTime != null && inclusiveEndCreateTime != null) {
            if (Order.DESC.equals(order) && inclusiveStartCreateTime < inclusiveEndCreateTime) {
                throw Exceptions.illegalArgument(
                    "inclusiveStartUpdateTime must be greater than inclusiveEndUpdateTime, because the results are returned in reverse order of update time"
                );
            }

            if (Order.ASC.equals(order) && inclusiveStartCreateTime > inclusiveEndCreateTime) {
                throw Exceptions.illegalArgument(
                    "inclusiveStartUpdateTime must be less than inclusiveEndUpdateTime, because the results are returned in order of update time"
                );
            }
        }
        PrimaryKeyValue constMin;
        PrimaryKeyValue constMax;
        if (Order.ASC.equals(order)) {
            constMin = PrimaryKeyValue.INF_MIN;
            constMax = PrimaryKeyValue.INF_MAX;
        } else {
            constMin = PrimaryKeyValue.INF_MAX;
            constMax = PrimaryKeyValue.INF_MIN;
        }
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, inclusiveStartCreateTime == null ? constMin : PrimaryKeyValue.fromLong(inclusiveStartCreateTime))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, constMin)
            .build();
        if (nextToken != null) {
            start = TablestoreHelper.decodeNextPrimaryKeyToken(nextToken);
        }
        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, inclusiveEndCreateTime == null ? constMax : PrimaryKeyValue.fromLong(inclusiveEndCreateTime))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, constMax)
            .build();
        TablestoreHelper.GetRangeIterator<Message> rangeIterator = new TablestoreHelper.GetRangeIterator<>(
            client,
            messageTableName,
            TablestoreHelper::rowToMessage,
            start,
            end,
            metadataFilter,
            order,
            (long) pageSize,
            batchSize,
            null
        );
        List<Message> messages = CollectionUtil.toList(rangeIterator);
        PrimaryKey nextStartPrimaryKey = rangeIterator.nextStartPrimaryKey();
        String token = nextStartPrimaryKey == null ? null : TablestoreHelper.encodeNextPrimaryKeyToken(nextStartPrimaryKey);
        return new Response<>(messages, token);
    }

    @Override
    public Response<Message> searchMessages(MemorySearchRequest searchRequest) {
        if (log.isDebugEnabled()) {
            log.debug("before search messages:{}", searchRequest);
        }
        ValidationUtils.ensureNotNull(searchRequest, "MemorySearchRequest");
        Query query = TablestoreHelper.parserSearchFilters(searchRequest.getMetadataFilter());
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
        SearchRequest otsSearchRequest = SearchRequest.newBuilder()
            .tableName(messageTableName)
            .indexName(messageSearchIndexName)
            .searchQuery(searchQuery)
            .returnAllColumns(true)
            .build();
        try {
            SearchResponse searchResponse = client.search(otsSearchRequest);
            log.info("search messages:{}, request_id:{}", searchRequest, searchResponse.getRequestId());
            Triple<List<Message>, String, List<Double>> triple = TablestoreHelper.parserSearchResponse(searchResponse, TablestoreHelper::rowToMessage);
            List<Message> messages = triple.getLeft();
            String nextTokenStr = triple.getMiddle();
            return new Response<>(messages, nextTokenStr);
        } catch (TableStoreException e) {
            throw Exceptions.runtimeThrowable(String.format("search messages failed, request_id:%s, query:[%s]", e.getRequestId(), searchRequest), e);
        } catch (Exception e) {
            throw Exceptions.runtimeThrowable(String.format("search messages failed, query:[%s]", searchRequest), e);
        }
    }

    @Override
    public void initTable() {
        // Create Session table
        List<Pair<String, MetaType>> sessionDefinedColumns = new ArrayList<>(sessionSecondaryIndexMeta);
        if (sessionSecondaryIndexMeta.stream().noneMatch(s -> Session.SESSION_UPDATE_TIME.equals(s.getKey()))) {
            sessionDefinedColumns.add(Pair.of(Session.SESSION_UPDATE_TIME, MetaType.INTEGER));
        } else {
            throw Exceptions.illegalArgument("sessionSecondaryIndexMeta can't contains 'update_time'");
        }
        TablestoreHelper.createTableIfNotExist(
            client,
            sessionTableName,
            Arrays.asList(Pair.of(Session.SESSION_USER_ID, MetaType.STRING), Pair.of(Session.SESSION_SESSION_ID, MetaType.STRING)),
            sessionDefinedColumns
        );

        // Create Session secondary index
        List<String> definedColumnNames = sessionSecondaryIndexMeta.stream().map(Pair::getKey).collect(Collectors.toList());
        TablestoreHelper.createSecondaryIndexIfNotExist(
            client,
            sessionTableName,
            sessionSecondaryIndexName,
            Arrays.asList(Session.SESSION_USER_ID, Session.SESSION_UPDATE_TIME, Session.SESSION_SESSION_ID),
            definedColumnNames,
            IndexType.IT_LOCAL_INDEX
        );

        // Create Message table
        TablestoreHelper.createTableIfNotExist(
            client,
            messageTableName,
            Arrays.asList(
                Pair.of(Message.MESSAGE_SESSION_ID, MetaType.STRING),
                Pair.of(Message.MESSAGE_CREATE_TIME, MetaType.INTEGER),
                Pair.of(Message.MESSAGE_MESSAGE_ID, MetaType.STRING)
            ),
            Collections.emptyList()
        );

        // Create secondary index for Message table
        TablestoreHelper.createSecondaryIndexIfNotExist(
            client,
            messageTableName,
            messageSecondaryIndexName,
            Arrays.asList(Message.MESSAGE_SESSION_ID, Message.MESSAGE_MESSAGE_ID, Message.MESSAGE_CREATE_TIME),
            Collections.emptyList(),
            IndexType.IT_LOCAL_INDEX
        );
    }

    @Override
    public void initSearchIndex() {
        if (sessionSearchIndexSchema == null || sessionSearchIndexSchema.isEmpty()) {
            log.warn("skip create session search index because sessionSearchIndexSchema is empty");
        } else {
            List<FieldSchema> sessionSchemas = new ArrayList<>(sessionSearchIndexSchema);
            TablestoreHelper.addSchemaIfNotExist(sessionSchemas, new FieldSchema(Session.SESSION_USER_ID, FieldType.KEYWORD));
            TablestoreHelper.addSchemaIfNotExist(sessionSchemas, new FieldSchema(Session.SESSION_SESSION_ID, FieldType.KEYWORD));
            TablestoreHelper.addSchemaIfNotExist(sessionSchemas, new FieldSchema(Session.SESSION_UPDATE_TIME, FieldType.LONG));
            TablestoreHelper.createSearchIndexIfNotExist(client, sessionTableName, sessionSearchIndexName, sessionSchemas, Collections.emptyList());
        }

        if (messageSearchIndexSchema == null || messageSearchIndexSchema.isEmpty()) {
            log.warn("skip create message search index because messageSearchIndexSchema is empty");
        } else {
            List<FieldSchema> messageSchemas = new ArrayList<>(messageSearchIndexSchema);
            TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(Message.MESSAGE_SESSION_ID, FieldType.KEYWORD));
            TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(Message.MESSAGE_MESSAGE_ID, FieldType.KEYWORD));
            TablestoreHelper.addSchemaIfNotExist(messageSchemas, new FieldSchema(Message.MESSAGE_CREATE_TIME, FieldType.LONG));
            TablestoreHelper.addSchemaIfNotExist(
                messageSchemas,
                new FieldSchema(Message.MESSAGE_CONTENT, FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord)
            );
            TablestoreHelper.createSearchIndexIfNotExist(client, messageTableName, messageSearchIndexName, messageSchemas, Collections.emptyList());
        }
    }

    @Override
    public void deleteTableAndIndex() {
        TablestoreHelper.deleteTable(client, sessionTableName);
        TablestoreHelper.deleteTable(client, messageTableName);
    }

    private Long getMessageCreateTimeFromSecondaryIndex(String sessionId, String messageId) {
        PrimaryKey start = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(messageId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.INF_MIN)
            .build();

        PrimaryKey end = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Message.MESSAGE_SESSION_ID, PrimaryKeyValue.fromString(sessionId))
            .addPrimaryKeyColumn(Message.MESSAGE_MESSAGE_ID, PrimaryKeyValue.fromString(messageId))
            .addPrimaryKeyColumn(Message.MESSAGE_CREATE_TIME, PrimaryKeyValue.INF_MAX)
            .build();
        TablestoreHelper.GetRangeIterator<Message> iterator = new TablestoreHelper.GetRangeIterator<>(
            client,
            messageSecondaryIndexName,
            TablestoreHelper::rowToMessage,
            start,
            end,
            null,
            Order.ASC,
            null,
            null,
            null
        );
        List<Message> messages = CollectionUtil.toList(iterator);
        if (messages.size() == 1) {
            return messages.get(0).getCreateTime();
        } else if (messages.size() > 1) {
            throw Exceptions.illegalArgument("message is not unique, sessionId:%s, messageId:%s, details messages:[%s]", sessionId, messageId, messages);
        } else {
            return null;
        }
    }
}
