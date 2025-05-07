package com.aliyun.openservices.tablestore.agent.memory;

import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import java.util.Iterator;

public interface MemoryStore {

    // <-------- Session related -------->

    /**
     * Insert (overwrite) a session
     *
     * @param session session object
     */
    void putSession(Session session);

    /**
     * Update a session.
     *
     * @param session session object
     */
    void updateSession(Session session);

    /**
     * Delete a session
     *
     * @param userId    user ID
     * @param sessionId session ID
     */
    void deleteSession(String userId, String sessionId);

    /**
     * Delete all sessions for a user
     *
     * @param userId user ID
     */
    void deleteSessions(String userId);

    /**
     * Delete all sessions and messages for a user
     *
     * @param userId    user ID
     * @param sessionId session ID
     */
    void deleteSessionAndMessages(String userId, String sessionId);

    /**
     * Delete all sessions for all users
     */
    void deleteAllSessions();

    /**
     * Get a session
     *
     * @param userId    user ID
     * @param sessionId session ID
     * @return session object
     */
    Session getSession(String userId, String sessionId);

    /**
     * List all sessions
     *
     * @return iterator over session objects
     */
    Iterator<Session> listAllSessions();

    /**
     * List sessions for a specific user.
     *
     * @param userId         user ID
     * @param metadataFilter metadata filter condition. null means no restriction.
     * @param maxCount       maximum number of items returned by the iterator. null or -1 returns all.
     * @param batchSize      internal batch size parameter. null or -1 starts adaptive batch size.
     * @return iterator over session objects
     */
    Iterator<Session> listSessions(String userId, Filter metadataFilter, Long maxCount, Integer batchSize);

    /**
     * List recent session information, sorted by session update time.
     *
     * @param userId                   user ID
     * @param metadataFilter           metadata filter condition. null means no restriction.
     * @param inclusiveStartUpdateTime inclusive start update time. null means no restriction.
     * @param inclusiveEndUpdateTime   inclusive end update time. null means no restriction.
     * @param maxCount                 maximum number of items returned by the iterator. null or -1 returns all.
     * @param batchSize                internal batch size parameter. null or -1 starts adaptive batch size.
     * @return iterator over session objects
     */
    Iterator<Session> listRecentSessions(
        String userId,
        Filter metadataFilter,
        Long inclusiveStartUpdateTime,
        Long inclusiveEndUpdateTime,
        Long maxCount,
        Integer batchSize
    );

    /**
     * Paginate through recent session information using continuous pagination, sorted by session update time.
     *
     * @param userId                   user ID
     * @param pageSize                 page size, range [1, 5000]
     * @param metadataFilter           metadata filter condition. null means no restriction.
     * @param inclusiveStartUpdateTime inclusive start update time. null means no restriction.
     * @param inclusiveEndUpdateTime   inclusive end update time. null means no restriction.
     * @param nextToken                pagination token. Pass the token from the previous query result to continue pagination.
     * @param batchSize                internal batch size parameter. null or -1 starts adaptive batch size.
     * @return paginated session response
     */
    Response<Session> listRecentSessionsPaginated(
        String userId,
        int pageSize,
        Filter metadataFilter,
        Long inclusiveStartUpdateTime,
        Long inclusiveEndUpdateTime,
        String nextToken,
        Integer batchSize
    );

    /**
     * Search sessions using search index. (Requires creation of search index)
     *
     * @param searchRequest search conditions {@link MemorySearchRequest}
     * @return session response
     */
    Response<Session> searchSessions(MemorySearchRequest searchRequest);

    // <-------- Message related-------->

    /**
     * Insert (overwrite) a message
     *
     * @param message message object
     */
    void putMessage(Message message);

    /**
     * Update a message.
     *
     * @param message message object
     */
    void updateMessage(Message message);

    /**
     * Delete a message
     *
     * @param message message object
     */
    void deleteMessage(Message message);

    /**
     * Delete a message
     *
     * @param sessionId  session ID
     * @param messageId  message ID
     * @param createTime create time. (Optional parameter, setting it can improve deletion performance)
     */
    void deleteMessage(String sessionId, String messageId, Long createTime);

    /**
     * Delete all messages for a session
     *
     * @param sessionId session ID
     */
    void deleteMessages(String sessionId);

    /**
     * Delete all messages for all sessions
     */
    void deleteAllMessages();

    /**
     * Get a message
     *
     * @param sessionId  session ID
     * @param messageId  message ID
     * @param createTime create time. (Optional parameter, setting it can improve performance)
     * @return message object
     */
    Message getMessage(String sessionId, String messageId, Long createTime);

    /**
     * List all messages
     *
     * @return iterator over message objects
     */
    Iterator<Message> listAllMessages();

    /**
     * List all messages for a session
     *
     * @param sessionId session ID
     * @return iterator over message objects
     */
    Iterator<Message> listMessages(String sessionId);

    /**
     * List all messages for a session
     *
     * @param sessionId                session ID
     * @param metadataFilter           metadata filter condition. null means no restriction.
     * @param inclusiveStartCreateTime inclusive start create time. null means no restriction.
     * @param inclusiveEndCreateTime   inclusive end create time. null means no restriction.
     * @param order                    sort order. null defaults to descending order.
     * @param maxCount                 maximum number of items returned by the iterator. null or -1 returns all.
     * @param batchSize                internal batch size parameter. null or -1 starts adaptive batch size.
     * @return iterator over message objects
     */
    Iterator<Message> listMessages(
        String sessionId,
        Filter metadataFilter,
        Long inclusiveStartCreateTime,
        Long inclusiveEndCreateTime,
        Order order,
        Long maxCount,
        Integer batchSize
    );

    /**
     * Paginate through messages using continuous pagination.
     *
     * @param sessionId                session ID
     * @param pageSize                 page size, range [1, 5000]
     * @param metadataFilter           metadata filter condition. null means no restriction.
     * @param inclusiveStartCreateTime inclusive start create time. null means no restriction.
     * @param inclusiveEndCreateTime   inclusive end create time. null means no restriction.
     * @param order                    sort order. null defaults to descending order.
     * @param nextToken                pagination token. Pass the token from the previous query result to continue pagination.
     * @param batchSize                internal batch size parameter. null or -1 starts adaptive batch size.
     * @return paginated session response
     */
    Response<Message> listMessagesPaginated(
        String sessionId,
        int pageSize,
        Filter metadataFilter,
        Long inclusiveStartCreateTime,
        Long inclusiveEndCreateTime,
        Order order,
        String nextToken,
        Integer batchSize
    );

    /**
     * Search messages using search index. (Requires creation of search index)
     *
     * @param searchRequest search conditions {@link MemorySearchRequest}
     * @return message response
     */
    Response<Message> searchMessages(MemorySearchRequest searchRequest);

    /**
     * Initialize table
     */
    void initTable();

    /**
     * Initialize search index
     */
    void initSearchIndex();

    /**
     * Delete table and index
     */
    void deleteTableAndIndex();
}
