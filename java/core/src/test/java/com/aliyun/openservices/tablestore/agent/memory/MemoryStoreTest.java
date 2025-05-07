package com.aliyun.openservices.tablestore.agent.memory;

import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.aliyun.openservices.tablestore.agent.BaseTest;
import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.MetaType;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.model.sort.ScoreSort;
import com.aliyun.openservices.tablestore.agent.util.CollectionUtil;
import com.aliyun.openservices.tablestore.agent.util.Pair;
import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
@SuppressWarnings({ "DataFlowIssue", "ConditionalBreakInInfiniteLoop" })
class MemoryStoreTest extends BaseTest {

    MemoryStoreImpl store;

    @BeforeEach
    void setUp() {

        List<Pair<String, MetaType>> sessionSecondaryIndexMeta = Arrays.asList(
            Pair.of("meta_example_string", MetaType.STRING),
            Pair.of("meta_example_text", MetaType.STRING),
            Pair.of("meta_example_long", MetaType.INTEGER),
            Pair.of("meta_example_double", MetaType.DOUBLE),
            Pair.of("meta_example_boolean", MetaType.BOOLEAN),
            Pair.of("meta_example_bytes", MetaType.BINARY)
        );

        List<FieldSchema> sessionSearchIndexSchema = Arrays.asList(
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );

        List<FieldSchema> messageSearchIndexSchema = Arrays.asList(
            new FieldSchema("meta_example_string", FieldType.KEYWORD),
            new FieldSchema("meta_example_text", FieldType.TEXT).setAnalyzer(FieldSchema.Analyzer.MaxWord),
            new FieldSchema("meta_example_long", FieldType.LONG),
            new FieldSchema("meta_example_double", FieldType.DOUBLE),
            new FieldSchema("meta_example_boolean", FieldType.BOOLEAN)
        );

        store = MemoryStoreImpl.builder()
            .client(client)
            .sessionSecondaryIndexMeta(sessionSecondaryIndexMeta)
            .sessionSearchIndexSchema(sessionSearchIndexSchema)
            .messageSearchIndexSchema(messageSearchIndexSchema)
            .build();
    }

    Session randomSession(String userId) {
        Session session = new Session(userId, UUID.randomUUID().toString());
        session.setUpdateTime(faker.number().numberBetween(0, 100L));
        String name = faker.name().fullName();
        session.getMetadata().put("meta_example_string", name);
        session.getMetadata().put("meta_example_text", randomFrom(Arrays.asList("abc", "def", "ghi", "abcd", "abcdef", "abcgh")));
        session.getMetadata().put("meta_example_long", faker.number().numberBetween(0, Long.MAX_VALUE));
        session.getMetadata().put("meta_example_double", faker.number().randomDouble(2, 0, 1));
        session.getMetadata().put("meta_example_boolean", faker.bool().bool());
        session.getMetadata().put("meta_example_bytes", name.getBytes(StandardCharsets.UTF_8));
        return session;
    }

    Message randomMessage(String sessionId) {
        Message message = new Message(sessionId, UUID.randomUUID().toString());
        message.setCreateTime(faker.number().numberBetween(0, 100L));
        String name = faker.name().fullName();
        String content = String.join(" ", randomList(Arrays.asList("abc", "def", "ghi", "abcd", "adef", "abcgh", "apple", "banana", "cherry"), 5));
        message.setContent(content);
        message.getMetadata().put("meta_example_string", name);
        message.getMetadata().put("meta_example_text", randomFrom(Arrays.asList("abc", "def", "ghi", "abcd", "abcdef", "abcgh")));
        message.getMetadata().put("meta_example_long", faker.number().numberBetween(0, Long.MAX_VALUE));
        message.getMetadata().put("meta_example_double", faker.number().randomDouble(2, 0, 1));
        message.getMetadata().put("meta_example_boolean", faker.bool().bool());
        message.getMetadata().put("meta_example_bytes", name.getBytes(StandardCharsets.UTF_8));
        return message;
    }

    @Test
    void basicSessionStore() {
        store.deleteTableAndIndex();
        store.initTable();
        store.deleteAllSessions();

        Session session = randomSession("1");
        String sessionId = session.getSessionId();
        store.putSession(session);

        Session sessionPut = store.getSession("1", sessionId);
        assertEquals(sessionPut, session);
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_string"));
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_text"));
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_long"));
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_double"));
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_boolean"));
        Assertions.assertTrue(sessionPut.getMetadata().containsKey("meta_example_bytes"));

        Session sessionToUpdate = new Session(session);
        Assertions.assertEquals(sessionToUpdate, session);
        sessionToUpdate.getMetadata().put("meta_example_string", "updated");
        store.updateSession(sessionToUpdate);
        Session sessionUpdated = store.getSession("1", sessionId);
        assertEquals(sessionUpdated, sessionToUpdate);

        store.deleteSession("1", sessionId);
        Session sessionDeleted = store.getSession("1", sessionId);
        Assertions.assertNull(sessionDeleted);

        int total = ThreadLocalRandom.current().nextInt(30, 80);
        int user1Count = 0;
        int user2Count = 0;
        for (int i = 0; i < total; i++) {
            Session sessionForTestDelete = randomSession(randomFrom(Arrays.asList("1", "2")));
            if (sessionForTestDelete.getUserId().equals("1")) {
                user1Count++;
            } else if (sessionForTestDelete.getUserId().equals("2")) {
                user2Count++;
            }
            store.putSession(sessionForTestDelete);
        }
        log.info("total:{}, user1Count:{}, user2Count:{}", total, user1Count, user2Count);
        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllSessions()).size());

        store.deleteSessions("1");
        Assertions.assertEquals(total - user1Count, CollectionUtil.toList(store.listAllSessions()).size());
        store.deleteSessions("2");
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllSessions()).size());

        for (int i = 0; i < total; i++) {
            Session sessionForTestDeleteAll = randomSession(randomFrom(Arrays.asList("1", "2")));
            store.putSession(sessionForTestDeleteAll);
        }

        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllSessions()).size());
        store.deleteAllSessions();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllSessions()).size());

        // 空内容测试
        Session emptySession = randomSession("1");
        emptySession.setUpdateTime(123L);
        emptySession.setMetadata(null);
        store.putSession(emptySession);
        store.updateSession(emptySession);
        Session emptySessionRead = store.getSession("1", emptySession.getSessionId());
        Assertions.assertEquals("1", emptySessionRead.getUserId());
        Assertions.assertEquals(emptySession.getSessionId(), emptySessionRead.getSessionId());
        Assertions.assertEquals(123L, emptySessionRead.getUpdateTime());
        Assertions.assertTrue(emptySessionRead.getMetadata() == null || emptySessionRead.getMetadata().size() == 0);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void basicBatchSessionStore() {
        store.deleteTableAndIndex();
        store.initTable();
        store.initSearchIndex();
        store.deleteAllSessions();
        int total = 100;
        int user1Count = 0;
        int user1MetaBoolTrue = 0;
        int user1MetaDoubleGtHalf1 = 0;
        int user1MetaBoolTrueAndMetaDoubleGtHalf1 = 0;
        for (int i = 0; i < total; i++) {
            Session session = randomSession(randomFrom(Arrays.asList("1", "2")));
            if (session.getUserId().equals("1")) {
                user1Count++;
                if (Boolean.TRUE.equals(session.getMetadata().getBoolean("meta_example_boolean"))) {
                    user1MetaBoolTrue++;
                }
                if (session.getMetadata().getDouble("meta_example_double") > 0.5) {
                    user1MetaDoubleGtHalf1++;
                }
                if (Boolean.TRUE.equals(
                    session.getMetadata().getBoolean("meta_example_boolean") && session.getMetadata().getDouble("meta_example_double") > 0.5
                )) {
                    user1MetaBoolTrueAndMetaDoubleGtHalf1++;
                }
            }
            store.putSession(session);
        }
        log.info(
            "total:{}, user1Count:{}, user1MetaBoolTrue:{}, user1MetaDoubleGtHalf1:{}, user1MetaBoolTrueAndMetaDoubleGtHalf1:{}",
            total,
            user1Count,
            user1MetaBoolTrue,
            user1MetaDoubleGtHalf1,
            user1MetaBoolTrueAndMetaDoubleGtHalf1
        );

        List<Session> sessions = CollectionUtil.toList(store.listAllSessions());
        Assertions.assertEquals(total, sessions.size());

        {
            Assertions.assertEquals(user1Count, CollectionUtil.toList(store.listSessions("1", null, 100L, 2)).size());
            Assertions.assertEquals(
                user1MetaBoolTrue,
                CollectionUtil.toList(store.listSessions("1", Filters.eq("meta_example_boolean", true), 100L, 2)).size()
            );
            Assertions.assertEquals(
                user1MetaDoubleGtHalf1,
                CollectionUtil.toList(store.listSessions("1", Filters.gt("meta_example_double", 0.5), 100L, -1)).size()
            );
            Assertions.assertEquals(
                user1MetaBoolTrueAndMetaDoubleGtHalf1,
                CollectionUtil.toList(
                    store.listSessions("1", Filters.and(Filters.eq("meta_example_boolean", true), Filters.gt("meta_example_double", 0.5)), 100L, null)
                ).size()
            );
        }

        {

            Assertions.assertEquals(user1Count, CollectionUtil.toList(store.listRecentSessions("1", null, null, null, 100L, 1)).size());
            List<Session> list = CollectionUtil.toList(store.listRecentSessions("1", Filters.eq("meta_example_boolean", true), null, null, 100L, 2));
            if (!list.isEmpty()) {
                Session session = list.get(0);
                Assertions.assertTrue(session.getMetadata().getBoolean("meta_example_boolean"));
                Session readSession = store.getSession(session.getUserId(), session.getSessionId());
                assertEquals(readSession, session);
            }
            Assertions.assertEquals(user1MetaBoolTrue, list.size());
            Assertions.assertEquals(
                user1MetaDoubleGtHalf1,
                CollectionUtil.toList(store.listRecentSessions("1", Filters.gt("meta_example_double", 0.5), null, null, 100L, -1)).size()
            );
            Assertions.assertEquals(
                user1MetaBoolTrueAndMetaDoubleGtHalf1,
                CollectionUtil.toList(
                    store.listRecentSessions(
                        "1",
                        Filters.and(Filters.eq("meta_example_boolean", true), Filters.gt("meta_example_double", 0.5)),
                        null,
                        null,
                        100L,
                        100
                    )
                ).size()
            );
        }

        {
            {
                Response<Session> sessionResponse = store.listRecentSessionsPaginated("1", 100, null, null, null, null, null);
                Assertions.assertEquals(user1Count, sessionResponse.getHits().size());
            }
            {
                int randomPageSize = ThreadLocalRandom.current().nextInt(1, 101);
                Response<Session> sessionResponse = store.listRecentSessionsPaginated("1", randomPageSize, null, null, null, null, null);
                Assertions.assertEquals(Math.min(randomPageSize, user1Count), sessionResponse.getHits().size());
            }
            {
                int pageSize = 3;
                List<Session> sessionResult = new ArrayList<>();
                Set<String> sessionIds = new HashSet<>();
                String nextToken = null;
                int batchCount = 0;
                while (true) {
                    Response<Session> sessionResponse = store.listRecentSessionsPaginated("1", pageSize, null, null, null, nextToken, null);
                    batchCount++;
                    nextToken = sessionResponse.getNextToken();
                    sessionIds.addAll(sessionResponse.getHits().stream().map(Session::getSessionId).collect(Collectors.toList()));
                    sessionResult.addAll(sessionResponse.getHits());
                    if (nextToken == null) {
                        break;
                    }
                }
                log.info("listRecentSessionsPaginated batchCount:{}", batchCount);
                Assertions.assertEquals(user1Count, sessionResult.size());
                Assertions.assertEquals(user1Count, sessionIds.size());
            }
        }
        {
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listRecentSessionsPaginated("1", 1000, null, 1L, 2L, null, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listRecentSessions("1", null, 1L, 2L, null, null));
            int testTimes = 100;
            for (int i = 0; i < testTimes; i++) {
                long big = ThreadLocalRandom.current().nextLong(80, 100);
                long small = ThreadLocalRandom.current().nextLong(10, 40);
                Response<Session> sessionResponse = store.listRecentSessionsPaginated("1", 1000, null, big, small, null, null);
                Assertions.assertNull(sessionResponse.getNextToken());
                Assertions.assertTrue(
                    sessionResponse.getHits().stream().allMatch(session -> session.getUpdateTime() <= big && session.getUpdateTime() >= small)
                );
            }
        }

        TablestoreHelper.waitSearchIndexReady(client, store.getSessionTableName(), store.getSessionSearchIndexName(), total);

        {
            Response<Session> response = store.searchSessions(
                MemorySearchRequest.builder().metadataFilter(Filters.eq("meta_example_boolean", true)).sort(new ScoreSort()).limit(50).build()
            );
            Assertions.assertTrue(response.getHits().stream().allMatch(m -> m.getMetadata().getBoolean("meta_example_boolean")));
        }
        {
            String nextToken = null;
            List<Session> sessionArrayList = new ArrayList<>();
            while (true) {
                Response<Session> sessionResponse = store.searchSessions(MemorySearchRequest.builder().limit(17).nextToken(nextToken).build());
                sessionArrayList.addAll(sessionResponse.getHits());
                nextToken = sessionResponse.getNextToken();
                if (nextToken == null) {
                    break;
                }
            }
            Assertions.assertEquals(total, sessionArrayList.size());
        }

    }

    @Test
    void simpleMessageStore() {
        store.deleteTableAndIndex();
        store.initTable();
        store.deleteAllMessages();
        long createTime = 123L;
        Message message = randomMessage("1");
        message.setCreateTime(createTime);
        store.putMessage(message);

        Message messagePut = store.getMessage("1", message.getMessageId(), createTime);
        assertEquals(messagePut, message);
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_string"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_text"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_long"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_double"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_boolean"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_bytes"));

        messagePut = store.getMessage("1", message.getMessageId(), null);
        assertEquals(messagePut, message);
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_string"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_text"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_long"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_double"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_boolean"));
        Assertions.assertTrue(messagePut.getMetadata().containsKey("meta_example_bytes"));

        Message messageToUpdate = new Message(message);
        Assertions.assertEquals(messageToUpdate, message);
        messageToUpdate.getMetadata().put("meta_example_string", "updated");
        store.updateMessage(messageToUpdate);
        Message messageUpdated = store.getMessage("1", message.getMessageId(), createTime);
        assertEquals(messageUpdated, messageToUpdate);
        messageUpdated = store.getMessage("1", message.getMessageId(), null);
        assertEquals(messageUpdated, messageToUpdate);

        store.deleteMessage("1", message.getMessageId(), null);
        Message messageDeleted = store.getMessage("1", message.getMessageId(), null);
        Assertions.assertNull(messageDeleted);
        store.deleteMessage("1", message.getMessageId(), null);
        store.deleteMessage("1", message.getMessageId(), createTime);
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllMessages()).size());

        int total = ThreadLocalRandom.current().nextInt(30, 80);
        int session1Count = 0;
        int session2Count = 0;
        for (int i = 0; i < total; i++) {
            Message messageForTestDelete = randomMessage(randomFrom(Arrays.asList("1", "2")));
            if (messageForTestDelete.getSessionId().equals("1")) {
                session1Count++;
            } else if (messageForTestDelete.getSessionId().equals("2")) {
                session2Count++;
            }
            store.putMessage(messageForTestDelete);
        }
        log.info("total:{}, session1Count:{}, session2Count:{}", total, session1Count, session2Count);
        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllMessages()).size());

        store.deleteMessages("1");
        Assertions.assertEquals(total - session1Count, CollectionUtil.toList(store.listAllMessages()).size());
        store.deleteMessages("2");
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllMessages()).size());

        for (int i = 0; i < total; i++) {
            Message messageForTestDeleteAll = randomMessage(randomFrom(Arrays.asList("1", "2")));
            store.putMessage(messageForTestDeleteAll);
        }

        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllMessages()).size());
        store.deleteAllMessages();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllMessages()).size());

        // 空内容测试
        Message emptyMessage = randomMessage("1");
        emptyMessage.setContent(null);
        emptyMessage.setMetadata(null);
        store.putMessage(emptyMessage);
        Message emptyMessageRead = store.getMessage("1", emptyMessage.getMessageId(), null);
        Assertions.assertEquals("1", emptyMessageRead.getSessionId());
        Assertions.assertEquals(emptyMessage.getMessageId(), emptyMessageRead.getMessageId());
        Assertions.assertEquals(emptyMessage.getCreateTime(), emptyMessageRead.getCreateTime());
        Assertions.assertNull(emptyMessageRead.getContent());
        Assertions.assertTrue(emptyMessageRead.getMetadata() == null || emptyMessageRead.getMetadata().size() == 0);
    }

    @Test
    void batchMessageStore() {
        store.deleteTableAndIndex();
        store.initTable();
        store.initSearchIndex();
        store.deleteAllMessages();
        Assertions.assertEquals(0, CollectionUtil.toList(store.listAllMessages()).size());

        int total = 100;
        int session1Count = 0;
        int session1MetaBoolTrue = 0;
        int session1MetaDoubleGtHalf1 = 0;
        int session1MetaBoolTrueAndMetaDoubleGtHalf1 = 0;
        for (int i = 0; i < total; i++) {
            Message message = randomMessage(randomFrom(Arrays.asList("1", "2")));
            if (message.getSessionId().equals("1")) {
                session1Count++;
                if (Boolean.TRUE.equals(message.getMetadata().getBoolean("meta_example_boolean"))) {
                    session1MetaBoolTrue++;
                }
                if (message.getMetadata().getDouble("meta_example_double") > 0.5) {
                    session1MetaDoubleGtHalf1++;
                }
                if (Boolean.TRUE.equals(
                    message.getMetadata().getBoolean("meta_example_boolean") && message.getMetadata().getDouble("meta_example_double") > 0.5
                )) {
                    session1MetaBoolTrueAndMetaDoubleGtHalf1++;
                }
            }
            store.putMessage(message);
        }
        log.info(
            "total:{}, session1Count:{}, session1MetaBoolTrue:{}, session1MetaDoubleGtHalf1:{}, session1MetaBoolTrueAndMetaDoubleGtHalf1:{}",
            total,
            session1Count,
            session1MetaBoolTrue,
            session1MetaDoubleGtHalf1,
            session1MetaBoolTrueAndMetaDoubleGtHalf1
        );

        Assertions.assertEquals(total, CollectionUtil.toList(store.listAllMessages()).size());

        {
            Assertions.assertEquals(session1Count, CollectionUtil.toList(store.listMessages("1")).size());
            Assertions.assertEquals(session1Count, CollectionUtil.toList(store.listMessages("1", null, null, null, Order.ASC, -1L, 15)).size());
            Assertions.assertEquals(
                session1MetaBoolTrue,
                CollectionUtil.toList(store.listMessages("1", Filters.eq("meta_example_boolean", true), null, null, null, 100L, 4)).size()
            );
            Assertions.assertEquals(
                session1MetaDoubleGtHalf1,
                CollectionUtil.toList(store.listMessages("1", Filters.gt("meta_example_double", 0.5), null, null, null, 100L, -1)).size()
            );
            Assertions.assertEquals(
                session1MetaBoolTrueAndMetaDoubleGtHalf1,
                CollectionUtil.toList(
                    store.listMessages(
                        "1",
                        Filters.and(Filters.eq("meta_example_boolean", true), Filters.gt("meta_example_double", 0.5)),
                        null,
                        null,
                        null,
                        100L,
                        null
                    )
                ).size()
            );
        }
        {
            log.info(
                "exception:",
                Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessages("1", null, 1L, 2L, Order.DESC, null, null))
            );
            log.info("exception:", Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessages("1", null, 2L, 1L, Order.ASC, null, null)));
            int testTimes = 100;
            for (int i = 0; i < testTimes; i++) {
                long big = ThreadLocalRandom.current().nextLong(80, 100);
                long small = ThreadLocalRandom.current().nextLong(10, 40);
                Iterator<Message> sessionResponse = store.listMessages("1", null, big, small, Order.DESC, null, null);
                Assertions.assertTrue(
                    CollectionUtil.toList(sessionResponse).stream().allMatch(session -> session.getCreateTime() <= big && session.getCreateTime() >= small)
                );
            }
        }

        {
            {
                Response<Message> messageResponse = store.listMessagesPaginated("1", 100, null, null, null, null, null, null);
                Assertions.assertEquals(session1Count, messageResponse.getHits().size());
            }
            {
                int randomPageSize = ThreadLocalRandom.current().nextInt(1, 101);
                Response<Message> messageResponse = store.listMessagesPaginated("1", randomPageSize, null, null, null, null, null, null);
                Assertions.assertEquals(Math.min(randomPageSize, session1Count), messageResponse.getHits().size());
            }
            {
                int pageSize = 3;
                List<Message> messageResult = new ArrayList<>();
                Set<String> messageIds = new HashSet<>();
                String nextToken = null;
                int batchCount = 0;
                while (true) {
                    Response<Message> messageResponse = store.listMessagesPaginated("1", pageSize, null, null, null, null, nextToken, null);
                    batchCount++;
                    nextToken = messageResponse.getNextToken();
                    messageIds.addAll(messageResponse.getHits().stream().map(Message::getMessageId).collect(Collectors.toList()));
                    messageResult.addAll(messageResponse.getHits());
                    if (nextToken == null) {
                        break;
                    }
                }
                log.info("listMessagesPaginated batchCount:{}", batchCount);
                Assertions.assertEquals(session1Count, messageResult.size());
                Assertions.assertEquals(session1Count, messageIds.size());
            }
        }
        {
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessagesPaginated("1", 1000, null, 1L, 2L, null, null, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessagesPaginated("1", 1000, null, 1L, 2L, Order.DESC, null, null));
            int testTimes = 100;
            for (int i = 0; i < testTimes; i++) {
                long big = ThreadLocalRandom.current().nextLong(80, 100);
                long small = ThreadLocalRandom.current().nextLong(10, 40);
                Response<Message> messageResponse = store.listMessagesPaginated("1", 1000, null, big, small, Order.DESC, null, null);
                Assertions.assertNull(messageResponse.getNextToken());
                Assertions.assertTrue(
                    messageResponse.getHits().stream().allMatch(message -> message.getCreateTime() <= big && message.getCreateTime() >= small)
                );
            }
        }
        {
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessagesPaginated("1", 1000, null, 2L, 1L, Order.ASC, null, null));
            Assertions.assertThrows(IllegalArgumentException.class, () -> store.listMessagesPaginated("1", 1000, null, 2L, 1L, null, null, null));
            int testTimes = 100;
            for (int i = 0; i < testTimes; i++) {
                long big = ThreadLocalRandom.current().nextLong(50, 100);
                long small = ThreadLocalRandom.current().nextLong(10, 50);
                Response<Message> messageResponse = store.listMessagesPaginated("1", 1000, null, small, big, Order.ASC, null, null);
                Assertions.assertNull(messageResponse.getNextToken());
                Assertions.assertTrue(
                    messageResponse.getHits().stream().allMatch(message -> message.getCreateTime() <= big && message.getCreateTime() >= small)
                );
            }
        }

        TablestoreHelper.waitSearchIndexReady(client, store.getMessageTableName(), store.getMessageSearchIndexName(), total);

        {
            Response<Message> messageResponse = store.searchMessages(
                MemorySearchRequest.builder().metadataFilter(Filters.eq("meta_example_boolean", true)).sort(new ScoreSort()).limit(50).build()
            );
            Assertions.assertTrue(messageResponse.getHits().stream().allMatch(m -> m.getMetadata().getBoolean("meta_example_boolean")));
        }
        {
            String nextToken = null;
            List<Message> messages = new ArrayList<>();
            while (true) {
                Response<Message> messageResponse = store.searchMessages(MemorySearchRequest.builder().limit(17).nextToken(nextToken).build());
                messages.addAll(messageResponse.getHits());
                nextToken = messageResponse.getNextToken();
                if (nextToken == null) {
                    break;
                }
            }
            Assertions.assertEquals(total, messages.size());
        }
    }
}
