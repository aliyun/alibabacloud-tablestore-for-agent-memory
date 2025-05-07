package com.aliyun.openservices.tablestore.agent;

import com.alicloud.openservices.tablestore.SyncClient;
import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import net.datafaker.Faker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

@Slf4j
public abstract class BaseTest {

    protected static volatile SyncClient client;
    protected static Faker faker = new Faker(Locale.CHINA);

    @BeforeAll
    public static void staticSetUp() {
        if (client == null) {
            synchronized (BaseTest.class) {
                if (client == null) {
                    String endPoint = System.getenv("tablestore_end_point");
                    String instanceName = System.getenv("tablestore_instance_name");
                    String accessKeyId = System.getenv("tablestore_access_key_id");
                    String accessKeySecret = System.getenv("tablestore_access_key_secret");
                    if (endPoint == null || instanceName == null || accessKeyId == null || accessKeySecret == null) {
                        Assumptions.abort(
                            "env tablestore_end_point, tablestore_instance_name, tablestore_access_key_id, tablestore_access_key_secret is not set"
                        );
                    }
                    client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);
                }
            }
        }
    }

    public static <T> T randomFrom(T[] array) {
        return array[ThreadLocalRandom.current().nextInt(array.length)];
    }

    public static <T> T randomFrom(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    public static <T> List<T> randomList(List<T> list, int maxSize) {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            result.add(list.get(ThreadLocalRandom.current().nextInt(list.size())));
        }
        return result;
    }

    public static void assertEquals(Session expected, Session actual) {
        Assertions.assertEquals(expected.getUserId(), actual.getUserId());
        Assertions.assertEquals(expected.getSessionId(), actual.getSessionId());
        Assertions.assertEquals(expected.getUpdateTime(), actual.getUpdateTime());
        Assertions.assertEquals(expected.getMetadata().size(), actual.getMetadata().size());

        for (Map.Entry<String, Object> entry : expected.getMetadata().toMap().entrySet()) {
            Object actualMeta = actual.getMetadata().get(entry.getKey());
            Assertions.assertNotNull(actualMeta);
            if (actualMeta instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) entry.getValue(), (byte[]) actualMeta);
            } else {
                Assertions.assertEquals(entry.getValue(), actualMeta, entry.getKey());
            }
        }
    }

    public static void assertEquals(Document expected, Document actual) {
        Assertions.assertEquals(expected.getDocumentId(), actual.getDocumentId());
        Assertions.assertEquals(expected.getTenantId(), actual.getTenantId());
        Assertions.assertEquals(expected.getText(), actual.getText());
        Assertions.assertArrayEquals(expected.getEmbedding(), actual.getEmbedding());
        Assertions.assertEquals(expected.getMetadata().size(), actual.getMetadata().size());

        for (Map.Entry<String, Object> entry : expected.getMetadata().toMap().entrySet()) {
            Object actualMeta = actual.getMetadata().get(entry.getKey());
            Assertions.assertNotNull(actualMeta);
            if (actualMeta instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) entry.getValue(), (byte[]) actualMeta);
            } else {
                Assertions.assertEquals(entry.getValue(), actualMeta, entry.getKey());
            }
        }
    }

    public static void assertEquals(Message expected, Message actual) {
        Assertions.assertEquals(expected.getSessionId(), actual.getSessionId());
        Assertions.assertEquals(expected.getMessageId(), actual.getMessageId());
        Assertions.assertEquals(expected.getCreateTime(), actual.getCreateTime());
        Assertions.assertEquals(expected.getContent(), actual.getContent());
        Assertions.assertEquals(expected.getMetadata().size(), actual.getMetadata().size());
        for (Map.Entry<String, Object> entry : expected.getMetadata().toMap().entrySet()) {
            Object actualMeta = actual.getMetadata().get(entry.getKey());
            Assertions.assertNotNull(actualMeta);
            if (actualMeta instanceof byte[]) {
                Assertions.assertArrayEquals((byte[]) entry.getValue(), (byte[]) actualMeta);
            } else {
                Assertions.assertEquals(entry.getValue(), actualMeta, entry.getKey());
            }
        }
    }

}
