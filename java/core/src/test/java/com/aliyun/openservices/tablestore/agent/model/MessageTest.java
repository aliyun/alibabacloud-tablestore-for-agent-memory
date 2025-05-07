package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MessageTest extends BaseTest {

    @Test
    void testSimple() {
        Metadata metadata = new Metadata().put("key", "value").put("key2", 1);
        Message messageBuilder = Message.builder().sessionId("123").messageId("456").createTime(123L).content("hello world").metadata(metadata).build();

        Message messageSimple3 = new Message("123", "456", 123L);
        messageSimple3.setContent("hello world");
        messageSimple3.setMetadata(metadata);

        Message messageSimple2 = new Message("123", "456");
        messageSimple2.setCreateTime(123L);
        messageSimple2.setContent("hello world");
        messageSimple2.setMetadata(metadata);

        Message messageCopy = new Message(messageBuilder);

        assertEquals(messageBuilder, messageSimple3);
        assertEquals(messageBuilder, messageSimple2);
        assertEquals(messageBuilder, messageCopy);

        Message toBuild = messageBuilder.toBuilder().messageId("123").build();
        Assertions.assertEquals("123", toBuild.getMessageId());
    }
}
