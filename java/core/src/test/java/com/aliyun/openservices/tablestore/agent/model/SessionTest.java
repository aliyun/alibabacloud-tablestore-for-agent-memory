package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionTest extends BaseTest {

    @Test
    void simple() {
        Metadata metadata = new Metadata().put("key", "value").put("key2", 1);
        Session sessionBuilder = Session.builder().userId("123").sessionId("456").updateTime(123L).metadata(metadata).build();

        Session sessionCopy = new Session(sessionBuilder);

        Session sessionSimple1 = new Session("123", "456", 123L);
        sessionSimple1.setMetadata(metadata);

        Session sessionSimple2 = new Session("123", "456");
        sessionSimple2.setUpdateTime(123L);
        sessionSimple2.setMetadata(metadata);

        Session sessionSimple3 = new Session("123", "456", 123L, metadata);

        assertEquals(sessionBuilder, sessionCopy);
        assertEquals(sessionBuilder, sessionSimple1);
        assertEquals(sessionBuilder, sessionSimple2);
        assertEquals(sessionBuilder, sessionSimple3);

        sessionSimple3.refreshUpdateTime();
        Assertions.assertTrue(Math.abs(sessionSimple3.getUpdateTime() - System.currentTimeMillis() * 1000) < 100_000);

        Session toBuilder = sessionSimple3.toBuilder().userId("abc").build();
        Assertions.assertEquals("abc", toBuilder.getUserId());

    }
}
