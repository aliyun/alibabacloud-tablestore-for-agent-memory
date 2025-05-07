package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.knowledge.KnowledgeStoreImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConstFieldsTest {

    @Test
    void cannotModify() {

        String msg = "修改后不同版本兼容性会有问题";

        // session
        Assertions.assertEquals("user_id", Session.SESSION_USER_ID, msg);
        Assertions.assertEquals("session_id", Session.SESSION_SESSION_ID, msg);
        Assertions.assertEquals("update_time", Session.SESSION_UPDATE_TIME, msg);

        // message
        Assertions.assertEquals("message_id", Message.MESSAGE_MESSAGE_ID, msg);
        Assertions.assertEquals("session_id", Message.MESSAGE_SESSION_ID, msg);
        Assertions.assertEquals("create_time", Message.MESSAGE_CREATE_TIME, msg);
        Assertions.assertEquals("content", Message.MESSAGE_CONTENT, msg);

        // document
        Assertions.assertEquals("document_id", Document.DOCUMENT_DOCUMENT_ID, msg);
        Assertions.assertEquals("tenant_id", Document.DOCUMENT_TENANT_ID, msg);
        Assertions.assertEquals("__default", Document.DOCUMENT_DEFAULT_TENANT_ID, msg);

        // knowledge_store_impl
        Assertions.assertEquals("_flag_routing_values", KnowledgeStoreImpl.FLAG_ROUTING_VALUES, msg);
        Assertions.assertEquals("_flag_skip_wrap_tenant_ids", KnowledgeStoreImpl.FLAG_SKIP_WRAP_TENANT_IDS, msg);
    }
}
