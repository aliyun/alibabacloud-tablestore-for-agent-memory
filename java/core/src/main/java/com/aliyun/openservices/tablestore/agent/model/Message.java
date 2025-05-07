package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.util.TimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@ToString
@Data
@Accessors(chain = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
public class Message {

    /**
     * Session ID
     */
    private final String sessionId;

    /**
     * Message ID
     */
    private final String messageId;

    /**
     * Creation time. This time cannot be modified after the message is created.
     */
    private Long createTime;

    /**
     * Message content
     */
    private String content;

    /**
     * Session metadata
     */
    private Metadata metadata;

    public Message(Message message) {
        this(message.getSessionId(), message.getMessageId(), message.getCreateTime());
        this.content = message.getContent();
        this.metadata = message.getMetadata();
    }

    public Message(String sessionId, String messageId) {
        this(sessionId, messageId, TimeUtils.currentTimeMicroseconds());
    }

    public Message(String sessionId, String messageId, Long createTime) {
        this(sessionId, messageId, createTime, null, new Metadata());
    }

    public Message(String sessionId, String messageId, Long createTime, String content, Metadata metadata) {
        this.sessionId = sessionId;
        this.messageId = messageId;
        this.createTime = createTime;
        this.content = content;
        this.metadata = metadata;
    }

    /**
     * Session ID Field Name
     */
    public static final String MESSAGE_SESSION_ID = "session_id";
    /**
     * Message ID Field Name
     */
    public static final String MESSAGE_MESSAGE_ID = "message_id";
    /**
     * Creation time Field Name
     */
    public static final String MESSAGE_CREATE_TIME = "create_time";
    /**
     * Creation time Field Name
     */
    public static final String MESSAGE_CONTENT = "content";
}
