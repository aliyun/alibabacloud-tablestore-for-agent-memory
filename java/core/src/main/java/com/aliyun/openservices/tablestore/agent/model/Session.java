package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.util.TimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@ToString
@EqualsAndHashCode
@Data
@Accessors(chain = true)
@SuperBuilder(toBuilder = true)
public class Session {

    /**
     * User ID
     */
    private final String userId;

    /**
     * Session ID
     */
    private final String sessionId;

    /**
     * Update time. This field needs to be updated each time a Message message is written.
     */
    private Long updateTime;

    /**
     * Session metadata
     */
    private Metadata metadata;

    public Session(Session session) {
        this(session.getUserId(), session.getSessionId(), session.getUpdateTime(), session.getMetadata());
    }

    public Session(String userId, String sessionId) {
        this(userId, sessionId, TimeUtils.currentTimeMicroseconds());
    }

    public Session(String userId, String sessionId, Long updateTime) {
        this(userId, sessionId, updateTime, new Metadata());
    }

    public Session(String userId, String sessionId, Long updateTime, Metadata metadata) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.updateTime = updateTime;
        this.metadata = metadata;
    }

    public void refreshUpdateTime() {
        this.updateTime = TimeUtils.currentTimeMicroseconds();
    }

    /**
     * User ID Field Name
     */
    public static final String SESSION_USER_ID = "user_id";
    /**
     * Session ID Field Name
     */
    public static final String SESSION_SESSION_ID = "session_id";
    /**
     * Update time Field Name
     */
    public static final String SESSION_UPDATE_TIME = "update_time";
}
