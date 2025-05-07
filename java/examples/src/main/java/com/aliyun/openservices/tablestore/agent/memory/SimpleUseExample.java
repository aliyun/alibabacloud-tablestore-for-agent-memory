package com.aliyun.openservices.tablestore.agent.memory;

import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.Metadata;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.util.TimeUtils;
import java.util.List;

public class SimpleUseExample {

    /**
     * 按照用户和大模型交互的流程，将session和message进行实战应用。
     * 
     * <p>
     * 细节使用可参考：
     * </p>
     * 
     * <li>{@link SessionExample}</li>
     * <li>{@link MessageExample}</li>
     */
    public void chat(MemoryStore store) {
        // 新用户进来使用示例：
        {
            // 1. 创建一个Session进行使用
            Session session = Session.builder()
                .userId("user小明")
                .sessionId("session_a")
                .updateTime(TimeUtils.currentTimeMicroseconds()) // 更新时间，展示最近消息时候会根据该值进行排序
                .metadata(new Metadata().put("meta示例:使用哪个模型", "通义千问2.5").put("meta示例:浏览器", "Chrome"))
                .build();
            store.putSession(session);

            // 2. 用户提问：你好，帮我讲个笑话
            Message message = Message.builder()
                .sessionId("session_a")
                .messageId("message_1")
                .createTime(TimeUtils.currentTimeMicroseconds())
                .content("你好，帮我讲个笑话")
                .metadata(new Metadata().put("meta示例_访问来源", "web").put("meta示例:消息分类", "用户"))
                .build();
            store.putMessage(message);
            // 用户发送消息后，更新session信息。这里仅更新updateTime为例。
            session.refreshUpdateTime();
            store.updateSession(session);

            // 3. 模型返回结果: 小白＋小白=? 小白兔(two)
            message = Message.builder()
                .sessionId("session_a")
                .messageId("message_2")
                .createTime(TimeUtils.currentTimeMicroseconds())
                .content("小白＋小白=? 小白兔(two)")
                .metadata(new Metadata().put("meta示例:消息分类", "大模型"))
                .build();
            store.putMessage(message);
            // 大模型返回消息后，更新session信息。这里仅更新updateTime为例。这里是否更新取决于自己业务逻辑。
            session.refreshUpdateTime();
            store.updateSession(session);

            // 4. 用户再次提问：再来一个
            message = Message.builder()
                .sessionId("session_a")
                .messageId("message_3")
                .createTime(TimeUtils.currentTimeMicroseconds())
                .content("再来一个")
                .metadata(new Metadata().put("meta示例_访问来源", "web").put("meta示例:消息分类", "用户"))
                .build();
            store.putMessage(message);
            // 用户发送消息后，更新session信息。这里仅更新updateTime为例。
            session.refreshUpdateTime();
            store.updateSession(session);

            // 5. 时间倒序查出来最近的3条上下文消息，然后告诉大模型，这样大模型才知道“再来一个”的上下文是什么。 这里可以根据自己业务来选择是否使用metaDataFilter和消息创建时间的过滤。假如需要根据某一个消息往前找3条，那么可以将那条消息的创建时间放到
            // inclusiveStartCreateTime 参数中。
            // 如果前端有这3条消息，那么也不要查询数据库，直接传给大模型即可。
            store.listMessagesPaginated("session_a", 3, null, null, null, Order.DESC, null, null);

            // 6. 大模型返回：有一个躲猫猫社团，他们团长现在还没找到。
            message = Message.builder()
                .sessionId("session_a")
                .messageId("message_4")
                .createTime(TimeUtils.currentTimeMicroseconds())
                .content("有一个躲猫猫社团，他们团长现在还没找到。")
                .metadata(new Metadata().put("meta示例:消息分类", "大模型"))
                .build();
            store.putMessage(message);
            // 大模型返回消息后，更新session信息。这里仅更新updateTime为例。这里是否更新取决于自己业务逻辑。
            session.refreshUpdateTime();
            store.updateSession(session);
        }

        // 存量用户进来继续接着某一个会话的使用示例：
        {
            // 1. 展示用户最近活跃的session会话列表(根据更新时间排序)
            Response<Session> sessionResponse = store.listRecentSessionsPaginated("user小明", 10, null, null, null, null, null);
            List<Session> sessions = sessionResponse.getHits();

            // 2. [可选步骤] 用户点击第一个session，获取该session详情
            // (因为上述sessions列表是从二级索引中获取的根据时间排序的列表，有可能meta不全,取决于首次初始化MemoryStore时候是否将必要的meta信息补全到二级索引中。因此这里如有必要刻意考虑再拿一次完整信息)
            Session session = sessions.get(0);
            Session sessionDetail = store.getSession(session.getUserId(), session.getSessionId());

            // 3. 展示用户第一个session下的消息列表(可按需往前翻页，可按需进行MetaDataFilter过滤)
            Order order = Order.DESC;
            Response<Message> messageResponse = store.listMessagesPaginated(session.getSessionId(), 100, null, null, null, order, null, null);
            List<Message> messages = messageResponse.getHits();
            // nextToken为下一页的参数，如果为null，则表示没有下一页。nextToken可以传递给前端浏览器里，方便进行下次翻页。
            String nextToken = messageResponse.getNextToken();

            // 4. 后续跟上面“新用户”体验一致，用户继续提问
        }
    }
}
