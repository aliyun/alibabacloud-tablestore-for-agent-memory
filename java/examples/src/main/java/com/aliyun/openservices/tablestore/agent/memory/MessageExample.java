package com.aliyun.openservices.tablestore.agent.memory;

import com.aliyun.openservices.tablestore.agent.model.Message;
import com.aliyun.openservices.tablestore.agent.model.Metadata;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import com.aliyun.openservices.tablestore.agent.model.sort.FieldSort;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.util.CollectionUtil;
import com.aliyun.openservices.tablestore.agent.util.TimeUtils;
import java.util.Iterator;
import java.util.List;

public class MessageExample {

    /**
     * 一个Session会话有n条Message聊天消息
     */
    public void messageExample(MemoryStore store) {

        // 创建一个 Message
        Message message = Message.builder()
            .sessionId("sessionId")
            .messageId("messageId")
            .createTime(TimeUtils.currentTimeMicroseconds())
            .content("content")
            .metadata(new Metadata().put("meta1", "v1").put("meta2", 123))
            .build();

        // 写入
        store.putMessage(message);
        // 获取单条消息 (第三个参数createTime是可选参数，设置该参数可提高获取性能)
        store.getMessage(message.getSessionId(), message.getMessageId(), message.getCreateTime());
        // 更新
        store.updateMessage(message);
        // 删除消息
        store.deleteMessage(message);
        // 删除消息 (第三个参数createTime是可选参数，设置该参数可提高删除性能)
        store.deleteMessage(message.getSessionId(), message.getMessageId(), message.getCreateTime());

        // 删除一个Session会话下的所有消息
        store.deleteMessages("session会话Id");

        // 列出一个Session会话下的所有消息
        Iterator<Message> iterator = store.listMessages("session会话Id");
        // 如需将Iterator转换为List，可使用如下函数。
        List<Message> messages = CollectionUtil.toList(iterator);

        // 倒序列出一个Session会话下的最多7条数据
        long maxCount = 7; // maxCount设置为-1或null即可获取全部数据
        Order order = Order.DESC;
        store.listMessages("session会话Id", null, null, null, order, maxCount, null);

        /**
         * 倒序列出一个Session会话下的最多7条数据，这7条数据需要满足Filter条件: meta1==‘v1’ && meta2>=30 其它过滤条件写法参考:
         * {@link com.aliyun.openservices.tablestore.agent.filter.FilterExample)
         */
        Filter metadataFilter = Filters.and(Filters.eq("meta1", "v1"), Filters.gte("meta2", 30));
        store.listMessages("session会话Id", metadataFilter, null, null, order, maxCount, null);

        // 倒序列出一个Session会话下的最多7条数据，这7条数据需要满足Filter条件: meta1==‘v1’ && meta2>=30, 消息的创建时间需要在2025~2024(倒序)年期间。
        long inclusiveStartCreateTime = 2025; // 假设2025是2025年的时间戳
        long inclusiveEndCreateTime = 2024; // 假设2024是2024年的时间戳
        store.listMessages("session会话Id", metadataFilter, inclusiveStartCreateTime, inclusiveEndCreateTime, order, maxCount, null);

        // 分页获取Session会话下的10条数据。这10条数据需要满足Filter条件: meta1==‘v1’ && meta2>=30, 消息的创建时间需要在2025~2024(倒序)年期间。
        {
            int pageSize = 10;
            Response<Message> messageResponse = store.listMessagesPaginated(
                "session会话Id",
                pageSize,
                metadataFilter,
                inclusiveStartCreateTime,
                inclusiveEndCreateTime,
                order,
                null,
                null
            );
            List<Message> messagesList = messageResponse.getHits();
            // nextToken为下一页的token，如果为null，则表示没有下一页。nextToken可以传递给前端浏览器里，方便进行下次翻页。
            String nextToken = messageResponse.getNextToken();
        }
        // 通过翻页获取全部数据
        {
            int pageSize = 100;
            String nextToken = null;
            int total = 0;
            do {
                Response<Message> messageResponse = store.listMessagesPaginated("session会话Id", pageSize, null, null, null, null, nextToken, null);
                List<Message> messagesList = messageResponse.getHits();
                total = total + messagesList.size();
                nextToken = messageResponse.getNextToken();
            } while (nextToken != null);
            System.out.println("total:" + total);
        }

        // 搜索Message会话(依赖初始化多元索引，可以跨Session搜索)
        {
            MemorySearchRequest request = MemorySearchRequest.builder()
                .metadataFilter(Filters.and(Filters.eq("meta1", "v1"), Filters.textMatch("text", "你好"), Filters.gte("meta2", 30)))
                .limit(50)
                .sort(new FieldSort("meta1", Order.DESC))
                .nextToken(null) // 可以将Response里的nextToken传递过来，进行下次翻页
                .build();
            Response<Message> response = store.searchMessages(request);
            List<Message> messageList = response.getHits();
            // nextToken为下一页的token，如果为null，则表示没有下一页。nextToken可以传递给前端浏览器里，方便进行下次翻页。
            String nextToken = response.getNextToken();
        }
    }
}
