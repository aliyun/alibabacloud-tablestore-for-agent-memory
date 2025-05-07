package com.aliyun.openservices.tablestore.agent.memory;

import com.aliyun.openservices.tablestore.agent.model.Metadata;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.Session;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import com.aliyun.openservices.tablestore.agent.model.sort.FieldSort;
import com.aliyun.openservices.tablestore.agent.model.sort.Order;
import com.aliyun.openservices.tablestore.agent.util.CollectionUtil;
import com.aliyun.openservices.tablestore.agent.util.TimeUtils;
import java.util.Iterator;
import java.util.List;

public class SessionExample {

    /**
     * 一个用户UserId会有n个Session会话，一个Session会话有n条Message消息
     */
    public void sessionExample(MemoryStore store) {

        // 创建一个 Session
        Session session = Session.builder()
            .userId("用户userId")
            .sessionId("会话sessionId")
            .updateTime(TimeUtils.currentTimeMicroseconds()) // 更新时间，展示最近消息时候会根据该值进行排序
            .metadata(new Metadata().put("meta1", "v1").put("meta2", 123))
            .build();

        // 写入一个 Session
        store.putSession(session);
        // 获取一个 Session
        store.getSession("用户userId", "会话sessionId");
        // 更新一个 Session
        store.updateSession(session);
        // 删除一个 Session
        store.deleteSession("用户userId", "会话sessionId");
        // 删除一个 UserId 下所有 Session 会话
        store.deleteSessions("用户userId");

        {
            // 展示最近活跃的会话(更新updateTime)，该查询从二级索引中查询数据，仅返回之前在MemoryStore初始化时候定义在二级索引里的属性列
            Iterator<Session> iterator = store.listRecentSessions("用户userId", null, null, null, -1L, null);
            // 如需将Iterator转换为List，可使用如下函数。
            List<Session> sessions = CollectionUtil.toList(iterator);
        }

        {
            // 展示最近活跃的会话(更新updateTime)，最多获取7条，并根据Meta过滤。更新时间需要在2025~2024(倒序排列)期间。
            Filter metadataFilter = Filters.and(Filters.eq("meta1", "v1"), Filters.gte("meta2", 30));
            long maxCount = 7; // maxCount设置为-1或null即可获取全部数据
            long inclusiveStartUpdateTime = 2025; // 假设2025是2025年的时间戳
            long inclusiveEndUpdateTime = 2024; // 假设2024是2024年的时间戳
            store.listRecentSessions("用户userId", metadataFilter, inclusiveStartUpdateTime, inclusiveEndUpdateTime, maxCount, null);
        }

        {
            // 使用token翻页的方式获取最近活跃的会话(更新updateTime)。该查询从二级索引中查询数据，仅返回之前在MemoryStore初始化时候定义在二级索引里的属性列.
            int pageSize = 100;
            Response<Session> response = store.listRecentSessionsPaginated("用户userId", pageSize, null, null, null, null, null);
            List<Session> sessions = response.getHits();
            // nextToken为下一页的token，如果为null，则表示没有下一页。nextToken可以传递给前端浏览器里，方便进行下次翻页。
            String nextToken = response.getNextToken();
        }
        {
            // 连续翻页获取最近活跃的会话(更新updateTime)
            int pageSize = 100;
            String nextToken = null;
            int total = 0;
            do {
                Response<Session> response = store.listRecentSessionsPaginated("用户userId", pageSize, null, null, null, nextToken, null);
                List<Session> sessionsList = response.getHits();
                total = total + sessionsList.size();
                nextToken = response.getNextToken();
            } while (nextToken != null);
            System.out.println("total:" + total);
        }
        {
            // 从表中获取一个用户的所有Session会话，可以根据MetaData进行过滤.
            store.listSessions("用户userId", null, null, null);
        }

        // 搜索Session会话(依赖初始化多元索引，可以跨用户搜索)
        {
            MemorySearchRequest request = MemorySearchRequest.builder()
                .metadataFilter(Filters.and(Filters.eq("meta1", "v1"), Filters.textMatch("text", "你好"), Filters.gte("meta2", 30)))
                .limit(50)
                .sort(new FieldSort("meta1", Order.DESC))
                .nextToken(null) // 可以将Response里的nextToken传递过来，进行下次翻页
                .build();
            Response<Session> response = store.searchSessions(request);
            List<Session> sessionList = response.getHits();
            // nextToken为下一页的token，如果为null，则表示没有下一页。nextToken可以传递给前端浏览器里，方便进行下次翻页。
            String nextToken = response.getNextToken();
        }
    }
}
