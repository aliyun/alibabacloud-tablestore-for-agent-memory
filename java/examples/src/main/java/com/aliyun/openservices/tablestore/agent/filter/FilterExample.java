package com.aliyun.openservices.tablestore.agent.filter;

import com.aliyun.openservices.tablestore.agent.model.filter.Filters;
import java.util.Arrays;

public class FilterExample {

    public static void main(String[] args) {

        /*
         * 比较运算符: eq, notEq, gt, gte, lt, lte, in, notIn, textMatch等. 使用Filters.xxx()可以查看更多.
         */
        {
            Filters.eq("city", "shanghai");
            Filters.gte("year", 2005);
            Filters.in("city", Arrays.asList("beijing", "hangzhou", "shanghai"));

            // 全文检索
            Filters.textMatch("content", "hello world");
        }
        // 逻辑运算符: and, or, not
        {
            // city=="shanghai" && year >= 2005
            Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005));
            // city=="shanghai" || year >= 2005
            Filters.or(Filters.eq("city", "shanghai"), Filters.gte("year", 2005));
            // !(city=="shanghai" || year >= 2005)
            Filters.not(Filters.or(Filters.eq("city", "shanghai"), Filters.gte("year", 2005)));
            // !(city=="shanghai" && year >= 2005)
            Filters.not(Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005)));
            // !(city=="shanghai" && year >= 2005) || city=="hangzhou"
            Filters.or(Filters.not(Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005))), Filters.eq("city", "hangzhou"));
        }
    }
}
