package com.aliyun.openservices.tablestore.agent.model.filter;

import com.aliyun.openservices.tablestore.agent.model.filter.condition.And;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.Not;
import com.aliyun.openservices.tablestore.agent.model.filter.condition.Or;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Eq;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Exists;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Gt;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Gte;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.In;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Lt;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.Lte;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.NotEq;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.NotIn;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.TextMatch;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.TextMatchPhrase;
import com.aliyun.openservices.tablestore.agent.model.filter.operation.VectorQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * <p>
 * Examples:
 * </p>
 * 
 * <pre>
 * 
 * // Comparison operators: eq, notEq, gt, gte, lt, lte, in, notIn, textMatch etc. Use Filters.xxx() for more details.
 * {
 *     Filters.eq("city", "shanghai");
 *     Filters.gte("year", 2005);
 *     Filters.in("city", List.of("beijing", "hangzhou", "shanghai"));
 * 
 *     // Full-text search
 *     Filters.textMatch("content", "hello world");
 * }
 * // Logical operators: and, or, not
 * {
 *     // city=="shanghai" && year >= 2005
 *     Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005));
 *     // city=="shanghai" || year >= 2005
 *     Filters.or(Filters.eq("city", "shanghai"), Filters.gte("year", 2005));
 *     // !(city=="shanghai" || year >= 2005)
 *     Filters.not(Filters.or(Filters.eq("city", "shanghai"), Filters.gte("year", 2005)));
 *     // !(city=="shanghai" && year >= 2005)
 *     Filters.not(Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005)));
 *     // !(city=="shanghai" && year >= 2005) || city=="hangzhou"
 *     Filters.or(Filters.not(Filters.and(Filters.eq("city", "shanghai"), Filters.gte("year", 2005))), Filters.eq("city", "hangzhou"));
 * }
 * </pre>
 */
public class Filters {

    public static And and(List<Filter> filters) {
        return new And(filters);
    }

    public static And and(Filter... filters) {
        ArrayList<Filter> arrayList = new ArrayList<>(Arrays.asList(filters));
        return new And(arrayList);
    }

    public static Or or(List<Filter> filters) {
        return new Or(filters);
    }

    public static Or or(Filter... filters) {
        ArrayList<Filter> arrayList = new ArrayList<>(Arrays.asList(filters));
        return new Or(arrayList);
    }

    public static Not not(List<Filter> filters) {
        return new Not(filters);
    }

    public static Not not(Filter... filters) {
        ArrayList<Filter> arrayList = new ArrayList<>(Arrays.asList(filters));
        return new Not(arrayList);
    }

    public static Eq eq(String key, Object value) {
        return new Eq(key, value);
    }

    public static NotEq notEq(String key, Object value) {
        return new NotEq(key, value);
    }

    public static Gt gt(String key, Object value) {
        return new Gt(key, value);
    }

    public static Gte gte(String key, Object value) {
        return new Gte(key, value);
    }

    public static Lt lt(String key, Object value) {
        return new Lt(key, value);
    }

    public static Lte lte(String key, Object value) {
        return new Lte(key, value);
    }

    public static In in(String key, Collection<?> values) {
        return new In(key, values);
    }

    public static Filter notIn(String key, Collection<?> values) {
        return new NotIn(key, values);
    }

    public static VectorQuery vectorQuery(String key, float[] queryVector) {
        return new VectorQuery(key, queryVector);
    }

    public static TextMatch textMatch(String key, String value) {
        return new TextMatch(key, value);
    }

    public static TextMatchPhrase textMatchPhrase(String key, String value) {
        return new TextMatchPhrase(key, value);
    }

    public static Exists exists(String key) {
        return new Exists(key);
    }
}
