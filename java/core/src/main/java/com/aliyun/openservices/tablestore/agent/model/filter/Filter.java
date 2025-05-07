package com.aliyun.openservices.tablestore.agent.model.filter;

/**
 * All filters in {@link Filters}
 *
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
public interface Filter {}
