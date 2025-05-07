package com.aliyun.openservices.tablestore.agent.model.filter;

import com.google.common.collect.Lists;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

@Slf4j
class FiltersTest {

    @Test
    void logTest() {
        Reflections reflections = new Reflections("com.aliyun.openservices.tablestore.agent.model.filter");
        Set<Class<? extends Filter>> allClass = reflections.getSubTypesOf(Filter.class);
        log.info("allClass:[{}]{}", allClass.size(), allClass);
        Assertions.assertEquals(
            17,
            allClass.size(),
            "allClass size is not 17, maybe you add a new filter, you must add a new test case and check the toString() result is readAble"
        );

        log.info("eq:{}", Filters.eq("key", "value"));
        Assertions.assertFalse(Filters.eq("key", "value").toString().contains("@"));
        log.info("gt:{}", Filters.gt("key", "value"));
        Assertions.assertFalse(Filters.gt("key", "value").toString().contains("@"));
        log.info("gte:{}", Filters.gte("key", "value"));
        Assertions.assertFalse(Filters.gte("key", "value").toString().contains("@"));
        log.info("lt:{}", Filters.lt("key", "value"));
        Assertions.assertFalse(Filters.lt("key", "value").toString().contains("@"));
        log.info("lte:{}", Filters.lte("key", "value"));
        Assertions.assertFalse(Filters.lte("key", "value").toString().contains("@"));
        log.info("in:{}", Filters.in("key", Lists.newArrayList("value", "value2")));
        Assertions.assertFalse(Filters.in("key", Lists.newArrayList("value", "value2")).toString().contains("@"));
        log.info("notIn:{}", Filters.notIn("key", Lists.newArrayList("value", "value2")));
        Assertions.assertFalse(Filters.notIn("key", Lists.newArrayList("value", "value2")).toString().contains("@"));
        log.info("vectorQuery:{}", Filters.vectorQuery("key", new float[] { 1.0f, 2.0f }));
        log.info("vectorQuery:{}", Filters.vectorQuery("key", new float[] { 1.0f, 2.0f }).setFilter(Filters.eq("key", "value")));
        Assertions.assertFalse(Filters.vectorQuery("key", new float[] { 1.0f, 2.0f }).setFilter(Filters.eq("key", "value")).toString().contains("@"));
        log.info("textMatch:{}", Filters.textMatch("key", "value"));
        Assertions.assertFalse(Filters.textMatch("key", "value").toString().contains("@"));
        log.info("textMatchPhrase:{}", Filters.textMatchPhrase("key", "value"));
        Assertions.assertFalse(Filters.textMatchPhrase("key", "value").toString().contains("@"));
        log.info("exists:{}", Filters.exists("key"));
        Assertions.assertFalse(Filters.exists("key").toString().contains("@"));

        log.info("and:{}", Filters.and(Filters.eq("key", "value"), Filters.eq("key2", "value2")));
        Assertions.assertFalse(Filters.and(Filters.eq("key", "value"), Filters.eq("key2", "value2")).toString().contains("@"));
        log.info("or:{}", Filters.or(Filters.eq("key", "value"), Filters.eq("key2", "value2")));
        Assertions.assertFalse(Filters.or(Filters.eq("key", "value"), Filters.eq("key2", "value2")).toString().contains("@"));
        log.info("not:{}", Filters.not(Filters.eq("key", "value")));
        Assertions.assertFalse(Filters.not(Filters.eq("key", "value")).toString().contains("@"));
    }
}
