package com.aliyun.openservices.tablestore.agent.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CollectionUtil {

    public static <T> List<T> toList(Iterator<T> iterable) {
        List<T> list = new ArrayList<>();
        while (iterable.hasNext()) {
            list.add(iterable.next());
        }
        return list;
    }
}
