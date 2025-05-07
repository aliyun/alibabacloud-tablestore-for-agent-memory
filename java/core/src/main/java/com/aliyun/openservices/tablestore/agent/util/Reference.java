package com.aliyun.openservices.tablestore.agent.util;

import org.jspecify.annotations.Nullable;

public class Reference<T> {

    private volatile T obj = null;

    public Reference() {}

    public Reference(T obj) {
        this.obj = obj;
    }

    @Nullable
    public T get() {
        return obj;
    }

    public void set(T obj) {
        this.obj = obj;
    }
}
