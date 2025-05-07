package com.aliyun.openservices.tablestore.agent.util;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@ToString
public class Pair<L, R> {

    public final L left;

    public final R right;

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    private Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getKey() {
        return left;
    }

    public R getValue() {
        return right;
    }
}
