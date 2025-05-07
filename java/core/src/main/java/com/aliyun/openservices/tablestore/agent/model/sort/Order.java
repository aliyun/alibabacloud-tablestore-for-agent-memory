package com.aliyun.openservices.tablestore.agent.model.sort;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Order {

    /**
     * Ascending order
     */
    ASC("FORWARD"),

    /**
     * Descending order
     */
    DESC("BACKWARD");

    private final String direction;
}
