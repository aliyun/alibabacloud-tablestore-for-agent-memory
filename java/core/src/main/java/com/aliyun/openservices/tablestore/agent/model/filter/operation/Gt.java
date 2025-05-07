package com.aliyun.openservices.tablestore.agent.model.filter.operation;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class Gt implements AbstractOperationFilter {

    private final String key;

    private final Object value;
}
