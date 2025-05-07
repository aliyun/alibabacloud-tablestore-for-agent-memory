package com.aliyun.openservices.tablestore.agent.model.filter.operation;

import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class NotIn implements AbstractOperationFilter {

    private final String key;

    private final Collection<?> values;
}
