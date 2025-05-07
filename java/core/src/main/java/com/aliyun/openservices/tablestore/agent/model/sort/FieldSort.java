package com.aliyun.openservices.tablestore.agent.model.sort;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Accessors(chain = true)
@ToString
@SuperBuilder(toBuilder = true)
public class FieldSort implements Sort {

    private String field;

    @Builder.Default
    private Order order = Order.DESC;

}
