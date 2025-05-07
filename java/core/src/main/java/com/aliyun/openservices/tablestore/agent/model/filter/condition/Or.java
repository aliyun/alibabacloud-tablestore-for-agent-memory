package com.aliyun.openservices.tablestore.agent.model.filter.condition;

import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class Or implements AbstractConditionFilter {

    private List<Filter> filters;

}
