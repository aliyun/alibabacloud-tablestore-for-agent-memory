package com.aliyun.openservices.tablestore.agent.memory;

import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.sort.Sort;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@Data
@Accessors(chain = true)
@ToString
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class MemorySearchRequest {

    /**
     * Filtering conditions
     */
    private Filter metadataFilter;

    /**
     * Number of results returned
     */
    @Builder.Default
    private int limit = 10;

    /**
     * Next page token. Pass the token from the previous query result for continuous pagination. Leave it empty for the first query.
     */
    private String nextToken;

    /**
     * Sorting (default ascending order by primary key)
     */
    @Singular(ignoreNullCollections = true)
    private List<Sort> sorts;

}
