package com.aliyun.openservices.tablestore.agent.knowledge;

import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.model.sort.Sort;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class KnowledgeSearchRequest {

    /**
     * Tenant ID (Leave empty if multi-tenancy capability is not used)
     */
    @Singular(ignoreNullCollections = true)
    private Set<String> tenantIds;

    /**
     * Filtering conditions
     */
    private Filter metadataFilter;

    /**
     * Number of results to return
     */
    @Builder.Default
    private int limit = 10;

    /**
     * Token for the next page. Pass the token from the previous query result to continue pagination. Leave it empty for the first query.
     */
    private String nextToken;

    /**
     * Which fields to return
     */
    private List<String> columnsToGet;

    /**
     * Sorting (default ascending order by primary key)
     */
    @Singular(ignoreNullCollections = true)
    private List<Sort> sorts;

    /**
     * Variable arguments. Default users do not need to set this parameter.
     */
    @Singular(ignoreNullCollections = true)
    private Map<String, Object> varArgs;

}
