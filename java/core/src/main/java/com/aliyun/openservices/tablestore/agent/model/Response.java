package com.aliyun.openservices.tablestore.agent.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@ToString
@AllArgsConstructor
public class Response<T> {

    /**
     * Search results
     */
    private List<T> hits;

    /**
     * Indicates the starting position for the next page.
     *
     * <p>
     * If nextToken is null, it means there is no next page.
     */
    private String nextToken;
}
