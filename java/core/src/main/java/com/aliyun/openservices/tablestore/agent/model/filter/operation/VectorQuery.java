package com.aliyun.openservices.tablestore.agent.model.filter.operation;

import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@Data
@Accessors(chain = true)
@ToString()
@SuperBuilder(toBuilder = true)
public class VectorQuery implements AbstractOperationFilter {

    private final String key;

    @ToString.Exclude
    private final float[] queryVector;

    private int topK;

    private Filter filter;

    private Float minScore;

    public VectorQuery(String key, float[] queryVector) {
        this.key = key;
        this.queryVector = queryVector;
        this.topK = 20;
    }

    @ToString.Include(name = "queryVector")
    private String shortQueryVector() {
        return TablestoreHelper.maxOrNull(queryVector == null ? null : TablestoreHelper.encodeEmbedding(queryVector), 80, "]");
    }
}
