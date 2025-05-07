package com.aliyun.openservices.tablestore.agent.util;

import java.util.concurrent.ThreadLocalRandom;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class FakeEmbedding {

    private int dimension;

    public float[] embed(String text) {
        float[] embedding = new float[dimension];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = ThreadLocalRandom.current().nextFloat();
        }
        return embedding;
    }
}
