package com.aliyun.openservices.tablestore.agent.knowledge;

import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FakedEmbeddingService {

    private final int dim;

    public FakedEmbeddingService(int dim) {
        this.dim = dim;
    }

    public float[] embed(String text) {
        return randomVector(dim);
    }

    public static float[] randomVector(int dim) {
        float[] vec = new float[dim];
        for (int i = 0; i < dim; i++) {
            vec[i] = ThreadLocalRandom.current().nextFloat();
            if (ThreadLocalRandom.current().nextBoolean()) {
                vec[i] = -vec[i];
            }
        }
        return l2normalize(vec, true);
    }

    public static float[] l2normalize(float[] v, boolean throwOnZero) {
        double squareSum = 0.0f;
        int dim = v.length;
        for (float x : v) {
            squareSum += x * x;
        }
        if (squareSum == 0) {
            if (throwOnZero) {
                throw new IllegalArgumentException("normalize a zero-length vector");
            } else {
                return v;
            }
        }
        double length = Math.sqrt(squareSum);
        for (int i = 0; i < dim; i++) {
            v[i] /= length;
        }
        return v;
    }
}
