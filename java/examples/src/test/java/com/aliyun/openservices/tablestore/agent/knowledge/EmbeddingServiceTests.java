package com.aliyun.openservices.tablestore.agent.knowledge;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class EmbeddingServiceTests {

    @Test
    void testEmbedding() throws Exception {
        try (EmbeddingService embeddingService = new EmbeddingService()) {
            float[] embedding = embeddingService.embed("The World");
            Assertions.assertEquals(768, embedding.length);
        }
    }
}
