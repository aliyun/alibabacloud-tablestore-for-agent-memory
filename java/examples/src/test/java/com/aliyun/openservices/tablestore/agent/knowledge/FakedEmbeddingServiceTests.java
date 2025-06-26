package com.aliyun.openservices.tablestore.agent.knowledge;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class FakedEmbeddingServiceTests {

    @Test
    void testEmbedding() throws Exception {
        FakedEmbeddingService fakedEmbeddingService = new FakedEmbeddingService(768);
        float[] embedding = fakedEmbeddingService.embed("The World");
        Assertions.assertEquals(768, embedding.length);
    }
}
