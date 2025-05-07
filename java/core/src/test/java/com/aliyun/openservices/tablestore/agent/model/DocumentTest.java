package com.aliyun.openservices.tablestore.agent.model;

import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.openservices.tablestore.agent.util.FakeEmbedding;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class DocumentTest {

    @Test
    void shortEmbedding() {

        FakeEmbedding fakeEmbedding1000 = new FakeEmbedding(1000);
        FakeEmbedding fakeEmbedding2 = new FakeEmbedding(2);

        Document document = new Document("1", "123");
        log.info("document:{}", document);

        document.setEmbedding(fakeEmbedding2.embed("abc"));
        log.info("document:{}", document);

        document.setEmbedding(fakeEmbedding1000.embed("abc"));
        log.info("document:{}", document);

    }
}
