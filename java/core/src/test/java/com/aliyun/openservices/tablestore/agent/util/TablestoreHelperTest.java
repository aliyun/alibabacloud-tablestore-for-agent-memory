package com.aliyun.openservices.tablestore.agent.util;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.aliyun.openservices.tablestore.agent.model.Session;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class TablestoreHelperTest {

    @Test
    void encodeNextPrimaryKeyToken() {

        // encode
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn(Session.SESSION_USER_ID, PrimaryKeyValue.fromString("123"))
            .addPrimaryKeyColumn(Session.SESSION_UPDATE_TIME, PrimaryKeyValue.fromLong(1744090297594000L))
            .addPrimaryKeyColumn(Session.SESSION_SESSION_ID, PrimaryKeyValue.fromString("123abc"))
            .build();
        String token = TablestoreHelper.encodeNextPrimaryKeyToken(primaryKey);
        Assertions.assertEquals("W1sidXNlcl9pZCIsIjEyMyJdLFsidXBkYXRlX3RpbWUiLDE3NDQwOTAyOTc1OTQwMDBdLFsic2Vzc2lvbl9pZCIsIjEyM2FiYyJdXQ==", token);

        // decode
        String nextToken = "W1sidXNlcl9pZCIsICIxMjMiXSwgWyJ1cGRhdGVfdGltZSIsIDE3NDQwOTAyOTc1OTQwMDBdLCBbInNlc3Npb25faWQiLCAiMTIzYWJjIl1d";
        PrimaryKey decodeNextPrimaryKey = TablestoreHelper.decodeNextPrimaryKeyToken(nextToken);
        Assertions.assertEquals(decodeNextPrimaryKey.size(), primaryKey.size());
        for (int i = 0; i < decodeNextPrimaryKey.size(); i++) {
            Assertions.assertEquals(decodeNextPrimaryKey.getPrimaryKeyColumn(i).getValue().toString(), primaryKey.getPrimaryKeyColumn(i).getValue().toString());
        }
    }

    @Test
    void testEmbedding() {
        float[] embedding = new float[10];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] = ThreadLocalRandom.current().nextFloat();
        }
        String embeddingString = TablestoreHelper.encodeEmbedding(embedding);
        log.info("embeddingString:{}", embeddingString);
        float[] decodeEmbedding = TablestoreHelper.decodeEmbedding(embeddingString);
        Assertions.assertArrayEquals(embedding, decodeEmbedding);
    }
}
