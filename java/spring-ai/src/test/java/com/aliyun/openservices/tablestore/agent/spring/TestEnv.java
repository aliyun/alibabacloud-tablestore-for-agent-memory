package com.aliyun.openservices.tablestore.agent.spring;

import com.alicloud.openservices.tablestore.SyncClient;
import org.junit.jupiter.api.Assumptions;

class TestEnv {

    private static volatile SyncClient client;

    static SyncClient getClient() {
        if (client == null) {
            synchronized (TestEnv.class) {
                if (client == null) {
                    String endPoint = System.getenv("tablestore_end_point");
                    String instanceName = System.getenv("tablestore_instance_name");
                    String accessKeyId = System.getenv("tablestore_access_key_id");
                    String accessKeySecret = System.getenv("tablestore_access_key_secret");
                    if (endPoint == null || instanceName == null || accessKeyId == null || accessKeySecret == null) {
                        Assumptions.abort(
                            "env tablestore_end_point, tablestore_instance_name, tablestore_access_key_id, tablestore_access_key_secret is not set"
                        );
                    }
                    client = new SyncClient(endPoint, accessKeyId, accessKeySecret, instanceName);
                }
            }
        }
        return client;
    }
}
