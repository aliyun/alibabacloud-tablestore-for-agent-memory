package com.aliyun.openservices.tablestore.agent.util;

import java.time.Instant;

public class TimeUtils {

    /**
     * Get the current time, unit: microseconds.
     *
     * <p>
     * Note: Actual precision depends on the machine running the code. On some machines, it can only achieve millisecond precision, resulting in
     * the last three digits being zero.
     * </p>
     *
     * @return Microsecond timestamp
     */
    public static long currentTimeMicroseconds() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000 + now.getNano() / 1000;
    }
}
