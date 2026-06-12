package com.neo4j.ha.common.util;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

public class IdGenerator {

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Generate a UUID v7 (time-ordered).
     * Uses current timestamp in the most significant bits for natural ordering.
     */
    public static String uuidV7() {
        long timestamp = Instant.now().toEpochMilli();

        // UUID v7: 48-bit timestamp | 4-bit version(7) | 12-bit random | 2-bit variant | 62-bit random
        long msb = (timestamp << 16) | 0x7000L | (RANDOM.nextLong() & 0x0FFFL);
        long lsb = 0x8000000000000000L | (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL);

        return new UUID(msb, lsb).toString();
    }

    public static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
