package com.neo4j.ha.common.util;

import java.util.concurrent.atomic.AtomicLong;

public class FencingTokenValidator {

    private final AtomicLong knownMaxToken = new AtomicLong(0);

    public boolean isValid(long token) {
        return token >= knownMaxToken.get();
    }

    public void updateToken(long newToken) {
        knownMaxToken.updateAndGet(current -> Math.max(current, newToken));
    }

    public long getCurrentToken() {
        return knownMaxToken.get();
    }
}
