package com.neo4j.ha.agent.http.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterTest {

    @Test
    void underThresholdNotLocked() {
        var rl = new RateLimiter(5, 60_000L);
        for (int i = 0; i < 4; i++) rl.recordFailure("1.1.1.1");
        assertEquals(0L, rl.checkLockedRemainingMs("1.1.1.1"));
    }

    @Test
    void atThresholdLocked() {
        var rl = new RateLimiter(5, 60_000L);
        for (int i = 0; i < 5; i++) rl.recordFailure("1.1.1.1");
        long remaining = rl.checkLockedRemainingMs("1.1.1.1");
        assertTrue(remaining > 0 && remaining <= 60_000L);
    }

    @Test
    void successResetsCounter() {
        var rl = new RateLimiter(5, 60_000L);
        for (int i = 0; i < 4; i++) rl.recordFailure("1.1.1.1");
        rl.recordSuccess("1.1.1.1");
        rl.recordFailure("1.1.1.1");
        assertEquals(0L, rl.checkLockedRemainingMs("1.1.1.1"));
    }

    @Test
    void differentIpsIndependent() {
        var rl = new RateLimiter(5, 60_000L);
        for (int i = 0; i < 5; i++) rl.recordFailure("1.1.1.1");
        rl.recordFailure("2.2.2.2");
        assertTrue(rl.checkLockedRemainingMs("1.1.1.1") > 0);
        assertEquals(0L, rl.checkLockedRemainingMs("2.2.2.2"));
    }

    @Test
    void nullIpIgnored() {
        var rl = new RateLimiter(5, 60_000L);
        rl.recordFailure(null);
        rl.recordSuccess(null);
        assertEquals(0L, rl.checkLockedRemainingMs(null));
    }
}
