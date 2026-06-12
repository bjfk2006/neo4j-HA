package com.neo4j.ha.agent.http.auth;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class SessionManagerTest {

    private static SessionManager mgr(long ttlMs, int maxPerUser, AtomicLong clock) {
        return new SessionManager(ttlMs, maxPerUser, clock::get, false);
    }

    @Test
    void createAndLookup() {
        var clock = new AtomicLong(1_000_000L);
        var sm = mgr(60_000L, 3, clock);
        var s = sm.create("alice", Role.ADMIN);
        assertNotNull(s.token());
        var found = sm.lookup(s.token());
        assertTrue(found.isPresent());
        assertEquals("alice", found.get().username());
        assertEquals(Role.ADMIN, found.get().role());
    }

    @Test
    void expiredSessionRejected() {
        var clock = new AtomicLong(0L);
        var sm = mgr(1_000L, 3, clock);
        var s = sm.create("alice", Role.ADMIN);
        clock.set(2_000L);
        assertTrue(sm.lookup(s.token()).isEmpty());
    }

    @Test
    void revokeClearsSession() {
        var clock = new AtomicLong(0L);
        var sm = mgr(60_000L, 3, clock);
        var s = sm.create("alice", Role.ADMIN);
        assertTrue(sm.revoke(s.token()));
        assertTrue(sm.lookup(s.token()).isEmpty());
        // double-revoke is harmless
        assertFalse(sm.revoke(s.token()));
    }

    @Test
    void perUserCapEvictsOldest() {
        var clock = new AtomicLong(0L);
        var sm = mgr(60_000L, 2, clock);
        var s1 = sm.create("alice", Role.ADMIN);
        clock.set(1L);
        var s2 = sm.create("alice", Role.ADMIN);
        clock.set(2L);
        var s3 = sm.create("alice", Role.ADMIN);
        // s1 (oldest) should be evicted
        assertTrue(sm.lookup(s1.token()).isEmpty());
        assertTrue(sm.lookup(s2.token()).isPresent());
        assertTrue(sm.lookup(s3.token()).isPresent());
    }

    @Test
    void slidingRenewalExtendsLife() {
        var clock = new AtomicLong(0L);
        var sm = mgr(10_000L, 3, clock);
        var s = sm.create("alice", Role.ADMIN);
        // advance to 90% of TTL — below renewal threshold (25% remaining)
        clock.set(9_000L);
        var s2 = sm.lookup(s.token()).orElseThrow();
        assertTrue(s2.expiresAtMs() > s.expiresAtMs(), "should be renewed");
        // far in future from original expiry
        clock.set(15_000L);
        assertTrue(sm.lookup(s.token()).isPresent(), "renewal must keep session alive");
    }

    @Test
    void revokeAllForUser() {
        var clock = new AtomicLong(0L);
        var sm = mgr(60_000L, 5, clock);
        sm.create("alice", Role.ADMIN);
        sm.create("alice", Role.ADMIN);
        sm.create("bob", Role.VIEWER);
        assertEquals(2, sm.revokeAllForUser("alice"));
        assertEquals(1, sm.activeCount());
    }
}
