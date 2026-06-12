package com.neo4j.ha.agent.consistency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EntityCounterTest {

    @Test
    void nullDriverReturnsErrorResult() {
        var counter = new EntityCounter("neo4j");
        var r = counter.count(null);
        assertFalse(r.isSuccess());
        assertNotNull(r.error());
        assertNull(r.nodeCount());
        assertNull(r.relCount());
        assertTrue(r.byLabel().isEmpty());
    }

    @Test
    void countResultSuccessFlag() {
        var success = new EntityCounter.CountResult(100L, 50L,
            java.util.Map.of("Person", 100L), 25L, null);
        assertTrue(success.isSuccess());

        var failed = new EntityCounter.CountResult(null, null,
            java.util.Map.of(), 10L, "timeout");
        assertFalse(failed.isSuccess());
    }
}
