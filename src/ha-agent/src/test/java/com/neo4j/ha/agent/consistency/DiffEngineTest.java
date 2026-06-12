package com.neo4j.ha.agent.consistency;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The full {@link DiffEngine#diff} path requires a live Neo4j cluster and
 * is exercised by integration tests. Here we cover the small pure pieces
 * that are unit-testable in isolation.
 */
class DiffEngineTest {

    @Test
    void diffEntryRecordHoldsAllFields() {
        // Smoke test: ensures the DiffEntry record signature is stable so
        // refactors that shift field order or drop fields break this test
        // before they break the JSON wire format.
        var entry = new DiffEngine.DiffEntry(
            "4:abc:1234", "node",
            java.util.List.of("Person"),
            Map.of("name", "Alice"),
            null,
            "primary-hash", null,
            null
        );
        assertEquals("4:abc:1234", entry.elementId());
        assertEquals("node", entry.kind());
        assertEquals(java.util.List.of("Person"), entry.labels());
        assertEquals(Map.of("name", "Alice"), entry.primaryProps());
        assertNull(entry.standbyProps());
        assertEquals("primary-hash", entry.primaryHash());
        assertNull(entry.standbyHash());
    }

    @Test
    void nodeDiffEmptyByDefault() {
        var nd = new DiffEngine.NodeDiff(
            java.util.List.of(), java.util.List.of(), java.util.List.of(),
            42, null);
        assertEquals(42, nd.matched());
        assertNull(nd.error());
        assertTrue(nd.missing().isEmpty());
        assertTrue(nd.extra().isEmpty());
        assertTrue(nd.propDiff().isEmpty());
    }

    @Test
    void scopeEnumStable() {
        // Wire format depends on these names — refactor with care.
        assertEquals("RECENT", DiffEngine.Scope.RECENT.name());
        assertEquals("LABEL",  DiffEngine.Scope.LABEL.name());
        assertEquals("RANDOM", DiffEngine.Scope.RANDOM.name());
        assertEquals("NODE", DiffEngine.Kind.NODE.name());
        assertEquals("REL",  DiffEngine.Kind.REL.name());
        assertEquals("BOTH", DiffEngine.Kind.BOTH.name());
    }
}
