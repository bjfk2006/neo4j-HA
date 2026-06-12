package com.neo4j.ha.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NodeServiceStateTest {

    @Test
    void allExpectedStatesExist() {
        NodeServiceState[] states = NodeServiceState.values();
        assertEquals(3, states.length);
        assertNotNull(NodeServiceState.valueOf("OFFLINE"));
        assertNotNull(NodeServiceState.valueOf("SYNCING"));
        assertNotNull(NodeServiceState.valueOf("ONLINE"));
    }

    @Test
    void valueOfReturnsCorrectInstance() {
        assertEquals(NodeServiceState.OFFLINE, NodeServiceState.valueOf("OFFLINE"));
        assertEquals(NodeServiceState.SYNCING, NodeServiceState.valueOf("SYNCING"));
        assertEquals(NodeServiceState.ONLINE, NodeServiceState.valueOf("ONLINE"));
    }

    @Test
    void invalidStateThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> NodeServiceState.valueOf("UNKNOWN"));
    }

    @Test
    void ordinalOrder() {
        // Verify the declaration order matches the expected lifecycle: OFFLINE -> SYNCING -> ONLINE
        assertTrue(NodeServiceState.OFFLINE.ordinal() < NodeServiceState.SYNCING.ordinal());
        assertTrue(NodeServiceState.SYNCING.ordinal() < NodeServiceState.ONLINE.ordinal());
    }

    @Test
    void nameMatchesToString() {
        assertEquals("OFFLINE", NodeServiceState.OFFLINE.name());
        assertEquals("SYNCING", NodeServiceState.SYNCING.name());
        assertEquals("ONLINE", NodeServiceState.ONLINE.name());
    }
}
