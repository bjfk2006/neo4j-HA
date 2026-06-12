package com.neo4j.ha.cdc.polling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PollingStateTest {

    @Test
    void initialStateHasZeroTimestampsAndEmptyIds() {
        PollingState state = PollingState.initial();

        assertEquals(0, state.getLastNodeTs());
        assertEquals("", state.getLastNodeEid());
        assertEquals(0, state.getLastRelTs());
        assertEquals("", state.getLastRelEid());
        assertEquals(0, state.getLastDeleteTs());
        assertEquals("", state.getLastDeleteEid());
        // Legacy composite getters
        assertEquals(0, state.getLastTs());
        assertEquals("", state.getLastElementId());
    }

    @Test
    void constructorSetsAllFields() {
        PollingState state = new PollingState(
            100L, "node-1",
            200L, "rel-1",
            300L, "del-1");

        assertEquals(100L, state.getLastNodeTs());
        assertEquals("node-1", state.getLastNodeEid());
        assertEquals(200L, state.getLastRelTs());
        assertEquals("rel-1", state.getLastRelEid());
        assertEquals(300L, state.getLastDeleteTs());
        assertEquals("del-1", state.getLastDeleteEid());
    }

    @Test
    void legacyGetLastTsReturnsMaxOfNodeAndRel() {
        PollingState state = new PollingState(100L, "n", 200L, "r", 999L, "d");
        assertEquals(200L, state.getLastTs(), "legacy lastTs should be max(node, rel), excluding delete");
    }

    @Test
    void legacyGetLastElementIdPicksFromLarger() {
        PollingState relHigher = new PollingState(100L, "n1", 200L, "r1", 0L, "");
        assertEquals("r1", relHigher.getLastElementId());

        PollingState nodeHigher = new PollingState(300L, "n2", 200L, "r2", 0L, "");
        assertEquals("n2", nodeHigher.getLastElementId());

        PollingState tieRelWins = new PollingState(100L, "n3", 100L, "r3", 0L, "");
        assertEquals("r3", tieRelWins.getLastElementId());
    }

    @Test
    void setLastNodeTsUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastNodeTs(42L);
        assertEquals(42L, state.getLastNodeTs());
    }

    @Test
    void setLastNodeEidUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastNodeEid("node-99");
        assertEquals("node-99", state.getLastNodeEid());
    }

    @Test
    void setLastRelTsUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastRelTs(77L);
        assertEquals(77L, state.getLastRelTs());
    }

    @Test
    void setLastRelEidUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastRelEid("rel-99");
        assertEquals("rel-99", state.getLastRelEid());
    }

    @Test
    void setLastDeleteTsUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastDeleteTs(999L);
        assertEquals(999L, state.getLastDeleteTs());
    }

    @Test
    void setLastDeleteEidUpdatesValue() {
        PollingState state = PollingState.initial();
        state.setLastDeleteEid("del-5");
        assertEquals("del-5", state.getLastDeleteEid());
    }

    @Test
    void settersOverwritePreviousValues() {
        PollingState state = new PollingState(10L, "a", 20L, "b", 30L, "c");

        state.setLastNodeTs(100L);
        state.setLastNodeEid("x");
        state.setLastRelTs(200L);
        state.setLastRelEid("y");
        state.setLastDeleteTs(300L);
        state.setLastDeleteEid("z");

        assertEquals(100L, state.getLastNodeTs());
        assertEquals("x", state.getLastNodeEid());
        assertEquals(200L, state.getLastRelTs());
        assertEquals("y", state.getLastRelEid());
        assertEquals(300L, state.getLastDeleteTs());
        assertEquals("z", state.getLastDeleteEid());
    }

    @Test
    void nullEidsAreCoercedToEmptyString() {
        PollingState state = new PollingState(1L, null, 2L, null, 3L, null);
        assertEquals("", state.getLastNodeEid());
        assertEquals("", state.getLastRelEid());
        assertEquals("", state.getLastDeleteEid());

        state.setLastNodeEid(null);
        state.setLastRelEid(null);
        state.setLastDeleteEid(null);
        assertEquals("", state.getLastNodeEid());
        assertEquals("", state.getLastRelEid());
        assertEquals("", state.getLastDeleteEid());
    }
}
