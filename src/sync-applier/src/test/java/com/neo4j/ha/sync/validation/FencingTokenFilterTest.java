package com.neo4j.ha.sync.validation;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.ChangeEventType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BUG-037 regression: {@link FencingTokenFilter} must never drop events on the consumer
 * side. The publisher-side Lua script in {@code StreamPublisher.publish} is the sole
 * authority for admitting events into the Redis Stream; once an event has been accepted
 * there, the consumer must apply it regardless of how the consumer's `knownMaxToken`
 * compares to the event's token. The previous implementation dropped legitimate old-epoch
 * events that happened to still be in flight across a switchover, producing silent data
 * loss.
 */
class FencingTokenFilterTest {

    private FencingTokenFilter filter;

    @BeforeEach
    void setUp() {
        filter = new FencingTokenFilter();
    }

    @Test
    void filter_eventWithCurrentToken_accepted() {
        filter.updateToken(5);
        List<ChangeEvent> result = filter.filter(List.of(makeEvent("e1", 5)));

        assertEquals(1, result.size());
        assertEquals("e1", result.get(0).eventId());
    }

    @Test
    void filter_eventWithNewerToken_accepted_andUpdatesKnownMax() {
        filter.updateToken(5);
        List<ChangeEvent> result = filter.filter(List.of(makeEvent("e1", 10)));

        assertEquals(1, result.size());
        assertEquals("e1", result.get(0).eventId());
    }

    @Test
    void filter_oldEpochEventMidSwitchover_NEVER_dropped() {
        // The scenario that produced 601 missing nodes in the load/switchover test:
        // switchover advanced fencing-token to 1, but events written on the old primary
        // with token=0 are still waiting in the Redis Stream. They MUST be applied — the
        // Lua check already vetted them at publish time.
        filter.updateToken(1);

        List<ChangeEvent> inFlightOldEpoch = List.of(
                makeEvent("pre-switchover-1", 0),
                makeEvent("pre-switchover-2", 0),
                makeEvent("pre-switchover-3", 0)
        );
        List<ChangeEvent> result = filter.filter(inFlightOldEpoch);

        assertEquals(3, result.size(), "in-flight pre-switchover events must NOT be dropped");
    }

    @Test
    void filter_mixedEpochs_allPassThrough() {
        filter.updateToken(5);
        List<ChangeEvent> events = List.of(
                makeEvent("old1", 3),
                makeEvent("old2", 4),
                makeEvent("new1", 5),
                makeEvent("new2", 7)
        );

        List<ChangeEvent> result = filter.filter(events);

        assertEquals(4, result.size());
        assertEquals("old1", result.get(0).eventId());
        assertEquals("new2", result.get(3).eventId());
    }

    @Test
    void filter_emptyList_returnsEmpty() {
        assertTrue(filter.filter(List.of()).isEmpty());
    }

    @Test
    void filter_allValid_returnsAll() {
        List<ChangeEvent> events = List.of(
                makeEvent("e1", 1),
                makeEvent("e2", 2),
                makeEvent("e3", 3)
        );
        assertEquals(3, filter.filter(events).size());
    }

    private static ChangeEvent makeEvent(String eventId, long fencingToken) {
        return new ChangeEvent(
                eventId,
                ChangeEventType.NODE_CREATED,
                "neo4j",
                System.currentTimeMillis(),
                fencingToken,
                "tx-1",
                null,
                null
        );
    }
}
