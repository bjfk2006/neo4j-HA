package com.neo4j.ha.sync.applier;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.ChangeEventType;
import com.neo4j.ha.common.model.EntityData;
import com.neo4j.ha.common.model.EntityType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for {@link ChangeApplier#splitByDuplicateElementId(List)}.
 *
 * <p>These tests pin down the algorithm's contract:
 * <ul>
 *   <li><b>No-op on the common case</b>: with all-distinct element ids we
 *       must return a single sub-batch containing the original events in
 *       original order. This is the BUG-081 mitigation's zero-overhead
 *       promise — anything else would regress sync throughput.</li>
 *   <li><b>Split on the first collision</b>: the second occurrence of an
 *       element id starts a fresh sub-batch whose {@code seen} set only
 *       carries the colliding id forward. This is what lets the second
 *       CREATE's {@code OPTIONAL MATCH stale} observe the first CREATE
 *       as a committed rel (the whole point of the mitigation).</li>
 *   <li><b>Stream order is preserved</b>: we never reorder events,
 *       because Redis stream order == primary commit order and applying
 *       CREATE-then-DELETE out of order would leave the standby with a
 *       zombie rel.</li>
 *   <li><b>Null element ids never trigger a split</b>: control events
 *       and entities without an id aren't part of the BUG-081 hazard and
 *       would fragment batches unnecessarily if they counted.</li>
 * </ul>
 */
class ChangeApplierSplitTest {

    @Test
    void emptyInput_returnsEmptyList() {
        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(List.of());
        assertEquals(0, out.size());
    }

    @Test
    void allDistinct_returnsSingleSubBatch() {
        ChangeEvent a = rel("eid-a");
        ChangeEvent b = rel("eid-b");
        ChangeEvent c = rel("eid-c");

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(List.of(a, b, c));

        assertEquals(1, out.size(), "no duplicates should produce a single sub-batch");
        assertEquals(3, out.get(0).size());
        assertSame(a, out.get(0).get(0));
        assertSame(b, out.get(0).get(1));
        assertSame(c, out.get(0).get(2));
    }

    @Test
    void twoEventsShareElementId_splitAtSecondOccurrence() {
        ChangeEvent create1 = rel("eid-522");
        ChangeEvent create2 = rel("eid-522");

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(List.of(create1, create2));

        assertEquals(2, out.size(), "duplicate element id must split into two sub-batches");
        assertEquals(1, out.get(0).size());
        assertSame(create1, out.get(0).get(0));
        assertEquals(1, out.get(1).size());
        assertSame(create2, out.get(1).get(0));
    }

    @Test
    void splitPreservesStreamOrderAcrossSubBatches() {
        // Typical BUG-081 layout: several unrelated events surrounding
        // two CREATEs that collided on Neo4j internal rel-id reuse.
        ChangeEvent unrelated1 = rel("eid-1");
        ChangeEvent create1 = rel("eid-522");
        ChangeEvent unrelated2 = rel("eid-2");
        ChangeEvent create2 = rel("eid-522");
        ChangeEvent unrelated3 = rel("eid-3");

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(
            List.of(unrelated1, create1, unrelated2, create2, unrelated3));

        assertEquals(2, out.size());

        // First sub-batch: everything before the collision, including the
        // first offender. Order preserved.
        assertEquals(List.of(unrelated1, create1, unrelated2), out.get(0));

        // Second sub-batch: starts with the colliding event, order preserved.
        assertEquals(List.of(create2, unrelated3), out.get(1));
    }

    @Test
    void threeOccurrencesOfSameId_produceThreeSubBatches() {
        // A chaos-triggered worst case where Neo4j reused the same rel-id
        // three times within one batch window. Each additional occurrence
        // must open a fresh sub-tx.
        ChangeEvent c1 = rel("eid-522");
        ChangeEvent c2 = rel("eid-522");
        ChangeEvent c3 = rel("eid-522");

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(List.of(c1, c2, c3));

        assertEquals(3, out.size());
        assertSame(c1, out.get(0).get(0));
        assertSame(c2, out.get(1).get(0));
        assertSame(c3, out.get(2).get(0));
    }

    @Test
    void nullElementId_doesNotTriggerSplit() {
        // Control events and any entity without an elementId (rare but
        // legal per EntityData) must not fragment the batch — they
        // aren't part of the BUG-081 hazard and splitting on them would
        // just cost sync throughput.
        ChangeEvent nullA = relWithEntity(null);
        ChangeEvent nullB = relWithEntity(null);

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(List.of(nullA, nullB));

        assertEquals(1, out.size(), "null element ids must not collide with each other");
        assertEquals(2, out.get(0).size());
    }

    @Test
    void nullEntity_doesNotTriggerSplit() {
        // Defensive: an event with null entity() (e.g. a pure control
        // marker) must round-trip through the split as a no-op.
        ChangeEvent ctrl1 = new ChangeEvent("evt-1", ChangeEventType.RELATIONSHIP_CREATED,
            "neo4j", 1L, 1L, "tx-1", null, null);
        ChangeEvent real = rel("eid-a");
        ChangeEvent ctrl2 = new ChangeEvent("evt-2", ChangeEventType.RELATIONSHIP_CREATED,
            "neo4j", 2L, 1L, "tx-2", null, null);

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(
            List.of(ctrl1, real, ctrl2));

        assertEquals(1, out.size());
        assertEquals(3, out.get(0).size());
    }

    @Test
    void largeBatchWithOneCollision_splitsIntoExactlyTwoSubBatches() {
        // Realistic PEL batch (100 events) with one collision mid-way —
        // the mitigation should pay exactly one extra commit round-trip,
        // not fragment further.
        List<ChangeEvent> events = new ArrayList<>();
        for (int i = 0; i < 50; i++) events.add(rel("unique-" + i));
        events.add(rel("eid-collision"));
        for (int i = 50; i < 98; i++) events.add(rel("unique-" + i));
        events.add(rel("eid-collision"));
        events.add(rel("unique-tail"));

        List<List<ChangeEvent>> out = ChangeApplier.splitByDuplicateElementId(events);

        assertEquals(2, out.size());
        assertEquals(51, out.get(0).size()); // 50 uniques + first collision
        assertEquals(50, out.get(1).size()); // second collision + 48 uniques + tail
        assertEquals(events.size(), out.get(0).size() + out.get(1).size(),
            "no events lost across the split boundary");
    }

    // ---- helpers -------------------------------------------------------

    private static ChangeEvent rel(String elementId) {
        return relWithEntity(elementId);
    }

    private static ChangeEvent relWithEntity(String elementId) {
        EntityData entity = new EntityData(
            EntityType.RELATIONSHIP, elementId, null, null, null,
            "start-eid", "end-eid", "RELATED_TO");
        return new ChangeEvent("evt-" + (elementId == null ? "null" : elementId),
            ChangeEventType.RELATIONSHIP_CREATED, "neo4j", 1L, 1L, "tx-1",
            entity, null);
    }
}
