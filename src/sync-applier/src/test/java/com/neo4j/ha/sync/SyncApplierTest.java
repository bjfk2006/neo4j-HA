package com.neo4j.ha.sync;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.ChangeEventType;
import com.neo4j.ha.common.model.EntityData;
import com.neo4j.ha.common.model.EntityType;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.common.redis.StreamConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Regression tests for {@link SyncApplier#isForThisTarget}.
 *
 * <p>Bug history:</p>
 * <ul>
 *   <li><b>BUG-071</b> (2026-04-17): {@code FULL_SYNC_START} and {@code
 *     FULL_SYNC_END} are published to the shared {@code changesStreamKey},
 *     so every standby's consumer group receives them. Without this filter,
 *     every standby — not just the intended target — would wipe and re-import
 *     its own database on every {@code OldPrimaryRecovery} or manual
 *     {@code /cluster/fullsync} call. Filter lives in SyncApplier's
 *     {@code FullSyncCallback} wrapper and delegates to this helper.</li>
 * </ul>
 */
class SyncApplierTest {

    @Test
    void bug071_matchingTarget_accepted() {
        ChangeEvent event = makeControlEvent(ChangeEventType.FULL_SYNC_START, "node-01", 10L);

        assertTrue(SyncApplier.isForThisTarget(event, "node-01"),
            "standby whose id matches entity.elementId must accept the control event");
    }

    @Test
    void bug071_mismatchedTarget_skipped() {
        ChangeEvent event = makeControlEvent(ChangeEventType.FULL_SYNC_START, "node-01", 10L);

        assertFalse(SyncApplier.isForThisTarget(event, "node-02"),
            "standby whose id differs from entity.elementId must NOT act on the control event " +
                "(this is the BUG-071 regression: before the fix, node-02 wiped its own DB " +
                "when node-01 was the sync target)");
    }

    @Test
    void bug071_matchingTargetEnd_accepted() {
        ChangeEvent event = makeControlEvent(ChangeEventType.FULL_SYNC_END, "node-03", 20L);

        assertTrue(SyncApplier.isForThisTarget(event, "node-03"),
            "symmetric filter applies to FULL_SYNC_END");
    }

    @Test
    void bug071_nullEntity_treatedAsNotForMe() {
        ChangeEvent event = new ChangeEvent(
            "evt-null-entity",
            ChangeEventType.FULL_SYNC_START,
            "neo4j",
            System.currentTimeMillis(),
            1L,
            "tx-1",
            null,    // null entity (malformed control event)
            null
        );

        assertFalse(SyncApplier.isForThisTarget(event, "node-01"),
            "fail-safe: malformed control event with null entity must not trigger full sync");
    }

    @Test
    void bug071_nullElementId_treatedAsNotForMe() {
        EntityData entity = new EntityData(
            EntityType.NODE,
            null,    // null elementId
            List.of(),
            Map.of("snapshotTs", 1L),
            null, null, null, null
        );
        ChangeEvent event = new ChangeEvent(
            "evt-null-elementid",
            ChangeEventType.FULL_SYNC_START,
            "neo4j",
            System.currentTimeMillis(),
            1L,
            "tx-1",
            entity,
            null
        );

        assertFalse(SyncApplier.isForThisTarget(event, "node-01"),
            "fail-safe: null elementId must not trigger full sync on any standby");
    }

    @Test
    void bug071_nullEvent_treatedAsNotForMe() {
        assertFalse(SyncApplier.isForThisTarget(null, "node-01"),
            "fail-safe: null event must not trigger full sync");
    }

    // --------------------------------------------------------------------
    // BUG-074: schedulePendingRecovery drains the per-node PEL on the next
    // consumeLoop iteration. Before this, a standby outage left its consumer-
    // group PEL stuffed with events that were delivered-but-unacked while the
    // Bolt endpoint was unreachable; subsequent XREADGROUP ">" calls never
    // re-read them, and the standby permanently diverged from the primary
    // (observed as missing CREATEs + "delete leaks" in chaos-test reports).
    // --------------------------------------------------------------------

    @Test
    void bug074_schedulePendingRecovery_knownNode_setsFlag() {
        SyncApplier applier = newApplier();
        Driver mockDriver = mock(Driver.class);
        applier.injectStandbyDriverForTest("node-02", mockDriver);

        applier.schedulePendingRecovery("node-02");

        assertTrue(applier.isPendingRecoveryScheduled("node-02"),
            "scheduling a known standby must arm the flag so the next consumeLoop "
                + "iteration drains its PEL via XREADGROUP \"0-0\"");
    }

    @Test
    void bug074_schedulePendingRecovery_unknownNode_isNoop() {
        SyncApplier applier = newApplier();
        // standbyDrivers intentionally empty

        applier.schedulePendingRecovery("node-99");

        assertFalse(applier.isPendingRecoveryScheduled("node-99"),
            "scheduling a node that was never added must not populate the schedule; "
                + "otherwise HealthChecker callbacks for nodes outside this agent's "
                + "cluster view would create phantom PEL replays");
    }

    @Test
    void bug074_schedulePendingRecovery_nullNode_isNoop() {
        SyncApplier applier = newApplier();

        applier.schedulePendingRecovery(null);

        // No NPE, nothing scheduled. Defensive because the HealthChecker contract
        // does not promise non-null nodeIds at all call sites.
        assertFalse(applier.isPendingRecoveryScheduled(null));
    }

    @Test
    void bug074_schedulePendingRecovery_isIdempotent() {
        SyncApplier applier = newApplier();
        Driver mockDriver = mock(Driver.class);
        applier.injectStandbyDriverForTest("node-02", mockDriver);

        applier.schedulePendingRecovery("node-02");
        applier.schedulePendingRecovery("node-02");
        applier.schedulePendingRecovery("node-02");

        assertTrue(applier.isPendingRecoveryScheduled("node-02"),
            "repeated schedule calls must collapse to a single scheduled replay so "
                + "a flapping standby does not trigger concurrent replays");
    }

    @Test
    void bug074_schedulePendingRecovery_multipleNodes_independent() {
        SyncApplier applier = newApplier();
        applier.injectStandbyDriverForTest("node-02", mock(Driver.class));
        applier.injectStandbyDriverForTest("node-03", mock(Driver.class));

        applier.schedulePendingRecovery("node-02");

        assertTrue(applier.isPendingRecoveryScheduled("node-02"));
        assertFalse(applier.isPendingRecoveryScheduled("node-03"),
            "per-node schedule state must not leak across nodes; otherwise one "
                + "flapping standby would force every standby to replay its PEL");
    }

    private static SyncApplier newApplier() {
        SyncApplierConfig cfg = new SyncApplierConfig(
            "sync-applier",                        // consumerGroup
            100,                                   // consumerBatchSize
            1000L,                                 // blockTimeoutMs
            "cypher",                              // applyMode
            true,                                  // batchCommit
            3,                                     // maxRetries
            100L,                                  // retryDelayMs
            10000,                                 // duplicateDetectorMaxSize
            "neo4j:cdc:neo4j:changes",             // changesStreamKey
            "neo4j:cdc:neo4j:fullsync",            // fullsyncStreamKey
            5000L,                                 // syncLagThreshold
            30000L,                                // stableDurationMs
            5000L                                  // checkIntervalMs
        );
        return new SyncApplier(cfg, mock(StreamConsumer.class),
            mock(CheckpointManager.class), mock(HaMetrics.class));
    }

    private static ChangeEvent makeControlEvent(ChangeEventType type, String targetNodeId,
                                                 long snapshotTs) {
        EntityData entity = new EntityData(
            EntityType.NODE,
            targetNodeId,
            List.of(),
            Map.of("snapshotTs", snapshotTs),
            null, null, null, null
        );
        return new ChangeEvent(
            "evt-" + targetNodeId,
            type,
            "neo4j",
            System.currentTimeMillis(),
            1L,
            "tx-1",
            entity,
            null
        );
    }
}
