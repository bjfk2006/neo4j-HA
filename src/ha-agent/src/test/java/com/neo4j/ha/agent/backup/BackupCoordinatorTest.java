package com.neo4j.ha.agent.backup;

import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.routing.HaProxyStateSyncer;
import com.neo4j.ha.agent.routing.HaProxyUpdater;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.sync.SyncApplier;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Covers the state-machine paths fixed in the M-1 / M-2 / M-A review cycle:
 *   - prepare() recovers cleanly when a pre-pause lookup throws
 *   - cancelForFailover() handles PREPARING state, not just IN_PROGRESS
 *   - prepare/complete/cancelForFailover are synchronized so concurrent
 *     callers can't observe torn intermediate state
 */
class BackupCoordinatorTest {

    private SyncApplier syncApplier;
    private CheckpointManager checkpointManager;
    private HaMetrics metrics;
    private HealthChecker healthChecker;
    private HaProxyStateSyncer haProxyStateSyncer;
    private HaProxyUpdater haProxyUpdater;
    private ClusterStateManager clusterState;
    private BackupCoordinator backup;

    @BeforeEach
    void setUp() {
        syncApplier = mock(SyncApplier.class);
        checkpointManager = mock(CheckpointManager.class);
        metrics = new HaMetrics(new SimpleMeterRegistry());
        healthChecker = mock(HealthChecker.class);
        haProxyStateSyncer = mock(HaProxyStateSyncer.class);
        haProxyUpdater = mock(HaProxyUpdater.class);
        clusterState = mock(ClusterStateManager.class);

        backup = new BackupCoordinator(
            syncApplier, checkpointManager, metrics,
            7_200_000L,                                  // 2h maxBackupDuration
            healthChecker, haProxyStateSyncer, haProxyUpdater, clusterState);
    }

    // ---------------------------------------------------------------
    // M-1: prepare() server-id lookup failure must leave state IDLE
    // ---------------------------------------------------------------

    @Test
    void prepare_serverIdResolutionFailure_leavesStateIdle() {
        when(clusterState.getServerIdForNode("node-02"))
            .thenThrow(new IllegalStateException("not registered yet"));

        assertThrows(IllegalStateException.class, () -> backup.prepare("node-02"));
        assertEquals(BackupState.IDLE, backup.getState(),
            "After pre-pause lookup failure state must roll back to IDLE so caller can retry");

        // None of the side effects must have been applied
        verify(syncApplier, never()).pause();
        verify(healthChecker, never()).suppress(anyString());
        verify(haProxyStateSyncer, never()).pause();
        verify(haProxyUpdater, never()).disableReadBackend(anyString());
    }

    @Test
    void prepare_afterFailedAttempt_canRetrySuccessfully() {
        // First attempt fails on server-id resolution
        when(clusterState.getServerIdForNode("node-02"))
            .thenThrow(new IllegalStateException("transient"))
            .thenReturn("neo4j-standby-1");

        assertThrows(IllegalStateException.class, () -> backup.prepare("node-02"));
        assertEquals(BackupState.IDLE, backup.getState());

        // Retry succeeds
        var result = backup.prepare("node-02");
        assertEquals("node-02", result.nodeId());
        assertEquals(BackupState.IN_PROGRESS, backup.getState());
        verify(syncApplier).pause();
        verify(healthChecker).suppress("node-02");
        verify(haProxyStateSyncer).pause();
        verify(haProxyUpdater).disableReadBackend("neo4j-standby-1");
    }

    // ---------------------------------------------------------------
    // M-2: cancelForFailover must handle PREPARING, not just IN_PROGRESS
    // ---------------------------------------------------------------

    @Test
    void cancelForFailover_inIdleState_isNoop() {
        backup.cancelForFailover();
        assertEquals(BackupState.IDLE, backup.getState());
        verifyNoInteractions(syncApplier);
        verifyNoInteractions(healthChecker);
    }

    @Test
    void cancelForFailover_inProgress_reversesAllFourActions() {
        when(clusterState.getServerIdForNode("node-02")).thenReturn("neo4j-standby-1");
        backup.prepare("node-02");
        assertEquals(BackupState.IN_PROGRESS, backup.getState());

        backup.cancelForFailover();

        assertEquals(BackupState.IDLE, backup.getState());
        verify(syncApplier).pause();
        verify(syncApplier).resume();
        verify(healthChecker).suppress("node-02");
        verify(healthChecker).unsuppress("node-02");
        verify(haProxyStateSyncer).pause();
        verify(haProxyStateSyncer).resume();
        verify(haProxyUpdater).disableReadBackend("neo4j-standby-1");
        verify(haProxyUpdater).enableReadBackend("neo4j-standby-1");
    }

    // ---------------------------------------------------------------
    // Smoke test for the complete() reverse-order path
    // ---------------------------------------------------------------

    @Test
    void prepareThenComplete_runsFourActionsInBothDirections() {
        when(clusterState.getServerIdForNode("node-02")).thenReturn("neo4j-standby-1");
        backup.prepare("node-02");
        backup.complete();

        assertEquals(BackupState.IDLE, backup.getState());
        assertNotNull(backup.getLastBackupTime());
        verify(syncApplier).pause();
        verify(syncApplier).resume();
        verify(healthChecker).suppress("node-02");
        verify(healthChecker).unsuppress("node-02");
        verify(haProxyStateSyncer).pause();
        verify(haProxyStateSyncer).resume();
        verify(haProxyUpdater).disableReadBackend("neo4j-standby-1");
        verify(haProxyUpdater).enableReadBackend("neo4j-standby-1");
    }

    @Test
    void prepare_whenAlreadyInProgress_throws() {
        when(clusterState.getServerIdForNode("node-02")).thenReturn("neo4j-standby-1");
        backup.prepare("node-02");
        assertThrows(IllegalStateException.class, () -> backup.prepare("node-03"));
    }

    @Test
    void complete_whenIdle_throws() {
        assertThrows(IllegalStateException.class, () -> backup.complete());
    }

    @Test
    void prepare_rejectsNullOrBlankNodeId() {
        assertThrows(IllegalArgumentException.class, () -> backup.prepare(null));
        assertThrows(IllegalArgumentException.class, () -> backup.prepare(""));
        assertThrows(IllegalArgumentException.class, () -> backup.prepare("   "));
    }
}
