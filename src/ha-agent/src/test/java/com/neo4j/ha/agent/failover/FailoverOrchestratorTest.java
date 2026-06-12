package com.neo4j.ha.agent.failover;

import com.neo4j.ha.agent.audit.FailoverAuditLog;
import com.neo4j.ha.agent.bootstrap.ApocTriggerInstaller;
import com.neo4j.ha.agent.bootstrap.IndexInstaller;
import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.agent.routing.HaProxyUpdater;
import com.neo4j.ha.cdc.CdcCollector;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.sync.SyncApplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FailoverOrchestratorTest {

    private HealthChecker healthChecker;
    private FencingTokenManager fencingTokenManager;
    private CdcCollector cdcCollector;
    private SyncApplier syncApplier;
    private StandbySelector standbySelector;
    private ApocTriggerInstaller triggerInstaller;
    private IndexInstaller indexInstaller;
    private HaProxyUpdater haProxyUpdater;
    private NodeRegistry nodeRegistry;
    private ClusterStateManager clusterState;
    private FailoverAuditLog audit;
    private HaMetrics metrics;
    private CheckpointManager checkpointManager;
    private Driver newPrimaryDriver;

    @BeforeEach
    void initMocks() {
        healthChecker = mock(HealthChecker.class);
        fencingTokenManager = mock(FencingTokenManager.class);
        cdcCollector = mock(CdcCollector.class);
        syncApplier = mock(SyncApplier.class);
        standbySelector = mock(StandbySelector.class);
        triggerInstaller = mock(ApocTriggerInstaller.class);
        indexInstaller = mock(IndexInstaller.class);
        haProxyUpdater = mock(HaProxyUpdater.class);
        nodeRegistry = mock(NodeRegistry.class);
        clusterState = mock(ClusterStateManager.class);
        audit = mock(FailoverAuditLog.class);
        metrics = mock(HaMetrics.class);
        checkpointManager = mock(CheckpointManager.class);
        newPrimaryDriver = mock(Driver.class);
    }

    private static final String FAILED_NODE = "node-primary";
    private static final String NEW_PRIMARY = "node-standby-1";

    /**
     * Creates an orchestrator with the given rate-limit parameters.
     * confirmationWaitMs is set to 0 so tests run fast.
     */
    private FailoverOrchestrator createOrchestrator(long minIntervalMs, int maxAutoPerHour) {
        return new FailoverOrchestrator(
                healthChecker, fencingTokenManager, cdcCollector, syncApplier,
                standbySelector, triggerInstaller, indexInstaller, haProxyUpdater,
                /* haProxyStateSyncer */ null,
                nodeRegistry, clusterState, audit, metrics, checkpointManager,
                "neo4j",
                /* confirmationWaitMs */ 0,
                minIntervalMs,
                maxAutoPerHour
        );
    }

    /**
     * Stubs all mocks so that a full failover can succeed end-to-end.
     */
    private void stubSuccessfulFailover() {
        when(healthChecker.isHealthy(FAILED_NODE)).thenReturn(false);
        when(fencingTokenManager.increment()).thenReturn(42L);
        when(standbySelector.selectBest()).thenReturn(NEW_PRIMARY);
        when(clusterState.getDriver(NEW_PRIMARY)).thenReturn(newPrimaryDriver);
    }

    // ---------------------------------------------------------------
    // Rate limiting: blocks failover if too recent
    // ---------------------------------------------------------------

    @Test
    void blocksFailoverIfMinIntervalNotMet() {
        // Use a very large minInterval so the second call is always "too recent"
        FailoverOrchestrator orchestrator = createOrchestrator(
                /* minIntervalMs */ 600_000,
                /* maxAutoPerHour */ 10
        );

        stubSuccessfulFailover();

        // First failover succeeds
        orchestrator.executeFailover(FAILED_NODE);
        verify(audit).logComplete(eq(FAILED_NODE), eq(NEW_PRIMARY), anyLong());

        // Second failover should be blocked by rate limit
        orchestrator.executeFailover(FAILED_NODE);
        verify(audit).logCancel(eq(FAILED_NODE), contains("rate limit"));
    }

    // ---------------------------------------------------------------
    // Rate limiting: blocks if max per hour exceeded
    // ---------------------------------------------------------------

    @Test
    void blocksFailoverIfMaxPerHourExceeded() {
        int maxPerHour = 2;
        FailoverOrchestrator orchestrator = createOrchestrator(
                /* minIntervalMs */ 0,
                maxPerHour
        );

        stubSuccessfulFailover();

        // Execute maxPerHour failovers -- all should succeed
        for (int i = 0; i < maxPerHour; i++) {
            orchestrator.executeFailover(FAILED_NODE);
        }
        verify(audit, times(maxPerHour)).logComplete(eq(FAILED_NODE), eq(NEW_PRIMARY), anyLong());

        // Next failover should be blocked
        orchestrator.executeFailover(FAILED_NODE);
        verify(audit).logCancel(eq(FAILED_NODE), contains("rate limit"));
    }

    // ---------------------------------------------------------------
    // Rate limiting: allows after hour window reset
    // ---------------------------------------------------------------

    @Test
    void allowsFailoverAfterHourWindowResets() throws Exception {
        int maxPerHour = 1;
        // Use a custom subclass that lets us control the hour-window start time.
        // Since hourWindowStart is initialized with System.currentTimeMillis() and
        // checkSafeToFailover resets when now - hourWindowStart > 3_600_000,
        // we use reflection to shift the window start back.
        FailoverOrchestrator orchestrator = createOrchestrator(
                /* minIntervalMs */ 0,
                maxPerHour
        );

        stubSuccessfulFailover();

        // First failover succeeds, consuming the hourly quota
        orchestrator.executeFailover(FAILED_NODE);
        verify(audit, times(1)).logComplete(eq(FAILED_NODE), eq(NEW_PRIMARY), anyLong());

        // Shift hourWindowStart and lastFailoverTime back by more than 1 hour via reflection
        var hourWindowField = FailoverOrchestrator.class.getDeclaredField("hourWindowStart");
        hourWindowField.setAccessible(true);
        var hourWindowStart = (java.util.concurrent.atomic.AtomicLong) hourWindowField.get(orchestrator);
        hourWindowStart.set(System.currentTimeMillis() - 3_700_000);

        var lastFailoverField = FailoverOrchestrator.class.getDeclaredField("lastFailoverTime");
        lastFailoverField.setAccessible(true);
        var lastFailoverTime = (java.util.concurrent.atomic.AtomicLong) lastFailoverField.get(orchestrator);
        lastFailoverTime.set(System.currentTimeMillis() - 3_700_000);

        // Now the hour window should reset and the failover should be allowed
        orchestrator.executeFailover(FAILED_NODE);
        verify(audit, times(2)).logComplete(eq(FAILED_NODE), eq(NEW_PRIMARY), anyLong());
    }

    // ---------------------------------------------------------------
    // Cancels failover if node recovers during confirmation wait
    // ---------------------------------------------------------------

    @Test
    void cancelsFailoverIfNodeRecoversDuringConfirmationWait() {
        FailoverOrchestrator orchestrator = new FailoverOrchestrator(
                healthChecker, fencingTokenManager, cdcCollector, syncApplier,
                standbySelector, triggerInstaller, indexInstaller, haProxyUpdater,
                /* haProxyStateSyncer */ null,
                nodeRegistry, clusterState, audit, metrics, checkpointManager,
                "neo4j",
                /* confirmationWaitMs */ 10,  // short wait
                /* minIntervalMs */ 0,
                /* maxAutoPerHour */ 10
        );

        // Node is healthy after the confirmation wait
        when(healthChecker.isHealthy(FAILED_NODE)).thenReturn(true);

        orchestrator.executeFailover(FAILED_NODE);

        // Should be cancelled, not completed
        verify(audit).logCancel(eq(FAILED_NODE), contains("recovered"));
        verify(standbySelector, never()).selectBest();
        verify(haProxyUpdater, never()).switchPrimary(anyString(), anyString());
    }
}
