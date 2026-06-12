package com.neo4j.ha.agent.health;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
import com.neo4j.ha.common.neo4j.Neo4jHealthChecker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for HealthChecker state transitions.
 * We invoke the private checkNode method via reflection to test the state machine
 * without starting the scheduler.
 */
class HealthCheckerTest {

    private ClusterStateManager clusterState;
    private Neo4jHealthChecker neo4jHealthChecker;
    private Driver driver;

    private HaMetrics metrics;
    private HealthChecker healthChecker;

    private static final String NODE_ID = "node-1";
    private static final String BOLT_URI = "bolt://localhost:7687";

    private NodeInfo testNode() {
        return new NodeInfo(NODE_ID, NodeRole.PRIMARY, BOLT_URI,
                NodeHealth.HEALTHY, NodeServiceState.ONLINE,
                0L, System.currentTimeMillis(), false);
    }

    @BeforeEach
    void setUp() {
        clusterState = mock(ClusterStateManager.class);
        neo4jHealthChecker = mock(Neo4jHealthChecker.class);
        driver = mock(Driver.class);

        // Use a real HaMetrics backed by a simple in-memory registry
        // so that counter fields (public final) are properly initialized.
        metrics = new HaMetrics(new SimpleMeterRegistry());

        HaConfig.HealthCheckConfig healthCheckConfig =
                new HaConfig.HealthCheckConfig("1000", "2000", 3, 2);
        HaConfig.FencingTokenConfig fencingTokenConfig =
                new HaConfig.FencingTokenConfig("test-key");
        HaConfig.FailoverConfig failoverConfig = new HaConfig.FailoverConfig(
                healthCheckConfig, fencingTokenConfig,
                "5s", "3s", "60s", 5
        );

        healthChecker = new HealthChecker(clusterState, neo4jHealthChecker, metrics, failoverConfig);

        // Initialize node state via start-like setup: put node into known maps
        NodeInfo node = testNode();
        when(clusterState.getAllNodes()).thenReturn(List.of(node));
        when(clusterState.getDriver(NODE_ID)).thenReturn(driver);

        // Call start() to initialize internal maps, then stop the scheduler immediately
        healthChecker.start();
        healthChecker.stop();
    }

    /**
     * Invokes the private checkNode(NodeInfo) method via reflection.
     */
    private void invokeCheckNode() throws Exception {
        NodeInfo node = testNode();
        Method method = HealthChecker.class.getDeclaredMethod("checkNode", NodeInfo.class);
        method.setAccessible(true);
        method.invoke(healthChecker, node);
    }

    /**
     * Stubs the health checker to return a failing health check (all levels fail).
     */
    private void stubUnhealthy() {
        lenient().when(neo4jHealthChecker.checkTcp(anyString(), anyInt(), anyInt())).thenReturn(false);
        lenient().when(neo4jHealthChecker.checkBolt(any(Driver.class))).thenReturn(false);
        lenient().when(neo4jHealthChecker.checkCypher(any(Driver.class), anyString())).thenReturn(false);
    }

    /**
     * Stubs the health checker to return a passing health check (all levels pass).
     */
    private void stubHealthy() {
        lenient().when(neo4jHealthChecker.checkTcp(anyString(), anyInt(), anyInt())).thenReturn(true);
        lenient().when(neo4jHealthChecker.checkBolt(any(Driver.class))).thenReturn(true);
        lenient().when(neo4jHealthChecker.checkCypher(any(Driver.class), anyString())).thenReturn(true);
    }

    // ---------------------------------------------------------------
    // HEALTHY -> SUSPECT after 1 fail
    // ---------------------------------------------------------------

    @Test
    void transitionsFromHealthyToSuspectAfterOneFail() throws Exception {
        stubUnhealthy();

        assertEquals(HealthState.HEALTHY, healthChecker.getState(NODE_ID));

        invokeCheckNode();

        assertEquals(HealthState.SUSPECT, healthChecker.getState(NODE_ID));
        verify(clusterState).updateHealth(NODE_ID, NodeHealth.SUSPECT);
    }

    // ---------------------------------------------------------------
    // SUSPECT -> DOWN after threshold fails (failThreshold = 3)
    // ---------------------------------------------------------------

    @Test
    void transitionsFromSuspectToDownAfterThresholdFails() throws Exception {
        stubUnhealthy();

        // Fail 3 times (failThreshold = 3): 1st -> SUSPECT, 2nd -> still SUSPECT, 3rd -> DOWN
        invokeCheckNode();
        assertEquals(HealthState.SUSPECT, healthChecker.getState(NODE_ID));

        invokeCheckNode();
        // Still SUSPECT (fail count = 2, threshold = 3)
        assertEquals(HealthState.SUSPECT, healthChecker.getState(NODE_ID));

        invokeCheckNode();
        // Now DOWN (fail count = 3 >= threshold)
        assertEquals(HealthState.DOWN, healthChecker.getState(NODE_ID));
        verify(clusterState).updateHealth(NODE_ID, NodeHealth.DOWN);
    }

    // ---------------------------------------------------------------
    // DOWN -> HEALTHY after threshold successes (successThreshold = 2)
    // ---------------------------------------------------------------

    @Test
    void transitionsFromDownToHealthyAfterThresholdSuccesses() throws Exception {
        // First, drive the node to DOWN state
        stubUnhealthy();
        for (int i = 0; i < 3; i++) {
            invokeCheckNode();
        }
        assertEquals(HealthState.DOWN, healthChecker.getState(NODE_ID));

        // Now switch to healthy checks
        stubHealthy();

        // 1st success: not yet recovered (successThreshold = 2)
        invokeCheckNode();
        assertEquals(HealthState.DOWN, healthChecker.getState(NODE_ID));

        // 2nd success: should recover
        invokeCheckNode();
        assertEquals(HealthState.HEALTHY, healthChecker.getState(NODE_ID));
        verify(clusterState, atLeastOnce()).updateHealth(NODE_ID, NodeHealth.HEALTHY);
    }

    // ---------------------------------------------------------------
    // BK1: suppress() skips probe; never triggers onNodeDown
    // ---------------------------------------------------------------

    @Test
    void suppress_skipsProbeAndDoesNotFireOnNodeDown() throws Exception {
        var listener = mock(HealthChecker.HealthChangeListener.class);
        healthChecker.setListener(listener);

        // Suppress the node, then stub a fully unhealthy environment.
        healthChecker.suppress(NODE_ID);
        assertTrue(healthChecker.isSuppressed(NODE_ID));
        stubUnhealthy();

        // Drive 5 probe iterations — each MUST be a no-op while suppressed.
        for (int i = 0; i < 5; i++) {
            invokeCheckNode();
        }

        // Listener never invoked; clusterState.updateHealth(DOWN) never called.
        verify(listener, never()).onNodeDown(anyString());
        verify(clusterState, never()).updateHealth(eq(NODE_ID), eq(NodeHealth.DOWN));

        // State should still be HEALTHY (the value seeded in start()).
        assertEquals(HealthState.HEALTHY, healthChecker.getState(NODE_ID));
    }

    @Test
    void unsuppress_resumesNormalProbes() throws Exception {
        var listener = mock(HealthChecker.HealthChangeListener.class);
        healthChecker.setListener(listener);

        healthChecker.suppress(NODE_ID);
        stubUnhealthy();
        for (int i = 0; i < 5; i++) invokeCheckNode();
        verify(listener, never()).onNodeDown(anyString());

        // After unsuppress, normal probe should resume and eventually mark DOWN.
        healthChecker.unsuppress(NODE_ID);
        assertFalse(healthChecker.isSuppressed(NODE_ID));
        for (int i = 0; i < 3; i++) invokeCheckNode();   // failThreshold = 3
        assertEquals(HealthState.DOWN, healthChecker.getState(NODE_ID));
    }
}
