package com.neo4j.ha.agent.failover;

import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.health.HealthState;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
import com.neo4j.ha.common.model.SyncMode;
import com.neo4j.ha.common.redis.CheckpointManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StandbySelectorTest {

    private ClusterStateManager clusterState;
    private HealthChecker healthChecker;
    private CheckpointManager checkpointManager;

    private StandbySelector selector;

    @BeforeEach
    void setUp() {
        clusterState = mock(ClusterStateManager.class);
        healthChecker = mock(HealthChecker.class);
        checkpointManager = mock(CheckpointManager.class);
        selector = new StandbySelector(clusterState, healthChecker, checkpointManager);
    }

    private NodeInfo standbyNode(String id) {
        return new NodeInfo(id, NodeRole.STANDBY, "bolt://" + id + ":7687",
                NodeHealth.HEALTHY, NodeServiceState.ONLINE, 0L, System.currentTimeMillis(), false);
    }

    // ---------------------------------------------------------------
    // Selects ONLINE + HEALTHY standby
    // ---------------------------------------------------------------

    @Test
    void selectsOnlineHealthyStandby() {
        NodeInfo node = standbyNode("standby-1");
        when(clusterState.getStandbyNodes()).thenReturn(List.of(node));
        when(healthChecker.getState("standby-1")).thenReturn(HealthState.HEALTHY);
        when(clusterState.getServiceState("standby-1")).thenReturn(NodeServiceState.ONLINE);
        when(checkpointManager.loadSyncCheckpoint("standby-1"))
                .thenReturn(Optional.of(new CheckpointManager.SyncCheckpoint(
                        "1-0", 1000L, SyncMode.INCREMENTAL, 0L, 0L)));

        String result = selector.selectBest();
        assertEquals("standby-1", result);
    }

    // ---------------------------------------------------------------
    // Prefers standby with newest checkpoint
    // ---------------------------------------------------------------

    @Test
    void prefersStandbyWithNewestCheckpoint() {
        NodeInfo nodeA = standbyNode("standby-a");
        NodeInfo nodeB = standbyNode("standby-b");

        when(clusterState.getStandbyNodes()).thenReturn(List.of(nodeA, nodeB));
        when(healthChecker.getState("standby-a")).thenReturn(HealthState.HEALTHY);
        when(healthChecker.getState("standby-b")).thenReturn(HealthState.HEALTHY);
        when(clusterState.getServiceState("standby-a")).thenReturn(NodeServiceState.ONLINE);
        when(clusterState.getServiceState("standby-b")).thenReturn(NodeServiceState.ONLINE);

        // standby-a has an older checkpoint
        when(checkpointManager.loadSyncCheckpoint("standby-a"))
                .thenReturn(Optional.of(new CheckpointManager.SyncCheckpoint(
                        "1-0", 500L, SyncMode.INCREMENTAL, 0L, 0L)));
        // standby-b has a newer checkpoint
        when(checkpointManager.loadSyncCheckpoint("standby-b"))
                .thenReturn(Optional.of(new CheckpointManager.SyncCheckpoint(
                        "2-0", 2000L, SyncMode.INCREMENTAL, 0L, 0L)));

        String result = selector.selectBest();
        assertEquals("standby-b", result, "Should select the standby with the most recent checkpoint");
    }

    // ---------------------------------------------------------------
    // Throws when no eligible standby found
    // ---------------------------------------------------------------

    @Test
    void throwsWhenNoEligibleStandbyFound() {
        // Empty standby list
        when(clusterState.getStandbyNodes()).thenReturn(List.of());

        assertThrows(StandbySelector.NoHealthyStandbyException.class, () -> selector.selectBest());
    }

    @Test
    void throwsWhenAllStandbyNodesAreDown() {
        NodeInfo node = standbyNode("standby-down");
        when(clusterState.getStandbyNodes()).thenReturn(List.of(node));
        when(healthChecker.getState("standby-down")).thenReturn(HealthState.DOWN);

        assertThrows(StandbySelector.NoHealthyStandbyException.class, () -> selector.selectBest());
    }

    @Test
    void throwsWhenAllStandbyNodesAreOffline() {
        NodeInfo node = standbyNode("standby-offline");
        when(clusterState.getStandbyNodes()).thenReturn(List.of(node));
        when(healthChecker.getState("standby-offline")).thenReturn(HealthState.HEALTHY);
        when(clusterState.getServiceState("standby-offline")).thenReturn(NodeServiceState.OFFLINE);

        assertThrows(StandbySelector.NoHealthyStandbyException.class, () -> selector.selectBest());
    }
}
