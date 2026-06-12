package com.neo4j.ha.agent.failover;

import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.health.HealthState;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
import com.neo4j.ha.common.redis.CheckpointManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

public class StandbySelector {

    private static final Logger log = LoggerFactory.getLogger(StandbySelector.class);

    private final ClusterStateManager clusterState;
    private final HealthChecker healthChecker;
    private final CheckpointManager checkpointManager;

    public StandbySelector(ClusterStateManager clusterState, HealthChecker healthChecker,
                            CheckpointManager checkpointManager) {
        this.clusterState = clusterState;
        this.healthChecker = healthChecker;
        this.checkpointManager = checkpointManager;
    }

    public String selectBest() {
        List<NodeInfo> candidates = clusterState.getStandbyNodes().stream()
            .filter(n -> healthChecker.getState(n.id()) == HealthState.HEALTHY)
            .filter(n -> clusterState.getServiceState(n.id()) == NodeServiceState.ONLINE)
            .toList();

        if (candidates.isEmpty()) {
            throw new NoHealthyStandbyException("No healthy ONLINE standby node available for failover");
        }

        return candidates.stream()
            .sorted(Comparator.comparing(
                (NodeInfo n) -> checkpointManager.loadSyncCheckpoint(n.id())
                    .map(CheckpointManager.SyncCheckpoint::lastEventTs)
                    .orElse(0L)
            ).reversed())
            .findFirst()
            .map(NodeInfo::id)
            .orElseThrow(() -> new NoHealthyStandbyException("Failed to select standby"));
    }

    public static class NoHealthyStandbyException extends RuntimeException {
        public NoHealthyStandbyException(String message) {
            super(message);
        }
    }
}
