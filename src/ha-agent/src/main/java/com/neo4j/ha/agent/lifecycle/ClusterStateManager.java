package com.neo4j.ha.agent.lifecycle;

import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterStateManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterStateManager.class);

    private final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final Map<String, Driver> drivers = new ConcurrentHashMap<>();
    private volatile String primaryNodeId;

    public void addNode(String nodeId, NodeInfo info, Driver driver) {
        nodes.put(nodeId, info);
        drivers.put(nodeId, driver);
        if (info.role() == NodeRole.PRIMARY) {
            primaryNodeId = nodeId;
        }
    }

    public NodeInfo getNodeInfo(String nodeId) {
        return nodes.get(nodeId);
    }

    public Driver getDriver(String nodeId) {
        return drivers.get(nodeId);
    }

    public String getPrimaryNodeId() {
        return primaryNodeId;
    }

    public Driver getPrimaryDriver() {
        return drivers.get(primaryNodeId);
    }

    public void setPrimary(String nodeId) {
        this.primaryNodeId = nodeId;
        log.info("Primary node set to: {}", nodeId);
    }

    public void updateRole(String nodeId, NodeRole role) {
        NodeInfo info = nodes.get(nodeId);
        if (info != null) {
            nodes.put(nodeId, info.withRole(role));
        }
    }

    public void updateHealth(String nodeId, NodeHealth health) {
        NodeInfo info = nodes.get(nodeId);
        if (info != null) {
            nodes.put(nodeId, info.withHealth(health));
        }
    }

    public void setServiceState(String nodeId, NodeServiceState state) {
        NodeInfo info = nodes.get(nodeId);
        if (info != null) {
            nodes.put(nodeId, info.withServiceState(state));
            log.info("Node {} service state changed to {}", nodeId, state);
        }
    }

    public void setPendingCleanup(String nodeId, boolean pendingCleanup) {
        NodeInfo info = nodes.get(nodeId);
        if (info != null) {
            nodes.put(nodeId, info.withPendingCleanup(pendingCleanup));
        }
    }

    /**
     * BUG-076: keep {@code NodeInfo.syncLagMs} in sync with the per-node lag
     * value computed by {@link com.neo4j.ha.agent.HaAgent#evaluateServiceStates}.
     * Must be called on every evaluation tick (including when lag is 0) so
     * that {@code /cluster/status} reflects the current replication state
     * rather than a stale bootstrap value. No log.info here because this
     * runs every 5s on every standby — would be log spam.
     */
    public void updateSyncLag(String nodeId, long syncLagMs) {
        NodeInfo info = nodes.get(nodeId);
        if (info != null && info.syncLagMs() != syncLagMs) {
            nodes.put(nodeId, info.withSyncLagMs(syncLagMs));
        }
    }

    public NodeServiceState getServiceState(String nodeId) {
        NodeInfo info = nodes.get(nodeId);
        return info != null ? info.serviceState() : NodeServiceState.OFFLINE;
    }

    public List<NodeInfo> getStandbyNodes() {
        return nodes.values().stream()
            .filter(n -> n.role() == NodeRole.STANDBY)
            .toList();
    }

    public List<NodeInfo> getAllNodes() {
        return List.copyOf(nodes.values());
    }

    public Map<String, Driver> getStandbyDrivers() {
        Map<String, Driver> result = new ConcurrentHashMap<>();
        for (var entry : nodes.entrySet()) {
            if (entry.getValue().role() == NodeRole.STANDBY) {
                Driver driver = drivers.get(entry.getKey());
                if (driver != null) {
                    result.put(entry.getKey(), driver);
                }
            }
        }
        return result;
    }

    public String getPrimaryServerId() {
        return getServerIdForNode(primaryNodeId);
    }

    /**
     * Translate a logical node ID (e.g. "node-01") into the HAProxy server name derived from
     * its bolt URI host (e.g. "neo4j-primary"). Returns null when the node is unknown.
     * Used by failover/switchover paths that need to drive HAProxy admin socket commands —
     * HAProxy only knows server names, not our internal node IDs.
     */
    public String getServerIdForNode(String nodeId) {
        if (nodeId == null) return null;
        NodeInfo n = nodes.get(nodeId);
        if (n == null) return null;
        String uri = n.boltUri();
        if (uri == null) return null;
        return uri.replace("bolt://", "").split(":")[0];
    }
}
