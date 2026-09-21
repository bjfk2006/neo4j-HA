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

    /**
     * REVIEW-S2: every mutator here used to be a non-atomic
     * {@code get() → withXxx() → put()}. {@code nodes} is a ConcurrentHashMap,
     * but that only makes each individual operation safe — the read-modify-write
     * as a whole was not, and four different threads run these concurrently:
     * {@code health-checker} (updateHealth), {@code ha-maintenance}
     * (setServiceState / updateSyncLag), {@code ha-failover} (updateRole /
     * setPrimary / setServiceState) and Javalin HTTP threads.
     *
     * <p>The dangerous interleaving is a lost update on {@code role}: failover
     * Phase 7 calls {@code updateRole(newPrimary, PRIMARY)} while the 5 s
     * service-state evaluator concurrently calls
     * {@code setServiceState(newPrimary, ONLINE)} from a snapshot taken before
     * the role change. Whichever writes last wins with its stale copy of the
     * other field — if that is the service-state writer, the freshly promoted
     * primary reverts to {@code STANDBY}, which puts it into
     * {@code getStandbyDrivers()} and makes SyncApplier replay CDC events onto
     * the primary itself.</p>
     *
     * <p>{@code compute} performs the whole read-modify-write atomically under
     * the map's per-bin lock, so concurrent field updates now compose instead of
     * clobbering each other.</p>
     */
    public void updateRole(String nodeId, NodeRole role) {
        nodes.computeIfPresent(nodeId, (k, info) -> info.withRole(role));
    }

    public void updateHealth(String nodeId, NodeHealth health) {
        nodes.computeIfPresent(nodeId, (k, info) -> info.withHealth(health));
    }

    public void setServiceState(String nodeId, NodeServiceState state) {
        NodeInfo updated = nodes.computeIfPresent(nodeId,
            (k, info) -> info.withServiceState(state));
        if (updated != null) {
            log.info("Node {} service state changed to {}", nodeId, state);
        }
    }

    public void setPendingCleanup(String nodeId, boolean pendingCleanup) {
        nodes.computeIfPresent(nodeId, (k, info) -> info.withPendingCleanup(pendingCleanup));
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
        nodes.computeIfPresent(nodeId,
            (k, info) -> info.syncLagMs() == syncLagMs ? info : info.withSyncLagMs(syncLagMs));
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
        return serverIdFromBoltUri(n.boltUri());
    }

    /**
     * Extract the host portion of a Bolt URI — the name HAProxy knows the
     * server by.
     *
     * <p>REVIEW-F4: this logic was copy-pasted in three places
     * ({@code HaAgent.evaluateServiceStates}, {@code HaProxyStateSyncer.sync}
     * and here), all written as {@code uri.replace("bolt://", "").split(":")[0]}.
     * That only works for the {@code bolt://} scheme: given {@code neo4j://h:7687}
     * or {@code bolt+s://h:7687} the {@code replace} is a no-op and the result is
     * the literal string {@code "neo4j"} / {@code "bolt+s"} — a server name
     * HAProxy has never heard of, so every {@code set server} silently targets
     * nothing and routing is never actually updated. Parsing the scheme properly
     * (and handling userinfo and bracketed IPv6 hosts) removes that trap.</p>
     */
    public static String serverIdFromBoltUri(String uri) {
        if (uri == null) return null;
        String s = uri.trim();
        int scheme = s.indexOf("://");
        if (scheme >= 0) s = s.substring(scheme + 3);
        int at = s.lastIndexOf('@');          // strip user:pass@
        if (at >= 0) s = s.substring(at + 1);
        int slash = s.indexOf('/');           // strip any path
        if (slash >= 0) s = s.substring(0, slash);
        if (s.startsWith("[")) {              // bracketed IPv6 literal
            int close = s.indexOf(']');
            if (close > 0) return s.substring(1, close);
        }
        int colon = s.indexOf(':');           // strip :port
        if (colon >= 0) s = s.substring(0, colon);
        return s.isEmpty() ? null : s;
    }
}
