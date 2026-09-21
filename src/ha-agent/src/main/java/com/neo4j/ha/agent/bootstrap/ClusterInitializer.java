package com.neo4j.ha.agent.bootstrap;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.recovery.ApocTriggerUninstaller;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.model.*;
import com.neo4j.ha.common.neo4j.Neo4jClientFactory;
import com.neo4j.ha.common.neo4j.Neo4jHealthChecker;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterInitializer {

    private static final Logger log = LoggerFactory.getLogger(ClusterInitializer.class);

    private final HaConfig config;
    private final Neo4jClientFactory neo4jFactory;
    private final ClusterStateManager clusterState;
    private final NodeRegistry nodeRegistry;
    private final Neo4jHealthChecker healthChecker;

    public ClusterInitializer(HaConfig config, Neo4jClientFactory neo4jFactory,
                               ClusterStateManager clusterState, NodeRegistry nodeRegistry) {
        this.config = config;
        this.neo4jFactory = neo4jFactory;
        this.clusterState = clusterState;
        this.nodeRegistry = nodeRegistry;
        this.healthChecker = new Neo4jHealthChecker();
    }

    public void init() {
        log.info("Initializing cluster with {} nodes", config.cluster().nodes().size());

        // Try to restore state from Redis
        Map<String, NodeInfo> savedState = nodeRegistry.loadAll();
        Map<String, Boolean> reachableById = new HashMap<>();

        for (HaConfig.NodeConfig nodeConfig : config.cluster().nodes()) {
            // Create Neo4j driver
            Driver driver = neo4jFactory.getOrCreateDriver(
                nodeConfig.id(),
                nodeConfig.neo4j().uri(),
                nodeConfig.neo4j().username(),
                nodeConfig.neo4j().password()
            );

            // Check connectivity
            boolean reachable = healthChecker.checkBolt(driver);

            // Determine role
            NodeRole role;
            if (savedState.containsKey(nodeConfig.id())) {
                role = savedState.get(nodeConfig.id()).role();
                log.info("Restored node {} role from registry: {}", nodeConfig.id(), role);
            } else {
                role = NodeRole.valueOf(nodeConfig.role().toUpperCase());
            }

            // REVIEW-S1: a node persisted as DOWN (written by a previous
            // failover) is neither PRIMARY nor STANDBY, so it would fall out of
            // getStandbyNodes() AND not be the primary — an orphan that is never
            // synced, never selectable, and never recovered. On restart a node
            // that is not the primary is, by definition, a standby candidate.
            if (role == NodeRole.DOWN) {
                log.info("Node {} was persisted as DOWN; normalising to STANDBY on restart "
                    + "(a DOWN node belongs to no collection and would be orphaned)",
                    nodeConfig.id());
                role = NodeRole.STANDBY;
            }

            NodeServiceState serviceState = reachable ? NodeServiceState.SYNCING : NodeServiceState.OFFLINE;
            NodeHealth health = reachable ? NodeHealth.HEALTHY : NodeHealth.DOWN;

            NodeInfo nodeInfo = new NodeInfo(
                nodeConfig.id(), role, nodeConfig.neo4j().uri(),
                health, serviceState, 0, System.currentTimeMillis(), false
            );

            clusterState.addNode(nodeConfig.id(), nodeInfo, driver);
            nodeRegistry.register(nodeInfo);
            reachableById.put(nodeConfig.id(), reachable);

            log.info("Node {} initialized: role={}, health={}, reachable={}",
                nodeConfig.id(), role, health, reachable);
        }

        reconcilePrimary(reachableById);

        log.info("Cluster initialized. Primary: {}", clusterState.getPrimaryNodeId());
    }

    /**
     * REVIEW-S1: enforce "exactly one PRIMARY" before anything else starts.
     *
     * <p>Roles are restored from the Redis registry, falling back to the static
     * roles in the YAML when the registry has no entry. Neither source was
     * validated, and both can disagree with reality:</p>
     *
     * <ul>
     *   <li><b>Registry lost or expired</b> (Redis flushed, key TTL'd, fresh
     *       Redis): every node falls back to its YAML role, so the node that was
     *       failed over <i>away from</i> is treated as primary again. The agent
     *       then installs CDC triggers on it and polls it, while HAProxy is
     *       routing client writes to the node that actually won the failover.</li>
     *   <li><b>Two PRIMARYs</b>: a failover that died between
     *       {@code updateRole(new, PRIMARY)} and {@code updateRole(old, DOWN)}
     *       persists two primaries. {@code addNode} just lets the last one in
     *       iteration order win, and the other becomes the orphan described
     *       above.</li>
     *   <li><b>Zero PRIMARYs</b>: leaves {@code primaryNodeId} null, and
     *       {@code HaAgent} then hands a null Driver to the trigger installer
     *       and index installer.</li>
     * </ul>
     *
     * <p>Resolution order: prefer a registry-declared primary that is reachable;
     * otherwise any reachable node; otherwise the first configured node (so the
     * agent still boots and reports the problem rather than NPE-ing). Every node
     * that is not the winner is forced to STANDBY.</p>
     */
    private void reconcilePrimary(Map<String, Boolean> reachableById) {
        List<NodeInfo> all = clusterState.getAllNodes();
        List<NodeInfo> primaries = all.stream()
            .filter(n -> n.role() == NodeRole.PRIMARY)
            .toList();

        if (primaries.size() == 1 && clusterState.getPrimaryNodeId() != null) {
            return; // healthy, unambiguous
        }

        String chosen = primaries.stream()
            .map(NodeInfo::id)
            .filter(id -> Boolean.TRUE.equals(reachableById.get(id)))
            .findFirst()
            .orElseGet(() -> all.stream()
                .map(NodeInfo::id)
                .filter(id -> Boolean.TRUE.equals(reachableById.get(id)))
                .findFirst()
                .orElseGet(() -> all.isEmpty() ? null : all.get(0).id()));

        if (chosen == null) {
            log.error("REVIEW-S1: no nodes configured; cannot establish a primary");
            return;
        }

        log.error("REVIEW-S1: inconsistent persisted topology — found {} node(s) marked PRIMARY "
            + "({}). Electing {} as primary and forcing every other node to STANDBY. "
            + "VERIFY against HAProxy's actual write backend before trusting this cluster: "
            + "if HAProxy is routing writes elsewhere, run a switchover to the correct node.",
            primaries.size(), primaries.stream().map(NodeInfo::id).toList(), chosen);

        for (NodeInfo n : all) {
            NodeRole want = n.id().equals(chosen) ? NodeRole.PRIMARY : NodeRole.STANDBY;
            if (n.role() != want) {
                clusterState.updateRole(n.id(), want);
                nodeRegistry.updateRole(n.id(), want);
            }
        }
        clusterState.setPrimary(chosen);

        // A node that was persisted as PRIMARY but lost the election still has
        // the CDC triggers armed from its previous life. Leaving them in place
        // is the BUG-046 hazard: every SyncApplier MERGE onto this node would
        // re-stamp `_updated_at` with local wall-clock time, corrupting the CDC
        // keyset cursor if it is ever promoted again. Demote it properly.
        for (NodeInfo n : primaries) {
            if (n.id().equals(chosen)) continue;
            demoteLoser(n.id());
        }
    }

    private void demoteLoser(String nodeId) {
        Driver driver = clusterState.getDriver(nodeId);
        if (driver == null) return;
        String db = databaseFor(nodeId);
        try {
            boolean dropped = ApocTriggerUninstaller.uninstall(driver, db);
            if (dropped) {
                log.warn("REVIEW-S1: uninstalled CDC triggers from {} — it was persisted as "
                    + "PRIMARY but lost the election; leaving them armed would re-stamp "
                    + "_updated_at on replayed writes (BUG-046)", nodeId);
            } else {
                nodeRegistry.markPendingCleanup(nodeId, true);
                log.error("REVIEW-S1: could NOT uninstall CDC triggers from demoted {}; marked "
                    + "for deferred cleanup. Until that succeeds this node must not be "
                    + "promoted.", nodeId);
            }
        } catch (Exception e) {
            nodeRegistry.markPendingCleanup(nodeId, true);
            log.error("REVIEW-S1: trigger uninstall on demoted {} failed ({}); marked for "
                + "deferred cleanup", nodeId, e.toString());
        }
    }

    private String databaseFor(String nodeId) {
        for (HaConfig.NodeConfig nc : config.cluster().nodes()) {
            if (nc.id().equals(nodeId)) return nc.neo4j().database();
        }
        return "neo4j";
    }
}
