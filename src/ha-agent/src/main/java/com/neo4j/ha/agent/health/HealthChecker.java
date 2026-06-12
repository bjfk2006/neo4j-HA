package com.neo4j.ha.agent.health;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.neo4j.Neo4jHealthChecker;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.*;

public class HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private final ClusterStateManager clusterState;
    private final Neo4jHealthChecker healthChecker;
    private final HaMetrics metrics;
    private final long intervalMs;
    private final int timeoutMs;
    private final int failThreshold;
    private final int successThreshold;

    private final Map<String, HealthState> nodeStates = new ConcurrentHashMap<>();
    private final Map<String, Integer> l12FailCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> l3FailCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> l4FailCounts = new ConcurrentHashMap<>();
    private final Map<String, Integer> successCounts = new ConcurrentHashMap<>();

    /**
     * Set of nodeIds whose health checks are temporarily suppressed (e.g. during
     * backup window where the container is intentionally stopped). Suppressed
     * nodes skip the probe entirely and never trigger onNodeDown/onNodeRecovered.
     */
    private final java.util.Set<String> suppressed =
        java.util.concurrent.ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService scheduler;

    private volatile HealthChangeListener listener;

    public interface HealthChangeListener {
        void onNodeDown(String nodeId);
        void onNodeRecovered(String nodeId);
    }

    public HealthChecker(ClusterStateManager clusterState, Neo4jHealthChecker healthChecker,
                          HaMetrics metrics, HaConfig.FailoverConfig failoverConfig) {
        this.clusterState = clusterState;
        this.healthChecker = healthChecker;
        this.metrics = metrics;
        this.intervalMs = failoverConfig.healthCheck().intervalMs();
        this.timeoutMs = (int) failoverConfig.healthCheck().timeoutMs();
        this.failThreshold = failoverConfig.healthCheck().failThreshold();
        this.successThreshold = failoverConfig.healthCheck().successThreshold();
    }

    public void setListener(HealthChangeListener listener) {
        this.listener = listener;
    }

    public void start() {
        // Initialize all nodes as HEALTHY
        for (NodeInfo node : clusterState.getAllNodes()) {
            nodeStates.put(node.id(), HealthState.HEALTHY);
            l12FailCounts.put(node.id(), 0);
            l3FailCounts.put(node.id(), 0);
            l4FailCounts.put(node.id(), 0);
            successCounts.put(node.id(), 0);
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-checker");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::checkAllNodes, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("Health checker started with interval={}ms, failThreshold={}", intervalMs, failThreshold);
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private void checkAllNodes() {
        for (NodeInfo node : clusterState.getAllNodes()) {
            checkNode(node);
        }
    }

    private void checkNode(NodeInfo node) {
        // Backup window: skip probe entirely so docker-stopped containers
        // don't trigger onNodeDown / cluster role changes.
        if (suppressed.contains(node.id())) return;
        Driver driver = clusterState.getDriver(node.id());
        if (driver == null) return;

        String database = "neo4j"; // default
        boolean l1 = true, l2 = false, l3 = false, l4 = true;

        try {
            // L1: TCP
            URI uri = URI.create(node.boltUri().replace("bolt://", "http://"));
            l1 = healthChecker.checkTcp(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 7687, timeoutMs);

            if (l1) {
                // L2: Bolt
                l2 = healthChecker.checkBolt(driver);

                if (l2) {
                    // L3: Cypher
                    l3 = healthChecker.checkCypher(driver, database);
                    // L4: Write check (primary only)
                    if (l3 && node.role() == NodeRole.PRIMARY) {
                        l4 = healthChecker.checkWrite(driver, database);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Health check error for node {}: {}", node.id(), e.getMessage());
        }

        HealthState currentState = nodeStates.getOrDefault(node.id(), HealthState.HEALTHY);

        if (l1 && l2 && l3 && l4) {
            successCounts.merge(node.id(), 1, Integer::sum);
            l12FailCounts.put(node.id(), 0);
            l3FailCounts.put(node.id(), 0);
            l4FailCounts.put(node.id(), 0);

            if (currentState == HealthState.DOWN && successCounts.get(node.id()) >= successThreshold) {
                nodeStates.put(node.id(), HealthState.HEALTHY);
                clusterState.updateHealth(node.id(), NodeHealth.HEALTHY);
                log.info("Node {} recovered (was DOWN)", node.id());
                if (listener != null) listener.onNodeRecovered(node.id());
            } else if (currentState != HealthState.HEALTHY && currentState != HealthState.DOWN) {
                nodeStates.put(node.id(), HealthState.HEALTHY);
                clusterState.updateHealth(node.id(), NodeHealth.HEALTHY);
            }
        } else {
            successCounts.put(node.id(), 0);
            metrics.healthCheckFailures.increment();

            // L1/L2 failures move to SUSPECT after threshold.
            if (!l1 || !l2) {
                int l12Fails = l12FailCounts.merge(node.id(), 1, Integer::sum);
                l3FailCounts.put(node.id(), 0);
                l4FailCounts.put(node.id(), 0);
                if (l12Fails >= failThreshold && currentState == HealthState.HEALTHY) {
                    nodeStates.put(node.id(), HealthState.SUSPECT);
                    clusterState.updateHealth(node.id(), NodeHealth.SUSPECT);
                    log.warn("Node {} is SUSPECT (L1/L2 failed {} times)", node.id(), l12Fails);
                }
                // BUG-068: sustained L1/L2 failure = process crash, OOM-kill, power loss, or
                // hard network partition. Before this escalation, the node stayed in SUSPECT
                // forever (L3/L4 are gated on L2 and thus never evaluated), so `onNodeDown`
                // — the sole Failover trigger — never fired for primary crashes. Escalate to
                // DOWN after 2x failThreshold consecutive L1/L2 failures (~ 2 * intervalMs *
                // failThreshold, default ~12 s): long enough to reject transient blips, short
                // enough to beat typical application retry budgets.
                //
                // BUG-075 (2026-04-17): the original BUG-068 fix limited the escalation to
                // `role == PRIMARY` out of caution, fearing standby L1/L2 failures would
                // trigger spurious Failover. But `HaAgent.onNodeDown` already gates Failover
                // on `nodeId == primaryNodeId`, so a standby going DOWN here does NOT trigger
                // Failover — it merely flows up to `onNodeRecovered` when the standby comes
                // back, which is the ONLY trigger for `SyncApplier.schedulePendingRecovery`
                // (BUG-074's PEL replay). Without this standby-side escalation, a killed
                // standby stayed in SUSPECT forever → when it came back, the transition was
                // SUSPECT→HEALTHY not DOWN→HEALTHY → `onNodeRecovered` never fired → PEL
                // backlog from the outage was never drained → BUG-074 reproduced even though
                // the sync-applier fix was in place. Removing the role guard here makes the
                // health state machine symmetric; role-specific action stays downstream in
                // HaAgent's listener.
                if (l12Fails >= failThreshold * 2 && currentState != HealthState.DOWN) {
                    nodeStates.put(node.id(), HealthState.DOWN);
                    clusterState.updateHealth(node.id(), NodeHealth.DOWN);
                    log.error("{} {} is DOWN (L1/L2 failed {} times; TCP/Bolt unreachable)",
                              node.role() == NodeRole.PRIMARY ? "Primary" : "Standby",
                              node.id(), l12Fails);
                    if (listener != null) listener.onNodeDown(node.id());
                }
                return;
            }

            // L3 failures move to UNHEALTHY after 2 consecutive failures.
            l12FailCounts.put(node.id(), 0);
            if (!l3) {
                int l3Fails = l3FailCounts.merge(node.id(), 1, Integer::sum);
                l4FailCounts.put(node.id(), 0);
                if (l3Fails >= 2 && currentState != HealthState.DOWN) {
                    nodeStates.put(node.id(), HealthState.UNHEALTHY);
                    clusterState.updateHealth(node.id(), NodeHealth.UNHEALTHY);
                    log.warn("Node {} is UNHEALTHY (L3 failed {} times)", node.id(), l3Fails);
                } else if (currentState == HealthState.HEALTHY) {
                    nodeStates.put(node.id(), HealthState.SUSPECT);
                    clusterState.updateHealth(node.id(), NodeHealth.SUSPECT);
                }
                return;
            }

            // L4 failures (primary only) move to DOWN after 2 consecutive failures.
            l3FailCounts.put(node.id(), 0);
            if (!l4) {
                int l4Fails = l4FailCounts.merge(node.id(), 1, Integer::sum);
                if (l4Fails >= 2 && currentState != HealthState.DOWN) {
                    nodeStates.put(node.id(), HealthState.DOWN);
                    clusterState.updateHealth(node.id(), NodeHealth.DOWN);
                    log.error("Node {} is DOWN (L4 failed {} times)", node.id(), l4Fails);
                    if (listener != null) listener.onNodeDown(node.id());
                } else {
                    nodeStates.put(node.id(), HealthState.UNHEALTHY);
                    clusterState.updateHealth(node.id(), NodeHealth.UNHEALTHY);
                    log.warn("Node {} is UNHEALTHY (L4 write check failed)", node.id());
                }
            }
        }
    }

    public HealthState getState(String nodeId) {
        return nodeStates.getOrDefault(nodeId, HealthState.HEALTHY);
    }

    public boolean isHealthy(String nodeId) {
        return getState(nodeId) == HealthState.HEALTHY;
    }

    /**
     * Suppress health checks for a node (used during backup window). While
     * suppressed: probe is skipped, no onNodeDown/onNodeRecovered fires,
     * existing fail counters are cleared so the resume-time first probe
     * starts fresh.
     */
    public void suppress(String nodeId) {
        if (nodeId == null) return;
        suppressed.add(nodeId);
        // Clear counters so when we unsuppress, the first probe doesn't
        // immediately trip a stale threshold from before-the-pause.
        l12FailCounts.put(nodeId, 0);
        l3FailCounts.put(nodeId, 0);
        l4FailCounts.put(nodeId, 0);
        successCounts.put(nodeId, 0);
        log.info("HealthChecker: suppressed probes for node {}", nodeId);
    }

    public void unsuppress(String nodeId) {
        if (nodeId == null) return;
        if (suppressed.remove(nodeId)) {
            log.info("HealthChecker: resumed probes for node {}", nodeId);
        }
    }

    public boolean isSuppressed(String nodeId) {
        return suppressed.contains(nodeId);
    }
}
