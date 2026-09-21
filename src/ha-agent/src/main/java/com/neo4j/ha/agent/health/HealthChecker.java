package com.neo4j.ha.agent.health;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
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
     * REVIEW-H4: consecutive probe rounds in which the node was not fully
     * healthy, <b>regardless of which layer failed</b>. Reset only by an
     * all-green round.
     *
     * <p>The per-layer counters cannot carry this job because each failure
     * branch zeroes the OTHER layers' counters: an L1/L2 failure clears
     * {@code l3FailCounts}/{@code l4FailCounts}, and a successful L1/L2 clears
     * {@code l12FailCounts}. A node that alternates between "TCP blip" and
     * "Cypher error" therefore resets both counters on every round, neither one
     * ever reaches its threshold, and the node sits in SUSPECT forever — so
     * {@code onNodeDown} never fires and a flapping primary is never failed
     * over. This counter makes the DOWN escalation immune to which layer is
     * failing on any given round.</p>
     */
    private final Map<String, Integer> consecutiveFailures = new ConcurrentHashMap<>();

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

    /**
     * REVIEW-H3: the user database used to be hardcoded to {@code "neo4j"}
     * inside {@code checkNode}, ignoring {@code cluster.nodes[].neo4j.database}.
     * On a deployment using any other database name the L3/L4 probes ran against
     * a database that does not exist, so EVERY node was permanently UNHEALTHY —
     * which in turn makes {@code StandbySelector.selectBest()} throw and failover
     * impossible. Silent, total, and only visible on non-default deployments.
     */
    private final String database;

    /**
     * REVIEW-H1: probes run on a bounded worker so one wedged node cannot stall
     * the whole health loop. Cached pool, one thread per node in practice.
     */
    private final ExecutorService probeExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "health-probe");
        t.setDaemon(true);
        return t;
    });

    public HealthChecker(ClusterStateManager clusterState, Neo4jHealthChecker healthChecker,
                          HaMetrics metrics, HaConfig.FailoverConfig failoverConfig) {
        this(clusterState, healthChecker, metrics, failoverConfig, "neo4j");
    }

    public HealthChecker(ClusterStateManager clusterState, Neo4jHealthChecker healthChecker,
                          HaMetrics metrics, HaConfig.FailoverConfig failoverConfig,
                          String database) {
        this.clusterState = clusterState;
        this.healthChecker = healthChecker;
        this.metrics = metrics;
        this.intervalMs = failoverConfig.healthCheck().intervalMs();
        this.timeoutMs = (int) failoverConfig.healthCheck().timeoutMs();
        this.failThreshold = failoverConfig.healthCheck().failThreshold();
        this.successThreshold = failoverConfig.healthCheck().successThreshold();
        this.database = (database == null || database.isBlank()) ? "neo4j" : database;
    }

    public void setListener(HealthChangeListener listener) {
        this.listener = listener;
    }

    public void start() {
        // Initialize all nodes as HEALTHY
        for (NodeInfo node : clusterState.getAllNodes()) {
            // REVIEW-C12: seed from what ClusterInitializer actually probed, not
            // a blanket HEALTHY. A node that was unreachable at boot is
            // serviceState=OFFLINE; if we seed it HEALTHY and it comes back
            // before accumulating 2 x failThreshold failures, there is never a
            // DOWN -> HEALTHY edge, `onNodeRecovered` never fires, and the node
            // stays OFFLINE forever with nothing to move it on.
            nodeStates.put(node.id(),
                node.health() == NodeHealth.DOWN ? HealthState.DOWN : HealthState.HEALTHY);
            l12FailCounts.put(node.id(), 0);
            l3FailCounts.put(node.id(), 0);
            l4FailCounts.put(node.id(), 0);
            successCounts.put(node.id(), 0);
            consecutiveFailures.put(node.id(), 0);
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
        probeExecutor.shutdownNow();
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

        Probe probe = runProbeWithBudget(node, driver);
        boolean l1 = probe.l1, l2 = probe.l2, l3 = probe.l3, l4 = probe.l4;

        HealthState currentState = nodeStates.getOrDefault(node.id(), HealthState.HEALTHY);

        if (l1 && l2 && l3 && l4) {
            int successes = successCounts.merge(node.id(), 1, Integer::sum);
            l12FailCounts.put(node.id(), 0);
            l3FailCounts.put(node.id(), 0);
            l4FailCounts.put(node.id(), 0);
            consecutiveFailures.put(node.id(), 0);

            if (currentState == HealthState.DOWN && successes >= successThreshold) {
                nodeStates.put(node.id(), HealthState.HEALTHY);
                clusterState.updateHealth(node.id(), NodeHealth.HEALTHY);
                // REVIEW-C12: leave OFFLINE behind. SYNCING is the correct
                // re-entry point — HaAgent.evaluateServiceStates promotes to
                // ONLINE only once the node's replication lag has been under
                // the threshold for stableDuration.
                if (clusterState.getServiceState(node.id()) == NodeServiceState.OFFLINE) {
                    clusterState.setServiceState(node.id(), NodeServiceState.SYNCING);
                }
                log.info("Node {} recovered (was DOWN)", node.id());
                if (listener != null) listener.onNodeRecovered(node.id());
            } else if (currentState != HealthState.HEALTHY && currentState != HealthState.DOWN
                    && successes >= successThreshold) {
                // REVIEW-H4: SUSPECT/UNHEALTHY → HEALTHY now also requires
                // successThreshold consecutive good rounds. It used to flip back
                // on the very first good probe while DOWN → HEALTHY required
                // successThreshold, so a flapping node oscillated between
                // SUSPECT and HEALTHY once per interval and never accumulated
                // enough failures in a row to escalate.
                nodeStates.put(node.id(), HealthState.HEALTHY);
                clusterState.updateHealth(node.id(), NodeHealth.HEALTHY);
                log.info("Node {} back to HEALTHY (was {}, {} consecutive successes)",
                    node.id(), currentState, successes);
            }
        } else {
            successCounts.put(node.id(), 0);
            metrics.healthCheckFailures.increment();

            // REVIEW-H4: layer-agnostic escalation. Counted before the per-layer
            // branches below, none of which can reset it.
            int consecutive = consecutiveFailures.merge(node.id(), 1, Integer::sum);
            if (consecutive >= failThreshold * 2 && currentState != HealthState.DOWN) {
                nodeStates.put(node.id(), HealthState.DOWN);
                clusterState.updateHealth(node.id(), NodeHealth.DOWN);
                markOffline(node.id());
                log.error("{} {} is DOWN ({} consecutive unhealthy probes across mixed layers; "
                        + "l1={} l2={} l3={} l4={})",
                    node.role() == NodeRole.PRIMARY ? "Primary" : "Standby",
                    node.id(), consecutive, l1, l2, l3, l4);
                if (listener != null) listener.onNodeDown(node.id());
                return;
            }

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
                    markOffline(node.id());
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
                    markOffline(node.id());
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

    /**
     * REVIEW-C12: a node that is DOWN cannot be serving traffic, so its
     * serviceState must follow.
     *
     * <p>{@code NodeServiceState}'s own documentation has always specified
     * {@code ONLINE -> OFFLINE: Node goes down} and
     * {@code SYNCING -> OFFLINE: Node goes down}, but nothing implemented it:
     * failover set {@code role=DOWN} and left serviceState untouched, so
     * {@code GET /cluster/status} advertised dead nodes as
     * {@code "role":"DOWN","serviceState":"ONLINE"}. Anything treating
     * serviceState as the readiness signal — the chaos-test precheck, dashboards,
     * an operator eyeballing the endpoint mid-incident — read that as healthy.</p>
     */
    private void markOffline(String nodeId) {
        if (clusterState.getServiceState(nodeId) != NodeServiceState.OFFLINE) {
            clusterState.setServiceState(nodeId, NodeServiceState.OFFLINE);
        }
    }

    /** Result of one probe round for a node. */
    private static final class Probe {
        boolean l1 = true, l2 = false, l3 = false, l4 = true;
    }

    /**
     * REVIEW-H1: run the L1..L4 ladder off the scheduler thread with a hard
     * wall-clock budget.
     *
     * <p>Two independent defences, because either alone is insufficient:</p>
     * <ul>
     *   <li>Each Cypher probe carries a server-side transaction timeout
     *       ({@code Neo4jHealthChecker.checkCypher/checkWrite}), so a slow query
     *       is killed by Neo4j itself.</li>
     *   <li>The whole ladder runs on {@code probeExecutor} with
     *       {@code Future.get(budget)}, so a probe that blocks <i>below</i> the
     *       query layer — a half-open TCP connection inside
     *       {@code verifyConnectivity()}, a socket that accepts and never
     *       answers — cannot pin the single {@code health-checker} thread.</li>
     * </ul>
     *
     * <p>A timeout is reported as "L2 failed", which is the truthful reading:
     * the node did not answer in time. That feeds the normal SUSPECT → DOWN
     * escalation instead of freezing the state machine.</p>
     */
    private Probe runProbeWithBudget(NodeInfo node, Driver driver) {
        // Budget: TCP + Bolt + Cypher + (optional) write, each bounded by
        // timeoutMs, plus a little slack. Bounded so the whole sweep of N nodes
        // still fits comfortably inside a few intervals in the worst case.
        long budgetMs = Math.max(1_000L, timeoutMs * 4L);

        Future<Probe> future;
        try {
            future = probeExecutor.submit(() -> probeInline(node, driver));
        } catch (RejectedExecutionException ree) {
            // Executor already shut down (agent stopping, or a probe racing
            // stop()). Run inline rather than letting an unchecked exception
            // escape into the health loop.
            log.debug("Probe executor unavailable for {}; running probe inline", node.id());
            return probeInline(node, driver);
        }

        try {
            return future.get(budgetMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            future.cancel(true);
            metrics.healthCheckTimeouts.increment();
            log.warn("Health probe for node {} exceeded its {}ms budget; treating as Bolt "
                + "failure so the state machine keeps advancing (REVIEW-H1)", node.id(), budgetMs);
            return unreachableProbe();
        } catch (Exception e) {
            log.debug("Health probe for node {} failed: {}", node.id(), e.toString());
            return unreachableProbe();
        }
    }

    /** The L1..L4 ladder, executed on whatever thread calls it. */
    private Probe probeInline(NodeInfo node, Driver driver) {
        Probe r = new Probe();
        try {
            URI uri = URI.create(node.boltUri().replace("bolt://", "http://"));
            r.l1 = healthChecker.checkTcp(uri.getHost(),
                uri.getPort() > 0 ? uri.getPort() : 7687, timeoutMs);
            if (r.l1) {
                r.l2 = healthChecker.checkBolt(driver);
                if (r.l2) {
                    r.l3 = healthChecker.checkCypher(driver, database);
                    if (r.l3 && node.role() == NodeRole.PRIMARY) {
                        r.l4 = healthChecker.checkWrite(driver, database);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Health check error for node {}: {}", node.id(), e.getMessage());
        }
        return r;
    }

    /** "Did not answer in time" — modelled as an L2 failure. */
    private static Probe unreachableProbe() {
        Probe p = new Probe();
        p.l1 = true; p.l2 = false; p.l3 = false; p.l4 = true;
        return p;
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
        consecutiveFailures.put(nodeId, 0);
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
