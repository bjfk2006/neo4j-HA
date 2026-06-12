package com.neo4j.ha.agent.routing;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HaProxyStateSyncer {

    private static final Logger log = LoggerFactory.getLogger(HaProxyStateSyncer.class);

    private final List<HaProxyInstance> instances;
    private final ClusterStateManager clusterState;
    private final HaProxySocketClient socketClient;
    private final String primaryBackend;
    private final HaMetrics metrics;
    private final long syncIntervalMs;
    private ScheduledExecutorService scheduler;

    /**
     * BUG-049: while a switchover is in progress, the periodic reconciler must NOT run.
     *
     * <p>During Phase 2..10 of the switchover:</p>
     * <ul>
     *   <li>HAProxy is intentionally in "all maint" state (blockWrites).</li>
     *   <li>{@code clusterState.primary} still points at the OLD node (until Phase 7).</li>
     * </ul>
     *
     * <p>The reconciler, seeing "no READY server in write backend but clusterState says
     * OLD is primary", "helpfully" fires {@code set server OLD state ready} — undoing
     * the blockWrites invariant. Client writes then land on OLD, miss CDC (which has
     * already switched to NEW), and become orphan writes on OLD. This produced ~600
     * phantom orphan writes per rotation in load testing.</p>
     *
     * <p>The {@link FailoverOrchestrator} calls {@link #pause()} at the start of a
     * switchover and {@link #resume()} at the end (in {@code finally} so failures
     * still unblock us).</p>
     */
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public HaProxyStateSyncer(List<HaProxyInstance> instances, ClusterStateManager clusterState,
                               HaProxySocketClient socketClient, String primaryBackend,
                               HaMetrics metrics, long syncIntervalMs) {
        this.instances = instances;
        this.clusterState = clusterState;
        this.socketClient = socketClient;
        this.primaryBackend = primaryBackend;
        this.metrics = metrics;
        this.syncIntervalMs = syncIntervalMs;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "haproxy-state-syncer");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::sync, syncIntervalMs, syncIntervalMs, TimeUnit.MILLISECONDS);
        log.info("HAProxy state syncer started with interval={}ms", syncIntervalMs);
    }

    public void stop() {
        if (scheduler != null) scheduler.shutdown();
    }

    /**
     * Suspend the periodic reconciler. Idempotent. See field doc on {@link #paused}
     * for the reason this exists (BUG-049).
     */
    public void pause() {
        if (paused.compareAndSet(false, true)) {
            log.info("HAProxy state syncer paused (switchover in progress)");
        }
    }

    /** Resume the periodic reconciler. Idempotent. */
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            log.info("HAProxy state syncer resumed");
        }
    }

    private void sync() {
        if (paused.get()) {
            // BUG-049: skip reconciliation while a switchover is in flight.
            return;
        }
        String expectedPrimary = clusterState.getPrimaryServerId();
        if (expectedPrimary == null) return;

        // BUG-069: if HealthChecker has classified the current primary as non-HEALTHY
        // (SUSPECT / UNHEALTHY / DOWN), HAProxy's own L4 tcp-check is very likely right
        // in marking the server DOWN — overriding with `set state ready` here fights
        // HAProxy and produces bursts of "no server available" + connection-refused for
        // clients. In the failover path specifically, StateSyncer runs BEFORE onNodeDown
        // has been called (if at all; cf. BUG-068 pre-fix), so clusterState.primaryNode
        // is stale for up to ~tens of seconds. We suppress only the "revive primary"
        // branch in that case; the "kick non-primary to maint" branch stays active
        // because it's always safe.
        String primaryNodeId = clusterState.getPrimaryNodeId();
        NodeInfo primaryInfo = primaryNodeId != null ? clusterState.getNodeInfo(primaryNodeId) : null;
        boolean primaryHealthy = primaryInfo != null && primaryInfo.health() == NodeHealth.HEALTHY;

        // Collect non-primary server names (standbys or downed old primary) whose HAProxy
        // write-backend state must be maint to prevent stale writes. Server names come from
        // the bolt host portion of each node's URI, matching the naming in haproxy.cfg.
        var nonPrimaryServers = new java.util.ArrayList<String>();
        for (var n : clusterState.getAllNodes()) {
            String server = n.boltUri().replace("bolt://", "").split(":")[0];
            if (!server.equals(expectedPrimary)) {
                nonPrimaryServers.add(server);
            }
        }

        for (HaProxyInstance instance : instances) {
            try {
                String state = socketClient.sendCommand(instance.socketPath(),
                    "show servers state " + primaryBackend);

                var desiredChanges = computeDesiredChanges(state, expectedPrimary,
                        nonPrimaryServers, primaryHealthy);
                if (!desiredChanges.isEmpty()) {
                    applyChanges(instance, desiredChanges);
                    log.info("Fixed HAProxy {} routing state ({} commands): {}",
                        instance.id(), desiredChanges.size(), desiredChanges);
                    metrics.recordHaproxyStateSyncFix(instance.id());
                }
                metrics.recordHaproxyReachable(instance.id(), true);
                metrics.recordHaproxyStateSync(instance.id());
            } catch (IOException e) {
                log.warn("HAProxy {} unreachable for state sync", instance.id());
                metrics.recordHaproxyReachable(instance.id(), false);
            }
        }
    }

    /**
     * Compute the minimum set of `set server … state …` commands needed to bring this
     * HAProxy instance to the desired state. Returns an empty list when already consistent.
     *
     * Decision uses BOTH op_state (runtime up/down) AND admin_state (admin flags):
     *   - "Primary is serving" ⟺ op_state == RUNNING (2)
     *     (admin_state is not sufficient: a `backup disabled` server that was later set to
     *      ready may still report admin_state=CMAINT=0x04 in `show servers state`, even
     *      though HAProxy is routing traffic to it normally. BUG-035.)
     *   - "Non-primary is out of pool" ⟺ admin_state has ANY maint/drain bit
     *     (mask 0x3F = FMAINT|IMAINT|CMAINT|FDRAIN|IDRAIN|RMAINT)
     */
    private static final int SRV_ADMF_ANY = 0x3F;
    private static final int SRV_OP_RUNNING = 2;

    static java.util.List<String> computeDesiredChanges(String state, String expectedPrimary,
                                                          List<String> nonPrimaryServers,
                                                          boolean primaryHealthy) {
        var cmds = new java.util.ArrayList<String>();
        if (state == null) return cmds;

        java.util.Map<String, ServerState> byName = parseServerStates(state);

        ServerState primary = byName.get(expectedPrimary);
        if (primary == null) return cmds; // primary missing from backend listing — can't act

        // Primary: only issue `set state ready` when HAProxy considers it NOT running AND
        // HealthChecker considers the primary HEALTHY. If HealthChecker has already
        // flagged the primary as SUSPECT/UNHEALTHY/DOWN (BUG-069), we trust HAProxy's
        // L4 probe verdict over our (stale) clusterState.primaryNode, and let Failover
        // catch up rather than reviving a dead server back into the write pool.
        if (primary.opState != SRV_OP_RUNNING && primaryHealthy) {
            cmds.add("set server " + expectedPrimary + " state ready");
        }

        // Non-primary: force into maint if admin_state has no maint/drain bit (i.e. it's in
        // the active pool). This covers the case where a failover left some non-primary
        // server in ready state.
        for (String server : nonPrimaryServers) {
            ServerState s = byName.get(server);
            if (s == null) continue;
            if ((s.adminState & SRV_ADMF_ANY) == 0) {
                cmds.add("set server " + server + " state maint");
            }
        }
        return cmds;
    }

    /** Immutable per-server snapshot parsed from `show servers state` output. */
    record ServerState(int opState, int adminState) {}

    private static java.util.Map<String, ServerState> parseServerStates(String state) {
        var map = new java.util.HashMap<String, ServerState>();
        int nameIdx = -1, opIdx = -1, adminIdx = -1;
        for (String line : state.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            if (trimmed.startsWith("#")) {
                String[] headers = trimmed.substring(1).trim().split("\\s+");
                for (int i = 0; i < headers.length; i++) {
                    switch (headers[i]) {
                        case "srv_name"        -> nameIdx = i;
                        case "srv_op_state"    -> opIdx = i;
                        case "srv_admin_state" -> adminIdx = i;
                    }
                }
                continue;
            }
            if (nameIdx < 0 || opIdx < 0 || adminIdx < 0) continue;
            String[] parts = trimmed.split("\\s+");
            int maxIdx = Math.max(nameIdx, Math.max(opIdx, adminIdx));
            if (parts.length <= maxIdx) continue;
            try {
                int op = Integer.parseInt(parts[opIdx]);
                int adm = Integer.parseInt(parts[adminIdx]);
                map.put(parts[nameIdx], new ServerState(op, adm));
            } catch (NumberFormatException ignored) { /* skip */ }
        }
        return map;
    }

    private void applyChanges(HaProxyInstance instance, java.util.List<String> commands) throws IOException {
        for (String cmd : commands) {
            // Prepend the backend name to each server reference: "set server <name> state X"
            // becomes "set server <backend>/<name> state X".
            String cmdWithBackend = cmd.replaceFirst("set server (\\S+)",
                "set server " + primaryBackend + "/$1");
            try {
                socketClient.sendCommand(instance.socketPath(), cmdWithBackend);
            } catch (IOException e) {
                log.warn("Failed to apply '{}' on {}: {}", cmdWithBackend, instance.id(), e.getMessage());
                throw e;
            }

            // BUG-042 follow-up: whenever we push a server to maint state, also force-close
            // its existing sessions. Without this, a connection-pooled write client that
            // established its connection before the HAProxy restart/reconfiguration will
            // keep writing to the demoted server, producing orphan writes that never hit
            // CDC. Match case-insensitively to tolerate variations in command formatting.
            if (cmd.toLowerCase().endsWith("state maint")) {
                // Extract server name from "set server <name> state maint"
                String[] parts = cmd.split("\\s+");
                if (parts.length >= 3 && "server".equalsIgnoreCase(parts[1])) {
                    String serverName = parts[2];
                    try {
                        socketClient.sendCommand(instance.socketPath(),
                            "shutdown sessions server %s/%s".formatted(primaryBackend, serverName));
                    } catch (IOException e) {
                        log.warn("shutdown-sessions for {}/{} on {} failed (non-fatal): {}",
                            primaryBackend, serverName, instance.id(), e.getMessage());
                    }
                }
            }
        }
    }

}
