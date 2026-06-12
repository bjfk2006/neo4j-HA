package com.neo4j.ha.agent.routing;

import com.neo4j.ha.common.metrics.HaMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HaProxyUpdater {

    private static final Logger log = LoggerFactory.getLogger(HaProxyUpdater.class);

    private final List<HaProxyInstance> instances;
    private final HaProxySocketClient socketClient;
    private final String primaryBackend;
    private final String readBackend;
    private final HaMetrics metrics;

    public HaProxyUpdater(List<HaProxyInstance> instances, HaProxySocketClient socketClient,
                           String primaryBackend, String readBackend, HaMetrics metrics) {
        this.instances = instances;
        this.socketClient = socketClient;
        this.primaryBackend = primaryBackend;
        this.readBackend = readBackend;
        this.metrics = metrics;
    }

    /**
     * Swap write-backend primary from oldPrimaryServer to newPrimaryServer in a way that
     * guarantees:
     *   1. NO "no server available" window (keeps ≥ 1 writable server at all times) — BUG-036
     *   2. NO writes continuing to the old primary after the switch — BUG-042
     *
     * Sequence:
     *   (1) `set server <newPrimary> state ready`   — add new primary to the pool. Briefly
     *       there are TWO writable servers; harmless because the old primary's CDC has
     *       already been stopped by the orchestrator before this call (FailoverOrchestrator
     *       Phase 3: cdcCollector.stop()).
     *   (2) `set server <oldPrimary> state maint`   — remove the old primary from routing.
     *       NEW connections go to the new primary. EXISTING connections still hold on to
     *       the old primary (HAProxy default: maint doesn't kill sessions).
     *   (3) `shutdown sessions server <backend>/<oldPrimary>` — BUG-042 fix. Force-close
     *       every active session on the old primary. Clients' Bolt drivers reconnect, and
     *       HAProxy now routes them to the new primary. Without this step a long-lived
     *       write session (e.g. a connection-pooled Bolt driver) continues MERGE'ing
     *       against the OLD primary, which is no longer captured by CDC. Those writes
     *       become "orphan writes": only exist on the demoted node, never replicated,
     *       silent data loss after the node is cleaned up or itself switches role again.
     */
    public void switchPrimary(String newPrimaryServer, String oldPrimaryServer) {
        for (HaProxyInstance instance : instances) {
            boolean newPrimaryEnabled = false;
            try {
                // 1. Enable new primary FIRST.
                socketClient.sendCommand(instance.socketPath(),
                    "set server %s/%s state ready".formatted(primaryBackend, newPrimaryServer));
                newPrimaryEnabled = true;

                // 2. Disable old primary (stops NEW routing).
                socketClient.sendCommand(instance.socketPath(),
                    "set server %s/%s state maint".formatted(primaryBackend, oldPrimaryServer));

                // 3. Force-close EXISTING sessions on the old primary (BUG-042). HAProxy's
                //    `set state maint` intentionally does not disturb in-flight TCP
                //    connections; without this follow-up, a connection-pooled write client
                //    keeps writing against the demoted node forever.
                try {
                    socketClient.sendCommand(instance.socketPath(),
                        "shutdown sessions server %s/%s".formatted(primaryBackend, oldPrimaryServer));
                } catch (IOException e) {
                    // Non-fatal: sessions will eventually drain on their own once idle.
                    log.warn("HAProxy {}: shutdown-sessions for {}/{} failed — existing "
                            + "write connections may briefly keep hitting the demoted node: {}",
                        instance.id(), primaryBackend, oldPrimaryServer, e.toString());
                }

                log.info("HAProxy {} primary switched: {} -> {} (old sessions dropped)",
                    instance.id(), oldPrimaryServer, newPrimaryServer);
                metrics.recordHaproxyReachable(instance.id(), true);
            } catch (IOException e) {
                if (newPrimaryEnabled) {
                    log.warn("HAProxy {}: new primary enabled but old primary could not be set to maint; "
                        + "StateSyncer will reconcile", instance.id(), e);
                } else {
                    log.warn("HAProxy {}: failed to enable new primary — writes may still go to old "
                        + "primary until StateSyncer reconciles", instance.id(), e);
                }
                metrics.recordHaproxyReachable(instance.id(), false);
            }
        }
    }

    public void setServerState(String backend, String server, String state) {
        for (HaProxyInstance instance : instances) {
            try {
                socketClient.sendCommand(instance.socketPath(),
                    "set server %s/%s state %s".formatted(backend, server, state));
            } catch (IOException e) {
                log.warn("Failed to set server state on HAProxy {}", instance.id(), e);
            }
        }
    }

    /**
     * Block ALL client write traffic on the write backend by putting every server into
     * `maint` and killing existing sessions. Used during switchover as the first step so
     * that no client-visible write succeeds while CDC / trigger re-wiring is in progress
     * (BUG-044). Clients connecting / holding connections during this window receive
     * ServiceUnavailable; Neo4j managed transactions retry automatically up to 30 s.
     *
     * The list of servers to block is read from haproxy via `show servers state`
     * (same mechanism StateSyncer uses) so we don't depend on caller knowing every
     * backend server. If the socket command fails we log and continue — worst case a
     * straggler server still accepts writes, but in practice all haproxy.cfg-defined
     * servers share a common instance list.
     */
    public void blockWrites(String primaryServer, java.util.List<String> otherServers) {
        for (HaProxyInstance instance : instances) {
            // Tell all servers in write backend (primary + backup standbys) to go maint.
            java.util.List<String> allServers = new java.util.ArrayList<>();
            if (primaryServer != null) allServers.add(primaryServer);
            if (otherServers != null) allServers.addAll(otherServers);
            for (String srv : allServers) {
                try {
                    socketClient.sendCommand(instance.socketPath(),
                        "set server %s/%s state maint".formatted(primaryBackend, srv));
                } catch (IOException e) {
                    log.warn("blockWrites set-maint for {}/{} on {} failed: {}",
                        primaryBackend, srv, instance.id(), e.toString());
                }
                try {
                    socketClient.sendCommand(instance.socketPath(),
                        "shutdown sessions server %s/%s".formatted(primaryBackend, srv));
                } catch (IOException e) {
                    log.warn("blockWrites shutdown-sessions for {}/{} on {} failed: {}",
                        primaryBackend, srv, instance.id(), e.toString());
                }
            }
            log.info("HAProxy {} write backend BLOCKED ({}). All write attempts will fail "
                + "until unblockWrites().", instance.id(), allServers);
        }
    }

    /**
     * Reverse of {@link #blockWrites}: put ONLY the new primary server back into ready.
     * This is the final atomic step of a switchover — from the instant this command
     * completes, client writes succeed on the new primary. Other servers (standbys)
     * stay in maint in the write backend, as per haproxy.cfg's `backup disabled`
     * semantics.
     */
    public void unblockWrites(String newPrimaryServer) {
        for (HaProxyInstance instance : instances) {
            try {
                socketClient.sendCommand(instance.socketPath(),
                    "set server %s/%s state ready".formatted(primaryBackend, newPrimaryServer));
                log.info("HAProxy {} write backend UNBLOCKED, new primary = {}",
                    instance.id(), newPrimaryServer);
                metrics.recordHaproxyReachable(instance.id(), true);
            } catch (IOException e) {
                log.error("HAProxy {}: failed to unblock writes on {} — cluster will remain "
                    + "write-blocked until StateSyncer reconciles (up to {}ms)!",
                    instance.id(), newPrimaryServer, 10_000, e);
                metrics.recordHaproxyReachable(instance.id(), false);
            }
        }
    }

    public void enableReadBackend(String server) {
        setServerState(readBackend, server, "ready");
        log.info("Enabled {} in read backend", server);
    }

    public void disableReadBackend(String server) {
        setServerState(readBackend, server, "maint");
        log.info("Disabled {} from read backend", server);
    }
}
