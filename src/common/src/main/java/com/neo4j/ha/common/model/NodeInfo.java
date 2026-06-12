package com.neo4j.ha.common.model;

public record NodeInfo(
    String id,
    NodeRole role,
    String boltUri,
    NodeHealth health,
    NodeServiceState serviceState,
    long syncLagMs,
    long lastCheck,
    boolean pendingCleanup
) {
    public NodeInfo withRole(NodeRole newRole) {
        return new NodeInfo(id, newRole, boltUri, health, serviceState, syncLagMs, lastCheck, pendingCleanup);
    }

    public NodeInfo withHealth(NodeHealth newHealth) {
        return new NodeInfo(id, role, boltUri, newHealth, serviceState, syncLagMs, lastCheck, pendingCleanup);
    }

    public NodeInfo withServiceState(NodeServiceState newState) {
        return new NodeInfo(id, role, boltUri, health, newState, syncLagMs, lastCheck, pendingCleanup);
    }

    public NodeInfo withPendingCleanup(boolean cleanup) {
        return new NodeInfo(id, role, boltUri, health, serviceState, syncLagMs, lastCheck, cleanup);
    }

    /**
     * BUG-076: before this, {@code syncLagMs} was set once by
     * {@link com.neo4j.ha.agent.bootstrap.ClusterInitializer} (to 0) and never
     * updated again, so every call to {@code GET /cluster/status} reported
     * {@code syncLagMs: 0} regardless of the true replication lag. The real
     * lag value was being computed in {@code HaAgent.evaluateServiceStates}
     * but only published to Prometheus; the {@link NodeInfo} shown to
     * operators and the {@code ha-backup-pause-test.sh} script stayed stale.
     * This setter lets {@code evaluateServiceStates} keep the per-node view
     * in sync with the per-node lag computation.
     */
    public NodeInfo withSyncLagMs(long newLagMs) {
        return new NodeInfo(id, role, boltUri, health, serviceState, newLagMs, lastCheck, pendingCleanup);
    }
}
