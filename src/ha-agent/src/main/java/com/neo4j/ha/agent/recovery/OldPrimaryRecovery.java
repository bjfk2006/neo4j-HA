package com.neo4j.ha.agent.recovery;

import com.neo4j.ha.agent.audit.FailoverAuditLog;
import com.neo4j.ha.agent.bootstrap.IndexInstaller;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.cdc.CdcCollector;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.NodeRole;
import com.neo4j.ha.common.model.NodeServiceState;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.sync.SyncApplier;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OldPrimaryRecovery {

    private static final Logger log = LoggerFactory.getLogger(OldPrimaryRecovery.class);

    /**
     * Minimum interval between automatic full-sync exports for the same node.
     * A flapping node (UP → DOWN → UP → …) must not drive repeated full-sync bursts
     * against the new primary, which would amplify backpressure and risk a
     * cluster-wide recovery storm. Operator can still force fullsync via the
     * Admin API (BUG-039).
     */
    private static final long DEFAULT_FULLSYNC_MIN_INTERVAL_MS = 60L * 60L * 1000L; // 1 hour

    private final ClusterStateManager clusterState;
    private final SyncApplier syncApplier;
    private final CdcCollector cdcCollector;
    private final CheckpointManager checkpointManager;
    private final NodeRegistry nodeRegistry;
    private final IndexInstaller indexInstaller;
    private final FailoverAuditLog audit;
    private final HaMetrics metrics;
    private final String changesStreamKey;
    /**
     * REVIEW-H3: the user database was hardcoded to "neo4j" in three places here
     * ({@code ensureIndexes}, {@code PostSwitchoverReconciler}, and the untargeted
     * {@code driver.session()}), even though {@code cluster.nodes[].neo4j.database}
     * is a real config knob. On any deployment using a non-default database the
     * recovery silently indexed / reconciled / cleaned the wrong database.
     */
    private final String database;
    private final long fullsyncMinIntervalMs;

    /**
     * Single-thread daemon executor for full-sync export tasks. Guarantees:
     * - At most one full-sync export runs at a time (single-thread)
     * - Daemon thread: JVM shutdown is not blocked by in-flight exports even if
     *   {@link #shutdown()} is not called (defense in depth)
     * - Lifecycle managed: {@link #shutdown()} drains in-flight exports for up
     *   to {@value #SHUTDOWN_DRAIN_TIMEOUT_SECONDS}s before forcing interrupt
     *
     * Assumed singleton: {@link OldPrimaryRecovery} is constructed once in
     * {@link com.neo4j.ha.agent.HaAgent}; if this ever changes to per-request,
     * the executor field must move out of declaration init.
     *
     * BUG-051 fix (Code Review M1): replaces raw `new Thread()` which was
     * non-daemon and had no lifecycle management. Follow-up review M1+M2:
     * added explicit shutdown() and failure-path bookkeeping.
     */
    private static final int SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 30;
    private final ExecutorService fullsyncExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "fullsync-worker");
        t.setDaemon(true);
        return t;
    });

    /** Per-node "last automatic full-sync start" timestamp (wall-clock ms). */
    private final Map<String, Long> lastAutoFullsyncByNode = new ConcurrentHashMap<>();

    public OldPrimaryRecovery(ClusterStateManager clusterState, SyncApplier syncApplier,
                               CdcCollector cdcCollector,
                               CheckpointManager checkpointManager, NodeRegistry nodeRegistry,
                               IndexInstaller indexInstaller, FailoverAuditLog audit,
                               HaMetrics metrics, String changesStreamKey, String database) {
        this(clusterState, syncApplier, cdcCollector, checkpointManager, nodeRegistry,
                indexInstaller, audit, metrics, changesStreamKey, database,
                DEFAULT_FULLSYNC_MIN_INTERVAL_MS);
    }

    public OldPrimaryRecovery(ClusterStateManager clusterState, SyncApplier syncApplier,
                               CdcCollector cdcCollector,
                               CheckpointManager checkpointManager, NodeRegistry nodeRegistry,
                               IndexInstaller indexInstaller, FailoverAuditLog audit,
                               HaMetrics metrics, String changesStreamKey, String database,
                               long fullsyncMinIntervalMs) {
        this.clusterState = clusterState;
        this.syncApplier = syncApplier;
        this.cdcCollector = cdcCollector;
        this.checkpointManager = checkpointManager;
        this.nodeRegistry = nodeRegistry;
        this.indexInstaller = indexInstaller;
        this.audit = audit;
        this.metrics = metrics;
        this.changesStreamKey = changesStreamKey;
        this.database = (database == null || database.isBlank()) ? "neo4j" : database;
        this.fullsyncMinIntervalMs = fullsyncMinIntervalMs;
    }

    public void execute(String oldPrimaryId) {
        // REVIEW-R2: recovery tasks and failover tasks share one single-thread
        // executor, so a queued recovery can be overtaken by a failover that
        // promotes the very node this recovery is about to demote. Concrete
        // double-fault sequence:
        //   node-01 fails over out   -> recovery(node-01) queued
        //   node-02 then fails       -> failover promotes node-01 back to PRIMARY
        //   queued recovery(node-01) finally runs
        // Without this guard Step 1 uninstalls the CDC triggers from the LIVE
        // primary (so no write is ever captured again), Step 3 demotes it to
        // STANDBY, and Step 6 subscribes SyncApplier to replay onto itself.
        String primaryAtEntry = clusterState.getPrimaryNodeId();
        if (oldPrimaryId != null && oldPrimaryId.equals(primaryAtEntry)) {
            log.warn("Skipping old-primary recovery for {}: it is the CURRENT primary "
                + "(promoted while this task was queued). Running it would uninstall the "
                + "live primary's CDC triggers and demote it.", oldPrimaryId);
            nodeRegistry.markPendingCleanup(oldPrimaryId, false);
            return;
        }

        log.info("Starting old primary recovery for node: {}", oldPrimaryId);
        audit.logRecoveryStart(oldPrimaryId);
        metrics.oldPrimaryRecoveryTotal.increment();
        Driver oldDriver = clusterState.getDriver(oldPrimaryId);

        try {
            // Step 1: Uninstall APOC triggers (BUG-046: must succeed or we abort recovery;
            // otherwise the node still mutates _updated_at on SyncApplier MERGE).
            // REVIEW-R1: gate on triggersDropped() ONLY. The afterAsync drain result is
            // reported separately — a drain that could not be confirmed means some rel
            // stamps may be missing (the healer repairs those), not that the node is
            // still armed with triggers.
            var uninstallResult = ApocTriggerUninstaller.uninstallDetailed(oldDriver, database);
            if (!uninstallResult.afterAsyncDrained() && !uninstallResult.drainSkipped()) {
                log.warn("Recovery of {}: afterAsync drain not confirmed; some relationships may "
                    + "be missing their stamps on this node. Continuing — NakedRelationshipHealer "
                    + "covers this case.", oldPrimaryId);
            }
            boolean triggersOk = uninstallResult.triggersDropped();
            if (!triggersOk) {
                audit.logRecoveryFailed(oldPrimaryId, new IllegalStateException(
                    "APOC trigger uninstall did not fully succeed; aborting recovery to avoid data corruption"));
                log.error("Aborting recovery of {}: APOC trigger uninstall failed. Node left as DOWN for manual intervention.", oldPrimaryId);
                nodeRegistry.markPendingCleanup(oldPrimaryId, true);
                return;
            }
            log.info("Step 1: APOC Triggers uninstalled from {}", oldPrimaryId);

            // Step 2: Cleanup residual _CDCDeleteEvent transit nodes
            long cleaned = cleanupResidualDeleteEvents(oldDriver);
            log.info("Step 2: Cleaned {} residual _CDCDeleteEvent from {}", cleaned, oldPrimaryId);

            // Step 3: Update role to STANDBY
            nodeRegistry.updateRole(oldPrimaryId, NodeRole.STANDBY);
            clusterState.updateRole(oldPrimaryId, NodeRole.STANDBY);
            clusterState.setServiceState(oldPrimaryId, NodeServiceState.SYNCING);
            log.info("Step 3: {} role set to STANDBY/SYNCING", oldPrimaryId);

            // Step 4: Ensure standby indexes
            // REVIEW-B1: recovery runs on the single ha-failover thread; a
            // full-graph dup scan here would block real failovers behind it.
            indexInstaller.ensureIndexes(oldDriver, database, false);
            log.info("Step 4: Standby indexes ensured on {}", oldPrimaryId);

            // Step 4.5 (BUG-080): reverse-reconcile afterAsync writes that were
            // stranded on this node's disk when failover killed it. The
            // FailoverOrchestrator.doFailoverPhases2to10 persisted a
            // PendingReconcile intent in Redis before copyCdcCheckpoint
            // advanced the cluster past the failure point; we consume that
            // intent here, scan the old primary's disk for rels/deletes past
            // the snapshotted cursor, and replay them onto the CURRENT primary
            // (which may differ from the "new primary at failover time" if the
            // cluster failed over again while this node was down).
            //
            // Ordering rationale for running BEFORE Step 6 (addTarget):
            //   1. Triggers are uninstalled (Step 1) — our MERGE/CREATE won't
            //      emit spurious _CDCDeleteEvent or double-stamp _updated_at
            //      on this node itself.
            //   2. Role is STANDBY (Step 3) — HAProxy sends no client writes
            //      here, so our direct-bolt write to the new primary is the
            //      only activity that could touch it.
            //   3. SyncApplier has NOT yet subscribed to this node — the new
            //      primary's CDC will publish our replay to the stream, and
            //      this node will consume it through the normal path moments
            //      later (Step 6) along with any other catch-up events. If we
            //      added the node first, we'd race "reconciler writing new
            //      primary" against "SyncApplier replaying back to old
            //      primary", which is safe (MERGE is idempotent) but noisy.
            //
            // Failure is swallowed: a failed reconcile leaves the cluster in
            // exactly the state it was already in before this code ran. The
            // pending marker is always cleared (one-shot per failover).
            try {
                var pendingOpt = checkpointManager.loadPendingReconcile(oldPrimaryId);
                if (pendingOpt.isPresent()) {
                    var p = pendingOpt.get();
                    String currentPrimary = clusterState.getPrimaryNodeId();
                    if (currentPrimary == null || currentPrimary.equals(oldPrimaryId)) {
                        log.warn("Step 4.5 (BUG-080): no distinct primary available " +
                            "(currentPrimary={}, oldPrimary={}); skipping reconcile, " +
                            "keeping pending-reconcile for next recovery attempt",
                            currentPrimary, oldPrimaryId);
                    } else {
                        log.info("Step 4.5 (BUG-080): reconciling {} stranded writes onto " +
                            "current primary {} (failoverTs={}, cursors rel={}, del={})",
                            oldPrimaryId, currentPrimary, p.failoverTs(),
                            p.lastRelTs(), p.lastDeleteTs());
                        var reconciler = new PostSwitchoverReconciler(clusterState, database);
                        int replayed = reconciler.reconcile(
                            oldPrimaryId, currentPrimary,
                            p.lastRelTs(), p.lastDeleteTs());
                        log.info("Step 4.5 (BUG-080): replayed {} stranded event(s) from {} onto {}",
                            replayed, oldPrimaryId, currentPrimary);
                        // One-shot semantics: clear the marker regardless of
                        // replayed count. A partial replay's remainder is
                        // observable via rel-gap-diag.sh; we do NOT want to
                        // re-scan on every subsequent restart.
                        checkpointManager.deletePendingReconcile(oldPrimaryId);
                    }
                }
            } catch (Exception e) {
                log.warn("Step 4.5 (BUG-080): pending-reconcile processing failed for {} " +
                    "({}); proceeding to normal sync path", oldPrimaryId, e.toString());
            }

            // Step 5: Evaluate sync strategy
            SyncDecision decision = evaluateSyncStrategy(oldPrimaryId);
            log.info("Step 5: Sync strategy for {} = {}", oldPrimaryId, decision);

            // Step 6: Start sync
            // The full sync path must (a) register the node as a sync target so the FullSyncReceiver
            // exists on the SyncApplier side, AND (b) actually trigger FullSyncCoordinator to publish
            // FULL_SYNC_START/BATCH*/END. Without (b) the receiver would sit in IDLE forever (M1).
            // REVIEW-R3: only subscribe the node to incremental replay once we know
            // how it is going to catch up. The old code called addTarget()
            // unconditionally, so a FULL_SYNC decision whose export was then
            // suppressed by the BUG-039 rate limit left the node consuming the
            // changes stream from a checkpoint already known to be invalid — the
            // gap between "what it has" and "where the stream starts" is silently
            // skipped, then its lag drops below the threshold and
            // evaluateServiceStates promotes it to ONLINE and sends reads to it.
            boolean fullSyncStarting = false;
            if (decision == SyncDecision.FULL_SYNC) {
                fullSyncStarting = canStartAutoFullsync(oldPrimaryId);
                if (!fullSyncStarting) {
                    long lastMs = lastAutoFullsyncByNode.getOrDefault(oldPrimaryId, 0L);
                    long sinceMs = System.currentTimeMillis() - lastMs;
                    log.error("Auto full-sync for {} SUPPRESSED (last one {} ms ago < min interval "
                        + "{} ms) and its checkpoint is INVALID. Node is NOT being added as a sync "
                        + "target: replaying from an invalid checkpoint would silently skip the gap "
                        + "and the node would later be promoted ONLINE with divergent data. It stays "
                        + "SYNCING/out-of-rotation until an operator runs "
                        + "POST /cluster/fullsync?nodeId={}.",
                        oldPrimaryId, sinceMs, fullsyncMinIntervalMs, oldPrimaryId);
                    if (metrics != null) {
                        metrics.autoFullsyncSuppressedTotal.increment();
                    }
                    clusterState.setServiceState(oldPrimaryId, NodeServiceState.SYNCING);
                    nodeRegistry.markPendingCleanup(oldPrimaryId, true);
                    audit.logRecoveryFailed(oldPrimaryId, new IllegalStateException(
                        "full-sync suppressed by rate limit and checkpoint invalid; "
                        + "operator must trigger /cluster/fullsync"));
                    return;
                }
            }

            syncApplier.addTarget(oldDriver, oldPrimaryId);
            if (fullSyncStarting) {
                // BUG-039 rate limit was already evaluated above (REVIEW-R3); reaching
                // here means the export is allowed to start.
                {
                    markAutoFullsyncStarted(oldPrimaryId);
                    log.info("Triggering full sync export for old primary {}", oldPrimaryId);
                    fullsyncExecutor.submit(() -> {
                        try {
                            cdcCollector.createFullSyncCoordinator().startFullSync(oldPrimaryId);
                        } catch (Exception e) {
                            // BUG-051 follow-up M2: a swallowed failure here used to leave
                            // the node silently stuck in SYNCING — no audit, no metric, and
                            // the rate-limit timestamp would block automatic retry for an
                            // hour. Now we (a) record the failure for observability,
                            // (b) emit an audit entry, and (c) clear the rate-limit
                            // timestamp so the next recovery attempt can re-try immediately
                            // (operator can still preempt via /cluster/fullsync).
                            log.error("Full sync export failed for {}", oldPrimaryId, e);
                            if (metrics != null) {
                                metrics.autoFullsyncFailedTotal.increment();
                            }
                            try {
                                audit.logRecoveryFailed(oldPrimaryId, e);
                            } catch (Exception auditEx) {
                                log.warn("Audit log for fullsync failure also failed for {}", oldPrimaryId, auditEx);
                            }
                            lastAutoFullsyncByNode.remove(oldPrimaryId);
                        }
                    });
                }
            }
            log.info("Step 6-7: Sync started for {}, waiting for ONLINE", oldPrimaryId);

            // Step 8: Clear pending cleanup
            nodeRegistry.markPendingCleanup(oldPrimaryId, false);

            audit.logRecoveryComplete(oldPrimaryId);
            log.info("Old primary recovery initiated for {}", oldPrimaryId);

        } catch (Exception e) {
            log.error("Old primary recovery failed for {}", oldPrimaryId, e);
            audit.logRecoveryFailed(oldPrimaryId, e);
        }
    }

    private long cleanupResidualDeleteEvents(Driver driver) {
        long totalCleaned = 0;
        try (Session session = driver.session(
                org.neo4j.driver.SessionConfig.forDatabase(database))) {
            long cleaned;
            do {
                cleaned = session.executeWrite(tx ->
                    tx.run("MATCH (e:_CDCDeleteEvent) WITH e LIMIT 10000 DETACH DELETE e RETURN count(*) AS c")
                        .single().get("c").asLong()
                );
                totalCleaned += cleaned;
            } while (cleaned >= 10000);
        }
        return totalCleaned;
    }

    private SyncDecision evaluateSyncStrategy(String nodeId) {
        Optional<CheckpointManager.SyncCheckpoint> cp = checkpointManager.loadSyncCheckpoint(nodeId);
        if (cp.isEmpty()) return SyncDecision.FULL_SYNC;

        boolean valid = checkpointManager.isCheckpointValid(changesStreamKey, cp.get().lastStreamId());
        return valid ? SyncDecision.INCREMENTAL : SyncDecision.FULL_SYNC;
    }

    enum SyncDecision { INCREMENTAL, FULL_SYNC }

    /**
     * Returns true iff the last automatic full-sync for this node happened longer
     * ago than {@link #fullsyncMinIntervalMs} — or never. Operator-initiated
     * full-syncs via Admin API do NOT update this state, so an operator override
     * is always immediate.
     */
    private boolean canStartAutoFullsync(String nodeId) {
        Long last = lastAutoFullsyncByNode.get(nodeId);
        if (last == null) return true;
        return (System.currentTimeMillis() - last) >= fullsyncMinIntervalMs;
    }

    private void markAutoFullsyncStarted(String nodeId) {
        lastAutoFullsyncByNode.put(nodeId, System.currentTimeMillis());
    }

    /**
     * Stops the full-sync executor, waiting up to
     * {@link #SHUTDOWN_DRAIN_TIMEOUT_SECONDS} seconds for an in-flight
     * fullsync to complete cleanly before forcing interrupt. Idempotent.
     *
     * Called from {@link com.neo4j.ha.agent.lifecycle.GracefulShutdown}.
     * Daemon-thread semantics on the executor are kept as defense in depth so
     * that JVM shutdown still works even if this method is skipped.
     *
     * BUG-051 follow-up M1: the original fix relied solely on daemon threads,
     * which would *interrupt* an in-flight fullsync at JVM exit and leave the
     * receiving standby in a half-synced state. Draining gives the export a
     * chance to finish (or at least to publish FULL_SYNC_END / abort cleanly).
     */
    public void shutdown() {
        log.info("Shutting down OldPrimaryRecovery fullsync executor (drain up to {}s)",
            SHUTDOWN_DRAIN_TIMEOUT_SECONDS);
        fullsyncExecutor.shutdown();
        try {
            if (!fullsyncExecutor.awaitTermination(SHUTDOWN_DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Fullsync executor did not drain in {}s; forcing shutdownNow. "
                    + "An in-flight fullsync may be aborted mid-stream — the affected "
                    + "standby will need a fresh fullsync after restart.",
                    SHUTDOWN_DRAIN_TIMEOUT_SECONDS);
                fullsyncExecutor.shutdownNow();
            } else {
                log.info("Fullsync executor drained cleanly");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fullsyncExecutor.shutdownNow();
        }
    }
}
