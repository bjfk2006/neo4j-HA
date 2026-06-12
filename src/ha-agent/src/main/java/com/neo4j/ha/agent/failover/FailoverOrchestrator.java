package com.neo4j.ha.agent.failover;

import com.neo4j.ha.agent.audit.FailoverAuditLog;
import com.neo4j.ha.agent.backup.BackupCoordinator;
import com.neo4j.ha.agent.bootstrap.ApocTriggerInstaller;
import com.neo4j.ha.agent.bootstrap.IndexInstaller;
import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.recovery.ApocTriggerUninstaller;
import com.neo4j.ha.agent.recovery.PostSwitchoverReconciler;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.agent.routing.HaProxyStateSyncer;
import com.neo4j.ha.agent.routing.HaProxyUpdater;
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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FailoverOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(FailoverOrchestrator.class);

    private final HealthChecker healthChecker;
    private final FencingTokenManager fencingTokenManager;
    private final CdcCollector cdcCollector;
    private final SyncApplier syncApplier;
    private final StandbySelector standbySelector;
    private final ApocTriggerInstaller triggerInstaller;
    private final IndexInstaller indexInstaller;
    private final HaProxyUpdater haProxyUpdater;
    private final HaProxyStateSyncer haProxyStateSyncer;
    private final NodeRegistry nodeRegistry;
    private final ClusterStateManager clusterState;
    private final FailoverAuditLog audit;
    private final HaMetrics metrics;
    private final CheckpointManager checkpointManager;
    private final String database;

    private volatile BackupCoordinator backupCoordinator;

    private final long confirmationWaitMs;
    private final long minIntervalMs;
    private final int maxAutoPerHour;
    private final InflightTxDrainWaiter inflightTxDrainWaiter = new InflightTxDrainWaiter();
    private final AtomicLong lastFailoverTime = new AtomicLong(0);
    private final AtomicInteger failoverCountInHour = new AtomicInteger(0);

    public FailoverOrchestrator(HealthChecker healthChecker, FencingTokenManager fencingTokenManager,
                                 CdcCollector cdcCollector, SyncApplier syncApplier,
                                 StandbySelector standbySelector, ApocTriggerInstaller triggerInstaller,
                                 IndexInstaller indexInstaller, HaProxyUpdater haProxyUpdater,
                                 HaProxyStateSyncer haProxyStateSyncer,
                                 NodeRegistry nodeRegistry, ClusterStateManager clusterState,
                                 FailoverAuditLog audit, HaMetrics metrics,
                                 CheckpointManager checkpointManager,
                                 String database,
                                 long confirmationWaitMs, long minIntervalMs, int maxAutoPerHour) {
        this.healthChecker = healthChecker;
        this.fencingTokenManager = fencingTokenManager;
        this.cdcCollector = cdcCollector;
        this.syncApplier = syncApplier;
        this.standbySelector = standbySelector;
        this.triggerInstaller = triggerInstaller;
        this.indexInstaller = indexInstaller;
        this.haProxyUpdater = haProxyUpdater;
        this.haProxyStateSyncer = haProxyStateSyncer;
        this.nodeRegistry = nodeRegistry;
        this.clusterState = clusterState;
        this.audit = audit;
        this.metrics = metrics;
        this.checkpointManager = checkpointManager;
        this.database = database;
        this.confirmationWaitMs = confirmationWaitMs;
        this.minIntervalMs = minIntervalMs;
        this.maxAutoPerHour = maxAutoPerHour;
    }

    public void setBackupCoordinator(BackupCoordinator backupCoordinator) {
        this.backupCoordinator = backupCoordinator;
    }

    public void executeFailover(String failedNodeId) {
        executeFailover(failedNodeId, true);
    }

    /**
     * @param auto true for automatic failover triggered by health checker — subject to rate
     *             limits (min interval + hourly cap). Manual invocations should pass false
     *             so urgent operator intervention is never blocked (M10).
     */
    public void executeFailover(String failedNodeId, boolean auto) {
        long startTime = System.currentTimeMillis();
        audit.logStart(failedNodeId);

        // Rate limits only apply to automatic failovers (design §11 "最大自动切换次数")
        if (auto && !checkSafeToFailover()) {
            audit.logCancel(failedNodeId, "Safety check failed (rate limit)");
            return;
        }

        try {
            // Phase 1: Confirmation wait
            log.info("Phase 1: Confirmation wait ({}ms) for node {}", confirmationWaitMs, failedNodeId);
            Thread.sleep(confirmationWaitMs);
            if (healthChecker.isHealthy(failedNodeId)) {
                audit.logCancel(failedNodeId, "Node recovered during confirmation");
                log.info("Failover cancelled: node {} recovered", failedNodeId);
                return;
            }

            // BUG-049: pause the periodic HAProxy state reconciler for the duration of
            // the switchover. Without this, the reconciler wakes up mid-switchover,
            // sees "HAProxy has no READY server in neo4j_primary" vs "clusterState says
            // OLD is primary" (Phase 7 hasn't run yet), and helpfully `set server OLD
            // state ready` — undoing Phase 2 blockWrites and allowing ~600 orphan
            // writes to land on OLD during the CDC re-wiring window.
            runSwitchoverSteps(() -> doFailoverPhases2to10(failedNodeId, startTime, auto));
        } catch (Throwable t) {
            // Catch Throwable, not Exception: a NoSuchMethodError / NoClassDefFoundError
            // (version-skew between modules) or OutOfMemoryError / StackOverflowError
            // used to silently kill the ha-failover thread with no log, no audit, and
            // no metric — observed via "Phase 3 completes, Phase 4 never logs".
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordFailover(false, duration);
            audit.logFailed(failedNodeId, t instanceof Exception ex ? ex : new RuntimeException(t));
            log.error("Failover FAILED for node {}", failedNodeId, t);
        }
    }

    /** Common wrapper that pauses/resumes the HAProxy state reconciler. BUG-049. */
    private void runSwitchoverSteps(Runnable steps) {
        if (haProxyStateSyncer != null) {
            haProxyStateSyncer.pause();
        }
        try {
            steps.run();
        } finally {
            if (haProxyStateSyncer != null) {
                haProxyStateSyncer.resume();
            }
        }
    }

    private void doFailoverPhases2to10(String failedNodeId, long startTime, boolean auto) {
        try {
            // Phase 2 (BUG-044 + BUG-048): WRITE BLOCK FIRST.
            //
            // HAProxy puts EVERY server in the write backend into maint + kills
            // existing sessions. From this moment until the final unblockWrites every
            // client write receives ServiceUnavailable.  This is the core invariant:
            //   no client write can succeed on the OLD primary while CDC is being
            //   re-wired, so there is no window where a commit lands on OLD but isn't
            //   captured by CDC.
            //
            // Neo4j managed-transaction clients auto-retry; non-managed clients (like
            // our load-test's `session.run`) see explicit failures that they can
            // retry at application level.
            //
            // Strategic cost: writes are blocked for ~1-2 s (mostly trigger install +
            // checkpoint copy). Benefit: zero-data-loss switchover.
            String oldPrimaryServer = clusterState.getServerIdForNode(failedNodeId);
            String newPrimary = standbySelector.selectBest();
            String newPrimaryServer = clusterState.getServerIdForNode(newPrimary);
            java.util.List<String> allStandbyServers = new java.util.ArrayList<>();
            for (var n : clusterState.getAllNodes()) {
                String srv = clusterState.getServerIdForNode(n.id());
                if (srv != null && !srv.equals(oldPrimaryServer)) {
                    allStandbyServers.add(srv);
                }
            }
            haProxyUpdater.blockWrites(oldPrimaryServer, allStandbyServers);
            log.info("Phase 2: Write backend BLOCKED (newPrimary={} will unblock at the end)",
                newPrimary);

            // Phase 2.5 (BUG-056): Wait for in-flight Neo4j write tx drain on
            // OLD. In the failover path the OLD primary may be unreachable
            // already (that's why we're failing over) — InflightTxDrainWaiter
            // handles null/broken driver gracefully (logs + returns). When OLD
            // is reachable this closes the same orphan-write window as in
            // executeSwitchover.
            try {
                Driver oldDriverForDrain = clusterState.getDriver(failedNodeId);
                long drainWait = inflightTxDrainWaiter.await(oldDriverForDrain, database);
                log.info("Phase 2.5: in-flight tx drain complete ({}ms)", drainWait);
            } catch (Exception e) {
                log.warn("Phase 2.5: in-flight tx drain skipped (OLD primary unreachable?): {}",
                    e.toString());
            }

            // Phase 2.6 (BUG-061): Drain APOC afterAsync queue on OLD primary
            // before CdcCollector.stop()'s final poll. See doSwitchoverPhases
            // Phase 2.6 for rationale. Best-effort in the failover path — OLD
            // primary may be dead; in that case the drain probe will fail
            // fast and we continue (the data on a dead primary is already
            // lost, so there's nothing to drain).
            try {
                Driver oldDriverForAsyncDrain = clusterState.getDriver(failedNodeId);
                boolean afterAsyncDrained = ApocTriggerInstaller.drainRelTriggerAfterAsync(
                    oldDriverForAsyncDrain, database);
                if (afterAsyncDrained) {
                    log.info("Phase 2.6: APOC afterAsync queue drained on {}", failedNodeId);
                } else {
                    log.warn("Phase 2.6: afterAsync drain on {} did NOT confirm; naked rels may result",
                        failedNodeId);
                }
            } catch (Exception e) {
                log.warn("Phase 2.6: afterAsync drain skipped (OLD primary unreachable?): {}",
                    e.toString());
            }

            // Phase 3 (BUG-047 + BUG-048): Stop data sync AFTER blockWrites but BEFORE
            // incrementing the fence token.
            //
            // * "After blockWrites" — so that no new client commits can land on the
            //   OLD primary once CDC starts winding down. `cdcCollector.stop()` runs
            //   a final flushing poll which picks up everything committed up to the
            //   instant HAProxy flipped to maint (already-running tx will either
            //   commit before `shutdown sessions` closes the socket, or abort; the
            //   final poll captures the ones that committed).
            // * "Before fence increment" — `fencingTokenManager.increment()` updates
            //   the shared Redis `fencing-token` atomically. If CDC is still alive,
            //   an in-flight `publishBatch` observes `redis-token > local-token` and
            //   throws FencingTokenRejectedException, draining that batch into the
            //   buffer (BUG-047 safety net) but still disrupting the pipeline.
            //   Stopping CDC first eliminates the race entirely.
            log.info("Phase 3: Stopping data sync on now-quiesced OLD primary");
            if (backupCoordinator != null) {
                backupCoordinator.cancelForFailover();
            }
            cdcCollector.stop();
            syncApplier.stop();
            syncApplier.drainPending();

            // Phase 3.5 (BUG-080): record a pending-reconcile intent for the old
            // primary.  cdcCollector.stop() just persisted its final CDC cursor
            // for failedNodeId; we snapshot that cursor NOW, before
            // copyCdcCheckpoint mutates the new primary's checkpoint or a second
            // failover overwrites anything. When the old primary eventually
            // comes back as a STANDBY, OldPrimaryRecovery Step 4.5 will read
            // this intent and replay its on-disk afterAsync stragglers onto
            // whichever node is primary at that time.
            //
            // Best-effort only: if loadCdcCheckpoint fails or the old node
            // never published anything (both cursors 0), we skip. The only
            // consequence of skipping is that a genuine afterAsync gap on that
            // node would go unreconciled — observable via rel-gap-diag.sh and
            // always hand-recoverable, i.e. strictly no worse than before this
            // mechanism existed.
            try {
                var cp = checkpointManager.loadCdcCheckpoint(failedNodeId).orElse(null);
                if (cp != null && (cp.lastRelTs() > 0 || cp.lastDeleteTs() > 0)) {
                    checkpointManager.savePendingReconcile(failedNodeId,
                        new com.neo4j.ha.common.redis.CheckpointManager.PendingReconcile(
                            cp.lastRelTs(), cp.lastDeleteTs(), cp.lastNodeTs(),
                            System.currentTimeMillis(), newPrimary));
                    // Force OldPrimaryRecovery.execute to fire on this node's next
                    // HealthChecker up-transition — otherwise tryCleanupOldPrimary's
                    // happy path (Phase 8 cleanup succeeds, old primary stayed
                    // reachable) never sets pendingCleanup and Step 4.5 never runs
                    // to consume the intent we just persisted.
                    nodeRegistry.markPendingCleanup(failedNodeId, true);
                    log.info("Phase 3.5 (BUG-080): recorded pending-reconcile for {} " +
                        "(cursor rel={}, del={}, node={}; newPrimary={})",
                        failedNodeId, cp.lastRelTs(), cp.lastDeleteTs(), cp.lastNodeTs(),
                        newPrimary);
                } else {
                    log.info("Phase 3.5 (BUG-080): skipping pending-reconcile for {} " +
                        "(no CDC checkpoint or cold cursors)", failedNodeId);
                }
            } catch (Exception e) {
                log.warn("Phase 3.5 (BUG-080): failed to record pending-reconcile for {}: {}",
                    failedNodeId, e.toString());
            }

            // Phase 4: Fence — increment the epoch. CDC is already stopped so
            // this is just the authoritative "no more writes from the OLD epoch".
            long newToken = fencingTokenManager.increment();
            log.info("Phase 4: Fencing token incremented to {}", newToken);

            // Phase 5: Prepare the new primary — APOC triggers + indexes + checkpoint.
            // Done while writes are still blocked so the NEW primary's Triggers/indexes
            // are fully installed before ANY client write lands on it.
            log.info("Phase 5: Selected new primary: {}", newPrimary);
            Driver newPrimaryDriver = clusterState.getDriver(newPrimary);
            triggerInstaller.ensureInstalled(newPrimaryDriver, database);
            indexInstaller.ensureIndexes(newPrimaryDriver, database);
            // Copy CDC checkpoint from the failed primary to the new primary (M7/BUG-027)
            checkpointManager.copyCdcCheckpoint(failedNodeId, newPrimary);
            log.info("Phase 5: NEW primary {} prepared (triggers, indexes, checkpoint)", newPrimary);

            // Phase 6: Start CDC polling on the NEW primary. The CDC loop is now live
            // against the NEW primary but nothing is writing there yet (write backend
            // still blocked).
            cdcCollector.switchTarget(newPrimaryDriver, newPrimary, newToken);
            log.info("Phase 6: CDC switched to NEW primary");

            // Phase 7: Update cluster state
            nodeRegistry.updateRole(newPrimary, NodeRole.PRIMARY);
            nodeRegistry.updateRole(failedNodeId, NodeRole.DOWN);
            clusterState.setPrimary(newPrimary);
            clusterState.updateRole(newPrimary, NodeRole.PRIMARY);
            clusterState.updateRole(failedNodeId, NodeRole.DOWN);
            log.info("Phase 7: Cluster state updated");

            // Phase 8: Restart SyncApplier with remaining standbys
            syncApplier.start(clusterState.getStandbyDrivers(), database, newToken);
            log.info("Phase 8: SyncApplier restarted with remaining standbys");

            // Phase 9: Best-effort old primary cleanup
            tryCleanupOldPrimary(failedNodeId);

            // Phase 10 (BUG-044): WRITE UNBLOCK — put the NEW primary into ready. This
            // is the atomic cut-over instant; from here on client writes succeed on the
            // NEW primary and are captured by its Triggers + CDC pipeline.
            haProxyUpdater.unblockWrites(newPrimaryServer);
            log.info("Phase 10: Write backend UNBLOCKED on {}", newPrimaryServer);

            long duration = System.currentTimeMillis() - startTime;
            lastFailoverTime.set(System.currentTimeMillis());
            if (auto) failoverCountInHour.incrementAndGet();
            metrics.recordFailover(true, duration);
            audit.logComplete(failedNodeId, newPrimary, duration);
            log.info("Failover complete: {} -> {} in {}ms", failedNodeId, newPrimary, duration);

        } catch (Throwable t) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordFailover(false, duration);
            audit.logFailed(failedNodeId, t instanceof Exception ex ? ex : new RuntimeException(t));
            log.error("Failover FAILED for node {}", failedNodeId, t);
        }
    }

    public void executeSwitchover(String targetNodeId) {
        String currentPrimary = clusterState.getPrimaryNodeId();
        long startTime = System.currentTimeMillis();
        audit.logStart(currentPrimary);

        // Switchover is always a planned/manual operation; rate limits from checkSafeToFailover
        // are intentionally NOT applied (M10). Operators must be able to switch on demand.

        try {
            String selected = targetNodeId;
            if (selected == null || selected.isBlank()) {
                selected = standbySelector.selectBest();
            } else {
                var targetInfo = clusterState.getNodeInfo(selected);
                if (targetInfo == null || targetInfo.role() != NodeRole.STANDBY) {
                    throw new IllegalArgumentException("Target node is not a standby: " + selected);
                }
                if (targetInfo.serviceState() != NodeServiceState.ONLINE) {
                    throw new IllegalStateException("Target standby is not ONLINE: " + selected);
                }
            }
            final String newPrimary = selected;

            // BUG-049: pause the HAProxy reconciler so it doesn't "fix" blockWrites
            // mid-switchover. Resumed in the finally of runSwitchoverSteps().
            runSwitchoverSteps(() -> doSwitchoverPhases(currentPrimary, newPrimary, startTime));
        } catch (Throwable t) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordFailover(false, duration);
            audit.logFailed(currentPrimary, t instanceof Exception ex ? ex : new RuntimeException(t));
            log.error("Switchover FAILED from {}", currentPrimary, t);
        }
    }

    private void doSwitchoverPhases(String currentPrimary, String newPrimary, long startTime) {
        try {
            // BUG-044 + BUG-048: WRITE BLOCK FIRST.
            // Put every server in the write backend into maint and kill existing
            // sessions BEFORE stopping CDC. This is the no-orphan-writes invariant:
            // from this instant forward, no client write can succeed on the OLD
            // primary. The subsequent cdcCollector.stop() drains everything already
            // committed up to this point.
            String newPrimaryServer = clusterState.getServerIdForNode(newPrimary);
            String oldPrimaryServer = clusterState.getServerIdForNode(currentPrimary);
            java.util.List<String> allOtherServers = new java.util.ArrayList<>();
            for (var n : clusterState.getAllNodes()) {
                String srv = clusterState.getServerIdForNode(n.id());
                if (srv != null && !srv.equals(oldPrimaryServer)) {
                    allOtherServers.add(srv);
                }
            }
            haProxyUpdater.blockWrites(oldPrimaryServer, allOtherServers);
            log.info("Switchover: write backend BLOCKED; {} -> {} beginning", currentPrimary, newPrimary);

            // Phase 2.5 (BUG-056): Wait for in-flight Neo4j write tx on OLD to
            // drain. blockWrites only stops HAProxy routing + TCP; kernel-side
            // tx already in commit pipeline continue to land, producing orphan
            // writes that CdcCollector.stop() may miss if its drain loop is
            // too eager. Polling SHOW TRANSACTIONS until count=0 (or timeout)
            // provides the real quiescence guarantee.
            Driver oldPrimaryDriverForDrain = clusterState.getDriver(currentPrimary);
            long drainWait = inflightTxDrainWaiter.await(oldPrimaryDriverForDrain, database);
            log.info("Switchover: Phase 2.5 in-flight tx drain complete ({}ms)", drainWait);

            // Phase 2.6 (BUG-061): Drain APOC afterAsync queue on OLD primary
            // BEFORE CdcCollector.stop() runs its final poll.
            //
            // cdc-rel-timestamp runs in phase:'afterAsync'. Stamps for rels
            // committed in the last ~100ms before blockWrites may still be
            // sitting in APOC's async executor queue. If we stop CDC now,
            // the final keyset scan
            //   WHERE r._updated_at > $lastTs
            // will miss every rel whose stamp is still pending — those rels
            // have _updated_at IS NULL. Worse, the subsequent
            // ApocTriggerUninstaller.uninstall() will then drop the trigger
            // and APOC silently discards the pending tasks, permanently
            // leaving those rels "naked" on this node and invisible to
            // every future CDC poll after this node is demoted.
            //
            // The drain probe inserts a sentinel rel and waits until its
            // _updated_at becomes non-null. Because writes are already
            // blocked (Phase 2) and business tx already drained (Phase 2.5),
            // the sentinel is guaranteed to be the LAST item queued, so
            // observing its stamp proves every prior rel has been stamped.
            boolean afterAsyncDrained = ApocTriggerInstaller.drainRelTriggerAfterAsync(
                oldPrimaryDriverForDrain, database);
            if (!afterAsyncDrained) {
                log.warn("Switchover: Phase 2.6 afterAsync drain on {} did NOT confirm in time; "
                    + "proceeding, but naked rels may appear on old primary.", currentPrimary);
            } else {
                log.info("Switchover: Phase 2.6 APOC afterAsync queue drained on {}", currentPrimary);
            }

            // BUG-047 + BUG-048: Stop data sync AFTER blockWrites and BEFORE fence
            // increment. stop() runs a final poll under a quiesced primary so every
            // committed write (up to the HAProxy cut) is captured. After CDC is
            // stopped, increment() is free of TOCTOU risk.
            if (backupCoordinator != null) {
                backupCoordinator.cancelForFailover();
            }
            cdcCollector.stop();
            syncApplier.stop();
            syncApplier.drainPending();

            // BUG-080 (graceful): snapshot OLD primary's post-stop CDC cursor
            // BEFORE copyCdcCheckpoint (line below) rewrites the NEW primary's
            // checkpoint key. cdcCollector.stop() has just run its final
            // flushing poll and persisted the highest (_updated_at) it
            // published; everything on OLD's disk past this cursor is — by
            // definition — stranded afterAsync work the drain chain didn't
            // catch. Captured here as a local snapshot so subsequent mutation
            // of either node's checkpoint can't race us.
            //
            // The 2026-04-22 ha-load-switchover-test disproved the previous
            // assumption that the Phase 2.5 InflightTxDrainWaiter + Phase 2.6
            // drainRelTriggerAfterAsync + stop()'s final poll together form
            // a zero-loss drain. Observed rel_miss=28 after a single
            // graceful rotation under sustained load means some
            // afterAsync rel stamps still land past the drain horizon. The
            // reverse-reconcile (added back below, after switchTarget)
            // closes that gap without slowing the drain's warning value —
            // drain failures still log, we just don't lose the rels.
            long reconcileRelCursor = 0L;
            long reconcileDelCursor = 0L;
            try {
                var cp = checkpointManager.loadCdcCheckpoint(currentPrimary).orElse(null);
                if (cp != null) {
                    reconcileRelCursor = cp.lastRelTs();
                    reconcileDelCursor = cp.lastDeleteTs();
                }
            } catch (Exception e) {
                log.warn("Switchover: could not snapshot OLD primary {} CDC cursor for "
                    + "graceful reconcile ({}); reconcile will be skipped", currentPrimary,
                    e.toString());
            }

            long newToken = fencingTokenManager.increment();
            log.info("Switchover: CDC stopped; fencing token incremented to {}", newToken);

            // Prepare new primary: triggers + indexes + checkpoint copy. Because writes
            // are globally blocked, this happens on a quiescent cluster.
            Driver newPrimaryDriver = clusterState.getDriver(newPrimary);
            triggerInstaller.ensureInstalled(newPrimaryDriver, database);
            indexInstaller.ensureIndexes(newPrimaryDriver, database);
            // M7 / BUG-027: preserve CDC cursor across switchover.
            checkpointManager.copyCdcCheckpoint(currentPrimary, newPrimary);

            // Start CDC on the NEW primary while writes are still blocked. The CDC
            // loop is now live against the NEW primary but nothing has written there
            // yet — it is idle, waiting for the write unblock.
            cdcCollector.switchTarget(newPrimaryDriver, newPrimary, newToken);

            nodeRegistry.updateRole(newPrimary, NodeRole.PRIMARY);
            nodeRegistry.updateRole(currentPrimary, NodeRole.STANDBY);
            clusterState.setPrimary(newPrimary);
            clusterState.updateRole(newPrimary, NodeRole.PRIMARY);
            clusterState.updateRole(currentPrimary, NodeRole.STANDBY);
            clusterState.setServiceState(currentPrimary, NodeServiceState.SYNCING);

            // BUG-080 (graceful reconcile): reverse-reconcile any afterAsync
            // stragglers that slipped past the drain chain.
            //
            // ORDERING — this block MUST run:
            //   * AFTER switchTarget above: the reconciler writes land on
            //     newPrimary via direct bolt; the NEW primary's CDC loop is
            //     already live and will publish the replay to the Redis
            //     stream. Standbys converge through the normal sync path.
            //   * BEFORE the `_CDCDeleteEvent` cleanup block below: REL_DELETED
            //     / NODE_DELETED scratch nodes are scanned out of the OLD
            //     primary's disk; deleting them first would erase the
            //     evidence.
            //   * BEFORE syncApplier.start() below: while the applier is
            //     stopped, the reconciler's publish → applier replay
            //     ordering is strictly serial (stream is the queue); when
            //     start() fires, standbys will consume the replay from the
            //     same stream position along with any other catch-up.
            //   * BEFORE unblockWrites below: the cluster is still globally
            //     write-blocked, so no client write races the replay on
            //     newPrimary. Any extra RTO cost is paid only while the
            //     cluster is already quiesced.
            //
            // The scanned cursor comes from the post-stop snapshot above —
            // independent of copyCdcCheckpoint's mutation of newPrimary's
            // key. Cold-cursor / empty-scan / budget-exhausted paths are
            // all handled inside PostSwitchoverReconciler.reconcile and are
            // best-effort: a failure here is no worse than the pre-BUG-080
            // state (which was "drop rels silently"), and is surfaced via
            // the reconcile log + rel-gap-diag.sh.
            //
            // Why CRASH failover still uses PendingReconcile instead of
            // running the reconciler inline: on the failover path the OLD
            // primary is unreachable by definition, so the scan has to be
            // deferred to recovery time. Graceful switchover keeps OLD
            // reachable throughout — no deferral needed.
            try {
                var reconciler = new PostSwitchoverReconciler(clusterState, database);
                int replayed = reconciler.reconcile(
                    currentPrimary, newPrimary,
                    reconcileRelCursor, reconcileDelCursor);
                if (replayed > 0) {
                    log.info("Switchover: BUG-080 graceful reconcile replayed {} " +
                        "stranded event(s) from {} -> {} (cursors rel={}, del={})",
                        replayed, currentPrimary, newPrimary,
                        reconcileRelCursor, reconcileDelCursor);
                } else {
                    log.info("Switchover: BUG-080 graceful reconcile found no stranded " +
                        "events on {} past cursors rel={}, del={}",
                        currentPrimary, reconcileRelCursor, reconcileDelCursor);
                }
            } catch (Throwable t) {
                // Non-fatal: a failed reconcile leaves the cluster in the
                // same state as before this code ran (i.e. drain-only).
                log.warn("Switchover: BUG-080 graceful reconcile failed for {} -> {} " +
                    "({}); proceeding with cleanup. Any stranded rels/deletes are " +
                    "observable via rel-gap-diag.sh.",
                    currentPrimary, newPrimary, t.toString());
            }

            // Uninstall APOC Triggers and clean up _CDCDeleteEvent on old primary (now standby).
            // BUG-046: the uninstall MUST succeed — otherwise the demoted node still has
            // triggers that rewrite _updated_at on any SyncApplier MERGE, corrupting the
            // CDC keyset cursor if this node ever becomes primary again.
            Driver oldPrimaryDriver = clusterState.getDriver(currentPrimary);
            boolean triggersUninstalled = false;
            try {
                triggersUninstalled = ApocTriggerUninstaller.uninstall(oldPrimaryDriver, database);
                try (Session cleanupSession = oldPrimaryDriver.session()) {
                    long deleted;
                    do {
                        deleted = cleanupSession.run(
                            "MATCH (e:_CDCDeleteEvent) WITH e LIMIT 10000 DETACH DELETE e RETURN count(*) AS c"
                        ).single().get("c").asLong();
                    } while (deleted > 0);
                }
                if (triggersUninstalled) {
                    log.info("Switchover: Old primary {} triggers uninstalled and cleanup done", currentPrimary);
                } else {
                    log.warn("Switchover: Old primary {} trigger uninstall returned partial failure; marking for deferred cleanup", currentPrimary);
                    nodeRegistry.markPendingCleanup(currentPrimary, true);
                }
            } catch (Exception e) {
                log.warn("Switchover: Failed to clean old primary {}, marking for deferred cleanup", currentPrimary, e);
                nodeRegistry.markPendingCleanup(currentPrimary, true);
            }

            syncApplier.start(clusterState.getStandbyDrivers(), database, newToken);

            // BUG-044: WRITE UNBLOCK. Atomic cut-over instant: writes resume on NEW.
            haProxyUpdater.unblockWrites(newPrimaryServer);
            log.info("Switchover: write backend UNBLOCKED on {}", newPrimaryServer);

            long duration = System.currentTimeMillis() - startTime;
            lastFailoverTime.set(System.currentTimeMillis());
            // Switchover is manual; do not charge against the automatic hourly quota (M10).
            metrics.recordFailover(true, duration);
            audit.logComplete(currentPrimary, newPrimary, duration);
            log.info("Switchover complete: {} -> {} in {}ms", currentPrimary, newPrimary, duration);
        } catch (Throwable t) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordFailover(false, duration);
            audit.logFailed(currentPrimary, t instanceof Exception ex ? ex : new RuntimeException(t));
            log.error("Switchover FAILED from {}", currentPrimary, t);
        }
    }

    private final AtomicLong hourWindowStart = new AtomicLong(System.currentTimeMillis());

    private boolean checkSafeToFailover() {
        long now = System.currentTimeMillis();
        long timeSinceLast = now - lastFailoverTime.get();
        if (timeSinceLast < minIntervalMs && lastFailoverTime.get() > 0) {
            log.warn("Failover blocked: minimum interval not met ({}ms < {}ms)", timeSinceLast, minIntervalMs);
            return false;
        }
        // Reset hourly counter if window expired
        if (now - hourWindowStart.get() > 3_600_000) {
            failoverCountInHour.set(0);
            hourWindowStart.set(now);
        }
        if (failoverCountInHour.get() >= maxAutoPerHour) {
            log.warn("Failover blocked: max auto failovers per hour reached ({})", maxAutoPerHour);
            return false;
        }
        return true;
    }

    private void tryCleanupOldPrimary(String failedNodeId) {
        try {
            Driver oldDriver = clusterState.getDriver(failedNodeId);
            boolean ok = ApocTriggerUninstaller.uninstall(oldDriver, database);
            if (!ok) {
                nodeRegistry.markPendingCleanup(failedNodeId, true);
                log.warn("Phase 8: Trigger uninstall partial failure on {}; marked for deferred cleanup", failedNodeId);
                return;
            }
            try (Session session = oldDriver.session()) {
                long total = 0;
                long deleted;
                do {
                    deleted = session.run(
                        "MATCH (e:_CDCDeleteEvent) WITH e LIMIT 10000 DETACH DELETE e RETURN count(*) AS c"
                    ).single().get("c").asLong();
                    total += deleted;
                } while (deleted > 0);
                if (total > 0) {
                    log.info("Phase 8: Cleaned {} residual _CDCDeleteEvent nodes from old primary {}", total, failedNodeId);
                }
            }
            log.info("Phase 8: Old primary {} cleaned up immediately", failedNodeId);
        } catch (Exception e) {
            nodeRegistry.markPendingCleanup(failedNodeId, true);
            log.info("Phase 8: Old primary {} unreachable, marked for deferred cleanup", failedNodeId);
        }
    }
}
