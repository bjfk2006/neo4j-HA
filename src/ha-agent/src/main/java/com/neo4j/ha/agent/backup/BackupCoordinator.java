package com.neo4j.ha.agent.backup;

import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.routing.HaProxyStateSyncer;
import com.neo4j.ha.agent.routing.HaProxyUpdater;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.sync.SyncApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Coordinates a standby-node backup window. On {@link #prepare(String)} the
 * target node is put into <b>maintenance</b>: a single API call atomically
 * pauses every Agent sub-system that could otherwise interfere with a
 * docker-stopped standby (sync replay, health-check alarms, HAProxy state
 * reconciliation, HAProxy read routing). {@link #complete()} reverses
 * everything in opposite order.
 *
 * <p>Why 4 things together (v2): docker stopping the standby for cp triggers
 * cascade effects if Agent isn't told:
 * <ul>
 *   <li>HealthChecker would see bolt port down → onNodeDown spam</li>
 *   <li>HaProxyStateSyncer (10s reconciler) would re-enable a read backend that
 *       is currently a dead docker-stopped container</li>
 *   <li>SyncApplier would keep consuming changes-stream events and applying
 *       them on top of a half-restored state</li>
 *   <li>HAProxy read backend would still send client reads to the stopped node</li>
 * </ul>
 *
 * <p>{@link #checkTimeout()} is the safety net: if the script crashes between
 * prepare and complete, {@code maxBackupDurationMs} elapses and we auto-resume
 * everything. Operator script's trap also calls complete on signal, so this
 * is purely a "last line of defense".
 */
public class BackupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(BackupCoordinator.class);

    private final SyncApplier syncApplier;
    private final CheckpointManager checkpointManager;
    private final HaMetrics metrics;
    private final long maxBackupDurationMs;

    // v2: extra collaborators for the 4-thing coordination
    private final HealthChecker healthChecker;
    private final HaProxyStateSyncer haProxyStateSyncer;
    private final HaProxyUpdater haProxyUpdater;
    private final ClusterStateManager clusterState;

    private volatile BackupState state = BackupState.IDLE;
    private volatile Instant prepareTime;
    private volatile Instant lastBackupTime;
    /** Node currently in backup mode (only set between prepare and complete). */
    private volatile String preparedNodeId;
    /** HAProxy server name for the prepared node, cached so complete can restore it. */
    private volatile String preparedServerId;

    public BackupCoordinator(SyncApplier syncApplier, CheckpointManager checkpointManager,
                              HaMetrics metrics, long maxBackupDurationMs,
                              HealthChecker healthChecker,
                              HaProxyStateSyncer haProxyStateSyncer,
                              HaProxyUpdater haProxyUpdater,
                              ClusterStateManager clusterState) {
        this.syncApplier = syncApplier;
        this.checkpointManager = checkpointManager;
        this.metrics = metrics;
        this.maxBackupDurationMs = maxBackupDurationMs;
        this.healthChecker = healthChecker;
        this.haProxyStateSyncer = haProxyStateSyncer;
        this.haProxyUpdater = haProxyUpdater;
        this.clusterState = clusterState;
    }

    /** Backward-compatible constructor for callers that don't need the v2 coordination. */
    public BackupCoordinator(SyncApplier syncApplier, CheckpointManager checkpointManager,
                              HaMetrics metrics, long maxBackupDurationMs) {
        this(syncApplier, checkpointManager, metrics, maxBackupDurationMs,
             null, null, null, null);
    }

    public synchronized BackupPrepareResult prepare(String nodeId) {
        if (state != BackupState.IDLE) {
            throw new IllegalStateException("Backup already in progress: " + state);
        }
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required");
        }

        // M-1 fix: keep `state` at IDLE until *after* the lookups that might
        // throw. If getServerIdForNode raises (e.g. nodeId not registered),
        // we end up in `catch` with state still IDLE — caller sees the
        // failure, BackupCoordinator stays usable for the next attempt.
        String serverId = null;
        try {
            serverId = clusterState != null
                ? clusterState.getServerIdForNode(nodeId) : null;

            state = BackupState.PREPARING;
            metrics.backupState.set(1);

            // 1. Pause SyncApplier so applied events stop landing on the standby
            syncApplier.pause();
            log.info("Backup prepare: SyncApplier paused for {}", nodeId);

            // 2. Suppress HealthChecker probes so docker-stopped bolt port
            //    doesn't trigger onNodeDown cascade.
            if (healthChecker != null) {
                healthChecker.suppress(nodeId);
            }

            // 3. Pause HaProxyStateSyncer so its 10s reconciler doesn't
            //    re-enable our maint'd read backend mid-backup.
            if (haProxyStateSyncer != null) {
                haProxyStateSyncer.pause();
            }

            // 4. Take node out of HAProxy read backend so clients won't read
            //    from a docker-stopped container.
            if (haProxyUpdater != null && serverId != null) {
                haProxyUpdater.disableReadBackend(serverId);
                log.info("Backup prepare: HAProxy read backend disabled for server {}", serverId);
            }

            prepareTime = Instant.now();
            preparedNodeId = nodeId;
            preparedServerId = serverId;
            state = BackupState.IN_PROGRESS;
            metrics.backupState.set(2);
            log.info("Backup prepared for node {} (server {})", nodeId, serverId);
            return new BackupPrepareResult(nodeId, prepareTime);

        } catch (RuntimeException e) {
            // Rollback any partial state. Even if cluster doesn't have
            // server mapping or reconciler is null, the SyncApplier.pause
            // above must be reversed.
            log.error("Backup prepare failed for {}, rolling back partial state", nodeId, e);
            rollbackPartialPrepare(nodeId, serverId);
            state = BackupState.IDLE;
            metrics.backupState.set(0);
            throw e;
        }
    }

    public synchronized void complete() {
        if (state != BackupState.IN_PROGRESS) {
            throw new IllegalStateException("No backup in progress: " + state);
        }
        String nodeId = preparedNodeId;
        String serverId = preparedServerId;

        // Reverse order — re-enable routing FIRST so reads can flow as soon
        // as the standby is healthy again.
        if (haProxyUpdater != null && serverId != null) {
            try { haProxyUpdater.enableReadBackend(serverId); }
            catch (Exception e) { log.warn("complete: enableReadBackend({}) failed: {}", serverId, e.toString()); }
        }
        if (haProxyStateSyncer != null) {
            try { haProxyStateSyncer.resume(); }
            catch (Exception e) { log.warn("complete: haProxyStateSyncer.resume failed: {}", e.toString()); }
        }
        if (healthChecker != null && nodeId != null) {
            try { healthChecker.unsuppress(nodeId); }
            catch (Exception e) { log.warn("complete: healthChecker.unsuppress({}) failed: {}", nodeId, e.toString()); }
        }
        try { syncApplier.resume(); }
        catch (Exception e) { log.warn("complete: syncApplier.resume failed: {}", e.toString()); }

        lastBackupTime = Instant.now();
        preparedNodeId = null;
        preparedServerId = null;
        state = BackupState.IDLE;
        metrics.backupState.set(0);
        metrics.lastBackupTimestamp.set(lastBackupTime.toEpochMilli());

        log.info("Backup complete for node {}; sync, health checks, and HAProxy routing restored",
            nodeId);
    }

    public synchronized void cancelForFailover() {
        // M-2 fix: also handle PREPARING (transient state, but can stick if
        // a M-1-style exception left the coordinator half-prepared). In both
        // cases we run the same rollback used on prepare-time failure.
        if (state == BackupState.IN_PROGRESS || state == BackupState.PREPARING) {
            log.warn("Cancelling backup due to failover (state was {}, node was {})",
                state, preparedNodeId);
            rollbackPartialPrepare(preparedNodeId, preparedServerId);
            preparedNodeId = null;
            preparedServerId = null;
            state = BackupState.IDLE;
            metrics.backupState.set(0);
        }
    }

    public synchronized void checkTimeout() {
        if (state == BackupState.IN_PROGRESS && prepareTime != null) {
            if (Duration.between(prepareTime, Instant.now()).toMillis() > maxBackupDurationMs) {
                log.warn("Backup timeout (>{}ms), auto-completing for node {}",
                    maxBackupDurationMs, preparedNodeId);
                try { complete(); }
                catch (IllegalStateException ignored) { /* race */ }
            }
        }
    }

    public BackupState getState() { return state; }
    public Instant getLastBackupTime() { return lastBackupTime; }
    public String getPreparedNodeId() { return preparedNodeId; }

    private void rollbackPartialPrepare(String nodeId, String serverId) {
        if (haProxyUpdater != null && serverId != null) {
            try { haProxyUpdater.enableReadBackend(serverId); } catch (Exception ignored) {}
        }
        if (haProxyStateSyncer != null) {
            try { haProxyStateSyncer.resume(); } catch (Exception ignored) {}
        }
        if (healthChecker != null) {
            try { healthChecker.unsuppress(nodeId); } catch (Exception ignored) {}
        }
        try { syncApplier.resume(); } catch (Exception ignored) {}
    }

    public record BackupPrepareResult(String nodeId, Instant prepareTime) {}
}
