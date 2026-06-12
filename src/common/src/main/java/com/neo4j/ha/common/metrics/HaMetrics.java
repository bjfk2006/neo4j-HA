package com.neo4j.ha.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HaMetrics {

    private final MeterRegistry registry;

    // CDC Collector metrics
    public final Counter cdcEventsPublished;
    public final Counter cdcPollErrors;
    public final Timer cdcPollDuration;
    public final AtomicLong cdcLastPublishedTs = new AtomicLong(0);

    // Sync Applier metrics
    public final Counter syncEventsApplied;
    public final Counter syncApplyErrors;
    public final Timer syncApplyDuration;
    public final AtomicLong syncLagMs = new AtomicLong(0);

    // Failover metrics
    public final Counter failoverTotal;
    public final Counter failoverSuccess;
    public final Counter failoverFailed;
    public final Timer failoverDuration;

    // Health check metrics
    public final Counter healthCheckFailures;

    // HAProxy metrics
    public final Counter haproxyUpdateErrors;

    // Old primary recovery
    public final Counter oldPrimaryRecoveryTotal;
    public final AtomicLong oldPrimaryCleanupEvents = new AtomicLong(0);

    // Backup metrics
    public final AtomicLong backupState = new AtomicLong(0);
    public final AtomicLong lastBackupTimestamp = new AtomicLong(0);

    // Buffer metrics
    public final AtomicLong bufferSize = new AtomicLong(0);

    // Stream retention / maintenance (BUG-038)
    public final AtomicLong streamRetentionCutoffMs = new AtomicLong(0);
    public final Counter streamRetentionTrimmedTotal;
    public final Counter pelTrimmedEventsTotal;
    public final Counter pelRecoveryTrimDetections;

    // BUG-081 mitigation: counts how often `ChangeApplier.applyBatch` had to
    // split a PEL / incremental batch into sub-batches because two events in
    // the same batch shared the same `_elementId` (Neo4j internal rel-id
    // reuse after DELETE). A non-zero rate means id reuse + batching is
    // actively being observed and the plan-A defense is firing. If this
    // counter stays at 0 across a full chaos cycle, BUG-081 is either
    // absent in the workload or has a different root cause (cross-tx
    // visibility / stream trim) and the guard here isn't the actual fix.
    public final Counter batchSplitForDuplicateElementIdTotal;

    // BUG-083: gauge of nodes on the local primary whose `_elementId`
    // PROPERTY value is shared by at least one other live node — i.e. nodes
    // that participate in any duplicate-`_elementId` group. Healthy steady
    // state is 0; sampled by `IndexInstaller.ensureIndexes` (boot,
    // post-failover, post-recovery) so each invocation fully refreshes the
    // gauge. Non-zero indicates the UNIQUE constraint either was not yet
    // installed (legacy database) or somehow let writes through (constraint
    // creation failed silently for some label) — operator should run
    // `scripts/deploy/elementid-dedup.sh` and restart the agent.
    public final AtomicLong dupElementIdNodes = new AtomicLong(0);

    // Auto full-sync suppression (BUG-039)
    public final Counter autoFullsyncSuppressedTotal;

    // Auto full-sync export failures (BUG-051 follow-up). Counts fullsync
    // export tasks that threw from OldPrimaryRecovery's executor — distinct
    // from suppression. A non-zero trend means standbys are likely stuck in
    // SYNCING; check agent logs and consider operator-forced fullsync.
    public final Counter autoFullsyncFailedTotal;

    // === Admin UI metrics ===
    public final Counter uiLoginSuccess;
    public final Counter uiLoginFailure;
    public final Counter uiLoginLocked;
    public final AtomicLong uiSessionActive = new AtomicLong(0);

    public HaMetrics(MeterRegistry registry) {
        this.registry = registry;

        // CDC
        cdcEventsPublished = Counter.builder("neo4j_ha_cdc_events_published_total")
            .description("Total CDC events published").register(registry);
        cdcPollErrors = Counter.builder("neo4j_ha_cdc_poll_errors_total")
            .description("CDC polling errors").register(registry);
        cdcPollDuration = Timer.builder("neo4j_ha_cdc_poll_duration")
            .description("CDC poll cycle duration").register(registry);
        Gauge.builder("neo4j_ha_cdc_last_published_ts", cdcLastPublishedTs, AtomicLong::get)
            .description("Timestamp of last published CDC event").register(registry);

        // Sync
        syncEventsApplied = Counter.builder("neo4j_ha_sync_events_applied_total")
            .description("Total sync events applied").register(registry);
        syncApplyErrors = Counter.builder("neo4j_ha_sync_apply_errors_total")
            .description("Sync apply errors").register(registry);
        syncApplyDuration = Timer.builder("neo4j_ha_sync_apply_duration")
            .description("Sync apply batch duration").register(registry);
        Gauge.builder("neo4j_ha_sync_lag_ms", syncLagMs, AtomicLong::get)
            .description("Current sync lag in milliseconds").register(registry);

        // Failover
        failoverTotal = Counter.builder("neo4j_ha_failover_total")
            .description("Total failover attempts").register(registry);
        failoverSuccess = Counter.builder("neo4j_ha_failover_success_total")
            .description("Successful failovers").register(registry);
        failoverFailed = Counter.builder("neo4j_ha_failover_failed_total")
            .description("Failed failovers").register(registry);
        failoverDuration = Timer.builder("neo4j_ha_failover_duration")
            .description("Failover duration").register(registry);

        // Health
        healthCheckFailures = Counter.builder("neo4j_ha_health_check_failures_total")
            .description("Health check failures").register(registry);

        // HAProxy
        haproxyUpdateErrors = Counter.builder("neo4j_ha_haproxy_update_errors_total")
            .description("HAProxy update errors").register(registry);

        // Recovery
        oldPrimaryRecoveryTotal = Counter.builder("neo4j_ha_old_primary_recovery_total")
            .description("Old primary recovery attempts").register(registry);
        Gauge.builder("neo4j_ha_old_primary_cleanup_events", oldPrimaryCleanupEvents, AtomicLong::get)
            .description("Residual _CDCDeleteEvent nodes cleaned").register(registry);

        // Backup
        Gauge.builder("neo4j_ha_backup_state", backupState, AtomicLong::get)
            .description("Backup state (0=IDLE, 1=PREPARING, 2=IN_PROGRESS)").register(registry);
        Gauge.builder("neo4j_ha_backup_last_success_timestamp", lastBackupTimestamp, AtomicLong::get)
            .description("Last successful backup timestamp").register(registry);

        // Buffer
        Gauge.builder("neo4j_ha_buffer_size", bufferSize, AtomicLong::get)
            .description("Publish buffer size").register(registry);

        // Stream retention / maintenance (BUG-038). The cutoff gauge tracks the
        // wall-clock time (ms) below which stream entries were last trimmed. A
        // ~0 value means the maintenance task either hasn't run yet, is disabled,
        // or found nothing to trim.
        Gauge.builder("neo4j_ha_stream_retention_cutoff_ms", streamRetentionCutoffMs, AtomicLong::get)
            .description("Most recent XTRIM MINID cutoff applied by the stream maintenance task")
            .register(registry);
        streamRetentionTrimmedTotal = Counter.builder("neo4j_ha_stream_retention_trimmed_total")
            .description("Total stream entries removed by the safe XTRIM maintenance task")
            .register(registry);
        pelTrimmedEventsTotal = Counter.builder("neo4j_ha_pel_trimmed_events_total")
            .description("Events found to have been trimmed from the stream while still "
                + "referenced by a consumer group's PEL. Non-zero indicates a standby "
                + "that fell behind the retention window — manual fullsync required.")
            .register(registry);
        pelRecoveryTrimDetections = Counter.builder("neo4j_ha_pel_recovery_trim_detections_total")
            .description("Times PendingRecovery detected trimmed PEL messages across all "
                + "standby startups. A sustained non-zero trend suggests tightening "
                + "stream retention or increasing consumer throughput.")
            .register(registry);

        // Auto-fullsync rate-limit counter. A non-zero value means the old-primary
        // recovery path wanted to start a fullsync but was suppressed because one
        // already ran recently — typically a flapping node. Operator can force the
        // sync via /cluster/fullsync if truly needed.
        autoFullsyncSuppressedTotal = Counter.builder("neo4j_ha_auto_fullsync_suppressed_total")
            .description("Auto full-sync attempts suppressed by rate-limit in "
                + "OldPrimaryRecovery to avoid flap-induced recovery storms (BUG-039)")
            .register(registry);

        autoFullsyncFailedTotal = Counter.builder("neo4j_ha_auto_fullsync_failed_total")
            .description("Auto full-sync export tasks that failed inside the "
                + "OldPrimaryRecovery executor. Non-zero means a standby is "
                + "likely still SYNCING — inspect agent logs (BUG-051 follow-up).")
            .register(registry);

        batchSplitForDuplicateElementIdTotal = Counter.builder(
                "neo4j_ha_batch_split_for_duplicate_elementid_total")
            .description("Times ChangeApplier.applyBatch split an incoming batch "
                + "because two events in it shared the same _elementId "
                + "(Neo4j internal rel-id reuse after DELETE — BUG-081). "
                + "Each increment = one split point, not one whole batch; a "
                + "single batch with 3 duplicates contributes 2 to this counter.")
            .register(registry);

        Gauge.builder("neo4j_ha_dup_element_id_nodes", dupElementIdNodes, AtomicLong::get)
            .description("Number of nodes on the local primary whose `_elementId` "
                + "PROPERTY value is shared by at least one other live node "
                + "(BUG-083). Sampled by IndexInstaller.ensureIndexes; healthy "
                + "steady state is 0. Non-zero means the UNIQUE constraint is "
                + "missing or was bypassed — run "
                + "scripts/deploy/elementid-dedup.sh and restart the agent.")
            .register(registry);

        // Admin UI metrics
        uiLoginSuccess = Counter.builder("ha_ui_login_total")
            .tag("result", "success")
            .description("Admin UI login outcomes").register(registry);
        uiLoginFailure = Counter.builder("ha_ui_login_total")
            .tag("result", "failure")
            .description("Admin UI login outcomes").register(registry);
        uiLoginLocked = Counter.builder("ha_ui_login_total")
            .tag("result", "locked")
            .description("Admin UI login outcomes").register(registry);
        Gauge.builder("ha_ui_session_active", uiSessionActive, AtomicLong::get)
            .description("Currently active admin UI sessions").register(registry);
    }

    public void recordUiApiRequest(String path, String method, int status, long durationMs) {
        Counter.builder("ha_ui_api_requests_total")
            .tag("path", path).tag("method", method).tag("status", String.valueOf(status))
            .description("Admin UI / admin API request count")
            .register(registry)
            .increment();
        Timer.builder("ha_ui_api_duration_seconds")
            .tag("path", path)
            .description("Admin UI / admin API request latency")
            .register(registry)
            .record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordFailover(boolean success, long durationMs) {
        failoverTotal.increment();
        if (success) {
            failoverSuccess.increment();
        } else {
            failoverFailed.increment();
        }
        // Previously the duration was never recorded — the Timer always reported 0 samples.
        failoverDuration.record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordHaproxyReachable(String instanceId, boolean reachable) {
        if (!reachable) {
            haproxyUpdateErrors.increment();
        }
        // Per-instance reachability gauge (1 reachable, 0 unreachable); creates a tagged
        // metric so alerts can target a specific HAProxy.
        registry.gauge("neo4j_ha_haproxy_instance_reachable",
            Tags.of("instance", instanceId), reachable ? 1 : 0);
    }

    public void recordHaproxyStateSync(String instanceId) {
        Counter.builder("neo4j_ha_haproxy_state_sync_total")
            .tag("instance", instanceId)
            .description("HAProxy state sync executions")
            .register(registry)
            .increment();
    }

    public void recordHaproxyStateSyncFix(String instanceId) {
        Counter.builder("neo4j_ha_haproxy_state_sync_fix_total")
            .tag("instance", instanceId)
            .description("HAProxy state sync corrections")
            .register(registry)
            .increment();
    }

    public void recordOldPrimaryCleanupEvents(long count) {
        oldPrimaryCleanupEvents.set(count);
    }
}
