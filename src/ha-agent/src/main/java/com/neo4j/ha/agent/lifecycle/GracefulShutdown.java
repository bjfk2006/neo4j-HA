package com.neo4j.ha.agent.lifecycle;

import com.neo4j.ha.agent.failover.FailoverOrchestrator;
import com.neo4j.ha.agent.recovery.OldPrimaryRecovery;
import com.neo4j.ha.cdc.CdcCollector;
import com.neo4j.ha.common.neo4j.Neo4jClientFactory;
import com.neo4j.ha.common.redis.RedisClientFactory;
import com.neo4j.ha.sync.SyncApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulShutdown implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdown.class);

    private final CdcCollector cdcCollector;
    private final SyncApplier syncApplier;
    private final OldPrimaryRecovery oldPrimaryRecovery;
    private final Neo4jClientFactory neo4jClientFactory;
    private final RedisClientFactory redisClientFactory;
    private final FailoverOrchestrator failoverOrchestrator;

    /**
     * How long to wait for an in-flight failover/switchover to finish before
     * tearing the agent down. Long enough to cover a normal switch (trigger
     * install + probes + reconcile), short enough not to hang a container stop.
     */
    private static final long SWITCH_DRAIN_TIMEOUT_MS = 90_000L;
    private static final long SWITCH_POLL_MS = 250L;

    public GracefulShutdown(CdcCollector cdcCollector, SyncApplier syncApplier,
                             OldPrimaryRecovery oldPrimaryRecovery,
                             Neo4jClientFactory neo4jClientFactory,
                             RedisClientFactory redisClientFactory) {
        this(cdcCollector, syncApplier, oldPrimaryRecovery, neo4jClientFactory,
             redisClientFactory, null);
    }

    public GracefulShutdown(CdcCollector cdcCollector, SyncApplier syncApplier,
                             OldPrimaryRecovery oldPrimaryRecovery,
                             Neo4jClientFactory neo4jClientFactory,
                             RedisClientFactory redisClientFactory,
                             FailoverOrchestrator failoverOrchestrator) {
        this.cdcCollector = cdcCollector;
        this.syncApplier = syncApplier;
        this.oldPrimaryRecovery = oldPrimaryRecovery;
        this.neo4jClientFactory = neo4jClientFactory;
        this.redisClientFactory = redisClientFactory;
        this.failoverOrchestrator = failoverOrchestrator;
    }

    /**
     * REVIEW-S4: wait for any running switch to finish before tearing anything
     * down.
     *
     * <p>A switch blocks every write in HAProxy as its first step and unblocks
     * them as its last. If the agent is stopped in between — a plain
     * {@code docker restart ha-agent} during a switchover is enough — the
     * shutdown hook used to close the Neo4j drivers and the Redis pool out from
     * under the switch thread, which then died mid-phase. Nothing else in the
     * system ever calls {@code unblockWrites}, and the HAProxy reconciler that
     * might have papered over it died with the agent: the cluster stays
     * permanently read-only until a human notices.</p>
     */
    private void awaitSwitchCompletion() {
        if (failoverOrchestrator == null) return;
        if (!failoverOrchestrator.isSwitchInProgress()) return;

        String op = failoverOrchestrator.getActiveOperation();
        log.warn("Shutdown requested while {} is running. Waiting up to {}ms for it to finish — "
            + "killing it now would leave HAProxy write-blocked with nothing to undo it.",
            op, SWITCH_DRAIN_TIMEOUT_MS);

        long deadline = System.currentTimeMillis() + SWITCH_DRAIN_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (!failoverOrchestrator.isSwitchInProgress()) {
                log.info("In-flight switch finished; continuing shutdown");
                return;
            }
            try {
                Thread.sleep(SWITCH_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.error("In-flight switch ({}) did NOT finish within {}ms. Proceeding with shutdown — "
            + "CHECK HAPROXY: the write backend may still be fully in maint. If so, "
            + "`set server <backend>/<primary> state ready` on each HAProxy admin socket.",
            op, SWITCH_DRAIN_TIMEOUT_MS);
    }

    @Override
    public void run() {
        log.info("Graceful shutdown initiated...");
        try {
            // REVIEW-S4: never tear down under a running switch.
            awaitSwitchCompletion();

            // Stop CDC Collector
            if (cdcCollector != null && cdcCollector.isRunning()) {
                log.info("Stopping CDC Collector...");
                cdcCollector.stop();
            }

            // Stop Sync Applier
            if (syncApplier != null && syncApplier.isRunning()) {
                log.info("Stopping Sync Applier...");
                syncApplier.stop();
            }

            // Drain in-flight fullsync exports (BUG-051 follow-up M1).
            // Done before closing Neo4j/Redis so the export task can still finish
            // its FULL_SYNC_END publish if it's near completion.
            if (oldPrimaryRecovery != null) {
                log.info("Draining old-primary recovery fullsync executor...");
                oldPrimaryRecovery.shutdown();
            }

            // Close Neo4j drivers
            log.info("Closing Neo4j connections...");
            neo4jClientFactory.closeAll();

            // Close Redis pool
            log.info("Closing Redis connection...");
            redisClientFactory.close();

            log.info("Graceful shutdown complete");
        } catch (Exception e) {
            log.error("Error during graceful shutdown", e);
        }
    }
}
