package com.neo4j.ha.agent.lifecycle;

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

    public GracefulShutdown(CdcCollector cdcCollector, SyncApplier syncApplier,
                             OldPrimaryRecovery oldPrimaryRecovery,
                             Neo4jClientFactory neo4jClientFactory,
                             RedisClientFactory redisClientFactory) {
        this.cdcCollector = cdcCollector;
        this.syncApplier = syncApplier;
        this.oldPrimaryRecovery = oldPrimaryRecovery;
        this.neo4jClientFactory = neo4jClientFactory;
        this.redisClientFactory = redisClientFactory;
    }

    @Override
    public void run() {
        log.info("Graceful shutdown initiated...");
        try {
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
