package com.neo4j.ha.agent.failover;

import com.neo4j.ha.sync.SyncApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrainWaiter {

    private static final Logger log = LoggerFactory.getLogger(DrainWaiter.class);

    private final SyncApplier syncApplier;
    private final long drainTimeoutMs;

    public DrainWaiter(SyncApplier syncApplier, long drainTimeoutMs) {
        this.syncApplier = syncApplier;
        this.drainTimeoutMs = drainTimeoutMs;
    }

    public void waitForDrain() {
        log.info("Waiting for Sync Applier to drain pending messages (timeout: {}ms)", drainTimeoutMs);
        syncApplier.drainPending();
        log.info("Drain complete");
    }
}
