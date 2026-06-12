package com.neo4j.ha.sync.fullsync;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.sync.consumer.FullSyncConsumer;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullSyncReceiver {

    private static final Logger log = LoggerFactory.getLogger(FullSyncReceiver.class);

    public enum State {
        IDLE, PREPARING, RECEIVING, CATCHING_UP
    }

    public interface CatchUpCallback {
        void onCatchUpComplete(String nodeId);
    }

    private volatile State state = State.IDLE;
    private final DatabaseCleaner databaseCleaner;
    private final FullSyncConsumer fullSyncConsumer;
    private final String database;
    private long snapshotTs;
    private volatile CatchUpCallback catchUpCallback;

    /**
     * Wall-clock ms of the most recent successful CATCHING_UP → IDLE transition.
     * Used by {@link #onFullSyncEnd} to tell apart "FULL_SYNC_END arrived after
     * a fast catch-up already finished" (normal — state==IDLE is expected) from
     * "FULL_SYNC_END arrived in IDLE without any fullsync having just completed"
     * (anomaly — likely a fullsync that failed to receive). 0 means never.
     */
    private volatile long lastFastCatchUpAtMs = 0L;

    /** Catch-up that lands within this window of FULL_SYNC_END counts as fast-path. */
    private static final long FAST_CATCH_UP_WINDOW_MS = 5_000L;

    public FullSyncReceiver(DatabaseCleaner databaseCleaner, FullSyncConsumer fullSyncConsumer,
                             String database) {
        this.databaseCleaner = databaseCleaner;
        this.fullSyncConsumer = fullSyncConsumer;
        this.database = database;
    }

    public void setCatchUpCallback(CatchUpCallback callback) {
        this.catchUpCallback = callback;
    }

    public void onFullSyncStart(ChangeEvent event, Driver standbyDriver, String nodeId) {
        if (state != State.IDLE && state != State.CATCHING_UP) {
            log.warn("Full sync start received in unexpected state: {}, resetting", state);
        }
        log.info("Full sync start received for node {}", nodeId);
        state = State.PREPARING;

        if (event.entity() != null && event.entity().properties() != null) {
            Object ts = event.entity().properties().get("snapshotTs");
            if (ts instanceof Number n) {
                this.snapshotTs = n.longValue();
            }
        }

        databaseCleaner.clean(standbyDriver, database);
        state = State.RECEIVING;

        // BUG-085: pass snapshotTs so the consumer can filter out batches
        // left over from a previous (e.g. crashed) fullsync run that share
        // the same consumer group but were published before this run started.
        boolean completed = fullSyncConsumer.consumeFullSyncBatches(
            standbyDriver, database, nodeId, snapshotTs);
        if (completed) {
            state = State.CATCHING_UP;
            log.info("Full sync receiving complete for node {}, entering CATCHING_UP (snapshotTs={})",
                nodeId, snapshotTs);
        } else {
            log.error("Full sync failed for node {}, returning to IDLE", nodeId);
            state = State.IDLE;
        }
    }

    public void onFullSyncEnd(ChangeEvent event) {
        if (state == State.RECEIVING) {
            state = State.CATCHING_UP;
            log.info("Full sync end received, switching to CATCHING_UP mode");
        } else if (state == State.CATCHING_UP) {
            log.info("Full sync end received while already in CATCHING_UP");
        } else if (state == State.IDLE
                && (System.currentTimeMillis() - lastFastCatchUpAtMs) < FAST_CATCH_UP_WINDOW_MS) {
            // Fast catch-up already returned to IDLE before FULL_SYNC_END
            // landed on the changes stream — normal path, not an anomaly.
            log.info("Full sync end received after fast catch-up (state=IDLE) — normal");
        } else {
            log.warn("Full sync end received in unexpected state: {} "
                   + "(no recent fast catch-up; possible fullsync failure)", state);
        }
    }

    /**
     * Called when incremental sync has caught up to snapshotTs.
     * Transitions from CATCHING_UP → IDLE.
     */
    public void onCatchUpComplete(String nodeId) {
        if (state != State.CATCHING_UP) {
            log.warn("Catch-up complete called in unexpected state: {}", state);
            return;
        }
        state = State.IDLE;
        lastFastCatchUpAtMs = System.currentTimeMillis();
        log.info("Catch-up complete for node {}, returning to IDLE", nodeId);
        if (catchUpCallback != null) {
            catchUpCallback.onCatchUpComplete(nodeId);
        }
    }

    /**
     * Checks if incremental events have caught up past the snapshot timestamp.
     * If so, completes the catch-up automatically.
     */
    public void checkCatchUp(String nodeId, long lastAppliedTs) {
        if (state == State.CATCHING_UP && snapshotTs > 0 && lastAppliedTs >= snapshotTs) {
            onCatchUpComplete(nodeId);
        }
    }

    public State getState() {
        return state;
    }

    public long getSnapshotTs() {
        return snapshotTs;
    }

    public boolean isReceiving() {
        return state == State.RECEIVING || state == State.PREPARING;
    }

    public boolean isCatchingUp() {
        return state == State.CATCHING_UP;
    }
}
