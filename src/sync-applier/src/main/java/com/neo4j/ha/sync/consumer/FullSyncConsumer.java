package com.neo4j.ha.sync.consumer;

import com.neo4j.ha.common.model.EntityType;
import com.neo4j.ha.common.model.FullSyncBatch;
import com.neo4j.ha.common.redis.StreamConsumer;
import com.neo4j.ha.common.serialization.EventDeserializer;
import com.neo4j.ha.sync.fullsync.BulkImporter;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;

public class FullSyncConsumer {

    private static final Logger log = LoggerFactory.getLogger(FullSyncConsumer.class);

    private final StreamConsumer streamConsumer;
    private final EventDeserializer deserializer;
    private final BulkImporter bulkImporter;
    private final String fullsyncStreamKey;
    private final String consumerGroup;

    public FullSyncConsumer(StreamConsumer streamConsumer, EventDeserializer deserializer,
                             BulkImporter bulkImporter, String fullsyncStreamKey,
                             String consumerGroup) {
        this.streamConsumer = streamConsumer;
        this.deserializer = deserializer;
        this.bulkImporter = bulkImporter;
        this.fullsyncStreamKey = fullsyncStreamKey;
        this.consumerGroup = consumerGroup;
    }

    private static final int MAX_CONSECUTIVE_EMPTY_READS = 30;

    /**
     * Grace window: after the node phase completes (last node batch arrived),
     * how long to wait for the relationship phase to start before assuming
     * the graph has no relationships and finishing the full sync.
     *
     * <p>Without this, an empty-relationship graph would never trigger the
     * RELATIONSHIP exit condition and the consumer would wait
     * {@code MAX_CONSECUTIVE_EMPTY_READS * blockTimeout} (~60s) before
     * giving up — wasteful but not incorrect. 5s is comfortably larger than
     * the publisher's between-phase gap (sub-second in practice).</p>
     */
    private static final long REL_PHASE_GRACE_MS = 5_000L;

    /**
     * Sentinel batchIndex marker. Mirrors {@code FullSyncCoordinator.SENTINEL_BATCH_INDEX}.
     * The publisher emits exactly one batch with this index after the NODE +
     * RELATIONSHIP phases complete; the consumer breaks the moment it sees
     * this sentinel, regardless of what other (possibly stale) batches sit
     * in the stream from a previous fullsync run.
     */
    private static final int SENTINEL_BATCH_INDEX = -1;

    /**
     * Consumes full sync batches until both phases (NODE + RELATIONSHIP) have
     * delivered their final batch, or a maximum number of consecutive empty
     * reads is reached.
     *
     * <p>BUG-084 fix: the previous implementation exited on the FIRST batch
     * whose {@code batchIndex+1 == totalBatches}. Because NODE and
     * RELATIONSHIP batches each carry their OWN {@code totalBatches} count
     * (e.g. nodes:7, rels:23) and are published into a single shared stream,
     * the NODE 7/7 batch triggered "completed=true" and the consumer
     * abandoned the loop while ~95% of the rel batches were still queued.
     * Symptom: standby had 100% of nodes but only ~13% of relationships
     * after a fresh fullsync.</p>
     *
     * <p>The fixed exit condition is "NODE-last AND REL-last both seen",
     * with a 5s grace fallback to handle the legitimate edge case of a
     * graph containing zero relationships.</p>
     *
     * @return true if full sync completed successfully, false if timed out
     */
    /** Backward-compatible overload: no snapshotTs filtering. */
    public boolean consumeFullSyncBatches(Driver standbyDriver, String database, String nodeId) {
        return consumeFullSyncBatches(standbyDriver, database, nodeId, 0L);
    }

    /**
     * @param snapshotTs  fullsync start time as published by FullSyncCoordinator
     *                    in {@code FULL_SYNC_START.entity.properties.snapshotTs}.
     *                    Batches with {@code batch.timestamp() < snapshotTs} are
     *                    skipped (ACKed without import) — they are leftovers
     *                    from a previous fullsync run that share the same
     *                    consumer group. 0 disables filtering.
     */
    public boolean consumeFullSyncBatches(Driver standbyDriver, String database,
                                          String nodeId, long snapshotTs) {
        streamConsumer.ensureGroup(fullsyncStreamKey, consumerGroup + "-fullsync");

        int consecutiveEmpty = 0;
        boolean nodePhaseDone = false;
        boolean relPhaseDone = false;
        long nodePhaseDoneAtMs = 0L;
        boolean completed = false;

        while (!completed && consecutiveEmpty < MAX_CONSECUTIVE_EMPTY_READS) {
            var results = streamConsumer.consume(
                fullsyncStreamKey, consumerGroup + "-fullsync",
                nodeId, 10, 2000
            );

            if (results.isEmpty()) {
                consecutiveEmpty++;
                // Empty-graph edge: node phase finished but no rel batch ever
                // arrived. Wait `REL_PHASE_GRACE_MS` of silence then conclude
                // there are no rels (rather than time out at 60s).
                if (nodePhaseDone && !relPhaseDone
                        && (System.currentTimeMillis() - nodePhaseDoneAtMs) > REL_PHASE_GRACE_MS) {
                    log.info("Full sync: node phase complete + {}ms of silence; "
                           + "assuming graph has no relationships",
                        REL_PHASE_GRACE_MS);
                    completed = true;
                }
                continue;
            }
            consecutiveEmpty = 0;

            for (var entry : results) {
                for (StreamEntry se : entry.getValue()) {
                    FullSyncBatch batch = deserializer.fullSyncBatchFromMap(se.getFields());

                    // BUG-085 fix: sentinel batch = "publisher has finished".
                    // Always ACK + skip (do NOT import the empty entities list),
                    // then mark completed and break out of both inner+outer for.
                    if (batch.batchIndex() == SENTINEL_BATCH_INDEX) {
                        streamConsumer.ack(fullsyncStreamKey,
                            consumerGroup + "-fullsync", se.getID().toString());
                        log.info("Full sync: SENTINEL received — publisher done. "
                               + "Exiting consumer loop "
                               + "(nodePhaseDone={}, relPhaseDone={})",
                            nodePhaseDone, relPhaseDone);
                        completed = true;
                        break;
                    }

                    // BUG-085: stale batch from a previous fullsync run.
                    // Same consumer group sees them because either the group
                    // wasn't cleaned up properly, or stream maintenance hasn't
                    // trimmed them yet. ACK + skip — no point importing rels
                    // whose endpoint nodes will get wiped/replaced by the
                    // current run's NODE phase anyway.
                    if (snapshotTs > 0 && batch.timestamp() < snapshotTs) {
                        streamConsumer.ack(fullsyncStreamKey,
                            consumerGroup + "-fullsync", se.getID().toString());
                        log.info("Full sync: skipping stale batch {}/{} (type={}, "
                               + "ts={} < snapshotTs={}) — leftover from previous run",
                            batch.batchIndex() + 1, batch.totalBatches(),
                            batch.entityType(), batch.timestamp(), snapshotTs);
                        continue;
                    }

                    try (Session session = standbyDriver.session(SessionConfig.forDatabase(database))) {
                        bulkImporter.importBatch(session, batch);
                    }

                    streamConsumer.ack(fullsyncStreamKey, consumerGroup + "-fullsync",
                        se.getID().toString());

                    log.info("Full sync batch {}/{} imported ({} entities, type={})",
                        batch.batchIndex() + 1, batch.totalBatches(),
                        batch.entities().size(), batch.entityType());

                    if (batch.batchIndex() + 1 >= batch.totalBatches()) {
                        if (batch.entityType() == EntityType.NODE) {
                            nodePhaseDone = true;
                            nodePhaseDoneAtMs = System.currentTimeMillis();
                            log.info("Full sync: NODE phase complete ({} batches)",
                                batch.totalBatches());
                        } else if (batch.entityType() == EntityType.RELATIONSHIP) {
                            relPhaseDone = true;
                            log.info("Full sync: RELATIONSHIP phase complete ({} batches)",
                                batch.totalBatches());
                        }
                    }
                }
                if (completed) break;
            }
        }

        if (!completed) {
            log.warn("Full sync timed out after {} consecutive empty reads "
                   + "(nodePhaseDone={}, relPhaseDone={})",
                MAX_CONSECUTIVE_EMPTY_READS, nodePhaseDone, relPhaseDone);
        }
        return completed;
    }
}
