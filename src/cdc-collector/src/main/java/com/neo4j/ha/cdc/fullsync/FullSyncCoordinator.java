package com.neo4j.ha.cdc.fullsync;

import com.neo4j.ha.common.model.*;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.util.IdGenerator;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class FullSyncCoordinator {

    private static final Logger log = LoggerFactory.getLogger(FullSyncCoordinator.class);

    private final Driver primaryDriver;
    private final StreamPublisher streamPublisher;
    private final String database;
    private final String fullsyncStreamKey;
    private final String changesStreamKey;
    private final int batchSize;
    private final int throttleMs;
    private final long fencingToken;

    private final NodeExporter nodeExporter = new NodeExporter();
    private final RelationshipExporter relExporter = new RelationshipExporter();

    public FullSyncCoordinator(Driver primaryDriver, StreamPublisher streamPublisher,
                                String database, String fullsyncStreamKey,
                                String changesStreamKey, int batchSize,
                                int throttleMs, long fencingToken) {
        this.primaryDriver = primaryDriver;
        this.streamPublisher = streamPublisher;
        this.database = database;
        this.fullsyncStreamKey = fullsyncStreamKey;
        this.changesStreamKey = changesStreamKey;
        this.batchSize = batchSize;
        this.throttleMs = throttleMs;
        this.fencingToken = fencingToken;
    }

    public void startFullSync(String targetNodeId) {
        log.info("Starting full sync for target node: {}", targetNodeId);
        long snapshotTs = System.currentTimeMillis();

        // Publish FULL_SYNC_START control event
        publishControlEvent(ChangeEventType.FULL_SYNC_START, targetNodeId, snapshotTs);

        try (Session session = primaryDriver.session(SessionConfig.forDatabase(database))) {
            // Export all nodes
            int nodeCount = nodeExporter.export(session, streamPublisher, fullsyncStreamKey,
                batchSize, throttleMs);

            // Export all relationships
            int relCount = relExporter.export(session, streamPublisher, fullsyncStreamKey,
                batchSize, throttleMs);

            log.info("Full sync export complete: {} nodes, {} relationships", nodeCount, relCount);
        }

        // BUG-084 follow-up (BUG-085): publish a SENTINEL batch to the fullsync
        // stream as an explicit "publisher done" marker. Without this, the
        // consumer's only completion signal is batchIndex+1==totalBatches per
        // type — but if the stream still carries residual batches from a
        // PREVIOUS fullsync (because XGROUP wasn't recreated, or because
        // ACK state and stream content diverged), the consumer can see two
        // separate "NODE 7/7" or "REL 23/23" events and mis-classify which
        // one is the real terminator. The sentinel sidesteps the entire
        // counter-based protocol: consumer breaks the moment it sees
        // batchIndex == SENTINEL_INDEX and totalBatches == 0, regardless of
        // what came before. EntityType=NODE on the sentinel is arbitrary —
        // consumer ignores it.
        FullSyncBatch sentinel = new FullSyncBatch(
            IdGenerator.uuidV7(),
            SENTINEL_BATCH_INDEX, 0,
            EntityType.NODE, Collections.emptyList(),
            System.currentTimeMillis()
        );
        streamPublisher.publishFullSyncBatch(fullsyncStreamKey, sentinel);
        log.info("Full sync sentinel published; consumer will exit on receipt");

        // Publish FULL_SYNC_END control event
        publishControlEvent(ChangeEventType.FULL_SYNC_END, targetNodeId, snapshotTs);

        log.info("Full sync completed for target node: {}", targetNodeId);
    }

    /**
     * Sentinel value indicating "publisher has finished — no more real
     * batches will follow". Consumer recognizes this and exits the consume
     * loop. Chosen as -1 because legitimate batches have non-negative
     * indices.
     */
    public static final int SENTINEL_BATCH_INDEX = -1;

    private void publishControlEvent(ChangeEventType type, String targetNodeId, long snapshotTs) {
        ChangeEvent event = new ChangeEvent(
            IdGenerator.uuidV7(),
            type,
            database,
            System.currentTimeMillis(),
            fencingToken,
            null,
            new EntityData(EntityType.NODE, targetNodeId, Collections.emptyList(),
                Map.of("snapshotTs", snapshotTs), null, null, null, null),
            null
        );
        streamPublisher.publish(changesStreamKey, event, fencingToken);
    }
}
