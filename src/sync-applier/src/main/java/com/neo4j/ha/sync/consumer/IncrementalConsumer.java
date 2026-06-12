package com.neo4j.ha.sync.consumer;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.ChangeEventType;
import com.neo4j.ha.common.model.SyncMode;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.common.redis.StreamConsumer;
import com.neo4j.ha.common.serialization.EventDeserializer;
import com.neo4j.ha.sync.SyncApplierConfig;
import com.neo4j.ha.sync.applier.ChangeApplier;
import com.neo4j.ha.sync.validation.DuplicateDetector;
import com.neo4j.ha.sync.validation.FencingTokenFilter;
import com.neo4j.ha.sync.validation.OrderValidator;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IncrementalConsumer {

    private static final Logger log = LoggerFactory.getLogger(IncrementalConsumer.class);

    private final StreamConsumer streamConsumer;
    private final EventDeserializer deserializer;
    private final ChangeApplier changeApplier;
    private final FencingTokenFilter fencingTokenFilter;
    private final DuplicateDetector duplicateDetector;
    private final OrderValidator orderValidator;
    private final CheckpointManager checkpointManager;
    private final HaMetrics metrics;
    private final SyncApplierConfig config;

    public IncrementalConsumer(StreamConsumer streamConsumer, EventDeserializer deserializer,
                                ChangeApplier changeApplier, FencingTokenFilter fencingTokenFilter,
                                DuplicateDetector duplicateDetector, OrderValidator orderValidator,
                                CheckpointManager checkpointManager, HaMetrics metrics,
                                SyncApplierConfig config) {
        this.streamConsumer = streamConsumer;
        this.deserializer = deserializer;
        this.changeApplier = changeApplier;
        this.fencingTokenFilter = fencingTokenFilter;
        this.duplicateDetector = duplicateDetector;
        this.orderValidator = orderValidator;
        this.checkpointManager = checkpointManager;
        this.metrics = metrics;
        this.config = config;
    }

    public interface FullSyncCallback {
        void onFullSyncStart(ChangeEvent event);
        void onFullSyncEnd(ChangeEvent event);
    }

    public void consumeOnce(Driver standbyDriver, String nodeId, String consumerGroup,
                            FullSyncCallback fullSyncCallback) {
        var results = streamConsumer.consume(
            config.changesStreamKey(), consumerGroup,
            nodeId, config.consumerBatchSize(), config.blockTimeoutMs()
        );

        if (results.isEmpty()) return;

        for (var entry : results) {
            List<StreamEntry> streamEntries = entry.getValue();
            if (streamEntries.isEmpty()) continue;

            List<ChangeEvent> events = new ArrayList<>();
            List<String> messageIds = new ArrayList<>();

            for (StreamEntry se : streamEntries) {
                ChangeEvent event = deserializer.fromMap(se.getFields());
                messageIds.add(se.getID().toString());

                // Handle control events
                if (event.eventType() == ChangeEventType.FULL_SYNC_START) {
                    fullSyncCallback.onFullSyncStart(event);
                    streamConsumer.ack(config.changesStreamKey(), consumerGroup,
                        se.getID().toString());
                    continue;
                }
                if (event.eventType() == ChangeEventType.FULL_SYNC_END) {
                    fullSyncCallback.onFullSyncEnd(event);
                    streamConsumer.ack(config.changesStreamKey(), consumerGroup,
                        se.getID().toString());
                    continue;
                }

                // Skip duplicates
                if (duplicateDetector.isDuplicate(event.eventId())) {
                    continue;
                }
                events.add(event);
            }

            // Filter by fencing token
            events = fencingTokenFilter.filter(events);
            orderValidator.validate(events);

            if (!events.isEmpty()) {
                // Apply batch
                changeApplier.applyBatch(events, standbyDriver);

                // Mark processed
                for (ChangeEvent event : events) {
                    duplicateDetector.mark(event.eventId());
                }

                // Per-node lag is tracked via checkpoint; global metric updated by SyncApplier
            }

            // ACK all messages
            streamConsumer.ack(config.changesStreamKey(), consumerGroup,
                messageIds.toArray(new String[0]));

            // Save checkpoint
            if (!messageIds.isEmpty()) {
                String lastStreamId = messageIds.get(messageIds.size() - 1);
                long lastEventTs = events.isEmpty() ? System.currentTimeMillis()
                    : events.get(events.size() - 1).timestamp();
                checkpointManager.saveSyncCheckpoint(nodeId, lastStreamId, lastEventTs,
                    SyncMode.INCREMENTAL);
            }
        }
    }
}
