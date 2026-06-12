package com.neo4j.ha.sync.consumer;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.redis.StreamConsumer;
import com.neo4j.ha.common.serialization.EventDeserializer;
import com.neo4j.ha.sync.applier.ChangeApplier;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Re-applies messages in a standby consumer group's PEL (delivered, not yet ACK'd).
 *
 * <p>BUG-038 hardening: when a message has been trimmed out of the stream while still
 * referenced by the PEL — this can happen if a standby fell behind further than the
 * retention window — Redis returns the PEL entry with an empty / null fields map. The
 * original implementation silently passed that to the deserializer, which either crashed
 * or produced a bogus {@link ChangeEvent}, then ACK'd the zombie id. Now we:
 *
 * <ul>
 *   <li>Detect the trimmed-payload case and <b>do not apply</b> the event</li>
 *   <li>ACK the zombie id so the PEL entry is cleared (the message body is unrecoverable
 *       anyway and leaving it in the PEL would block later maintenance)</li>
 *   <li>Increment observability counters ({@code neo4j_ha_pel_trimmed_events_total} and
 *       {@code neo4j_ha_pel_recovery_trim_detections_total})</li>
 *   <li>Log an ERROR that names the stream, group, and count so an operator can decide
 *       whether to trigger a manual fullsync</li>
 * </ul>
 *
 * <p>Deliberately we do <b>NOT</b> trigger a fullsync automatically. Doing so under
 * sustained backpressure would create a cluster-wide amplification loop (standby falls
 * behind → fullsync → primary IO/CPU spike → other standbys fall behind → cascade).
 * The operator decides whether the missing events matter and issues a fullsync via
 * {@code POST /cluster/fullsync?targetNodeId=...} if required.
 */
public class PendingRecovery {

    private static final Logger log = LoggerFactory.getLogger(PendingRecovery.class);

    private final StreamConsumer streamConsumer;
    private final EventDeserializer deserializer;
    private final ChangeApplier changeApplier;
    private final String streamKey;
    private final String groupName;
    private final String consumerName;
    private final HaMetrics metrics;

    public PendingRecovery(StreamConsumer streamConsumer, EventDeserializer deserializer,
                            ChangeApplier changeApplier, String streamKey,
                            String groupName, String consumerName) {
        this(streamConsumer, deserializer, changeApplier, streamKey, groupName, consumerName, null);
    }

    public PendingRecovery(StreamConsumer streamConsumer, EventDeserializer deserializer,
                            ChangeApplier changeApplier, String streamKey,
                            String groupName, String consumerName, HaMetrics metrics) {
        this.streamConsumer = streamConsumer;
        this.deserializer = deserializer;
        this.changeApplier = changeApplier;
        this.streamKey = streamKey;
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.metrics = metrics;
    }

    public int recover(Driver standbyDriver) {
        log.info("Starting PEL recovery for group={}, consumer={}", groupName, consumerName);
        int recovered = 0;
        long trimmedInThisRun = 0;

        List<StreamEntry> pending = streamConsumer.readPending(streamKey, groupName, consumerName, 100);
        // BUG-081 DIAG (2026-04-22): track per-batch membership so we can tell whether
        // two CREATE events that share the same `_elementId` (Neo4j internal rel-id
        // reuse after DELETE) land in the same bolt tx. Emits one INFO line per batch
        // summarising size + stream-id range + a compact list of `<streamId>=<eventType>[<elementId>]`.
        // Remove once BUG-081 root cause is confirmed (expected lifetime: ≤1 chaos cycle).
        int batchIdx = 0;
        while (!pending.isEmpty()) {
            List<ChangeEvent> events = new ArrayList<>();
            List<String> applicableIds = new ArrayList<>();
            List<String> trimmedIds = new ArrayList<>();

            for (StreamEntry entry : pending) {
                Map<String, String> fields = entry.getFields();
                if (isTrimmedPayload(fields)) {
                    trimmedIds.add(entry.getID().toString());
                    continue;
                }
                try {
                    events.add(deserializer.fromMap(fields));
                    applicableIds.add(entry.getID().toString());
                } catch (Exception e) {
                    // Treat deserialisation failure on PEL recovery as if the payload had
                    // been trimmed — the entry is unusable. This also prevents a poison
                    // message from blocking all future PEL recovery.
                    log.warn("PEL entry {} failed to deserialize, treating as trimmed: {}",
                        entry.getID(), e.toString());
                    trimmedIds.add(entry.getID().toString());
                }
            }

            if (!applicableIds.isEmpty()) {
                batchIdx++;
                logBatchForBug081(batchIdx, events, applicableIds);
                changeApplier.applyBatch(events, standbyDriver);
                streamConsumer.ack(streamKey, groupName,
                    applicableIds.toArray(new String[0]));
                recovered += events.size();
            }

            if (!trimmedIds.isEmpty()) {
                // ACK the zombie PEL entries so they don't block future maintenance /
                // XCLAIM scans. The message bodies are gone from the stream already.
                streamConsumer.ack(streamKey, groupName,
                    trimmedIds.toArray(new String[0]));
                trimmedInThisRun += trimmedIds.size();
                log.error(
                    "PEL recovery detected {} TRIMMED message(s) in group={}, consumer={} on stream={}. "
                    + "These events were purged from the stream before this standby could consume them. "
                    + "Data integrity may be broken on this node — run manual fullsync via "
                    + "POST /cluster/fullsync?targetNodeId=<this-node> to recover.",
                    trimmedIds.size(), groupName, consumerName, streamKey);
                if (metrics != null) {
                    metrics.pelTrimmedEventsTotal.increment(trimmedIds.size());
                    metrics.pelRecoveryTrimDetections.increment();
                }
            }

            pending = streamConsumer.readPending(streamKey, groupName, consumerName, 100);
        }

        if (trimmedInThisRun > 0) {
            log.error("PEL recovery complete: recovered={} applied, trimmed={} lost. "
                + "Operator intervention required for group={} (consumer={}).",
                recovered, trimmedInThisRun, groupName, consumerName);
        } else {
            log.info("PEL recovery complete: recovered {} messages", recovered);
        }
        return recovered;
    }

    /** A trimmed PEL entry is returned by Redis with null/empty fields. */
    private static boolean isTrimmedPayload(Map<String, String> fields) {
        return fields == null || fields.isEmpty();
    }

    /**
     * BUG-081 DIAG: one INFO line per PEL batch listing every event's
     * stream-id, eventType and entity elementId. Intended to confirm
     * whether two CREATE events with the same `_elementId` (Neo4j rel-id
     * reuse) fall into the same bolt tx. Safe to remove after BUG-081 is
     * closed. Intentionally builds the log string once per batch (not per
     * event) to keep log volume bounded to ≤10 lines per PEL recovery.
     */
    private void logBatchForBug081(int batchIdx, List<ChangeEvent> events,
                                    List<String> applicableIds) {
        if (!log.isInfoEnabled() || events.isEmpty()) return;
        StringBuilder sb = new StringBuilder(events.size() * 64);
        for (int i = 0; i < events.size(); i++) {
            ChangeEvent ev = events.get(i);
            String eid = (ev.entity() != null && ev.entity().elementId() != null)
                ? ev.entity().elementId() : "-";
            sb.append(' ').append(applicableIds.get(i)).append('=')
              .append(ev.eventType()).append('[').append(eid).append(']');
        }
        log.info("BUG-081 DIAG: PEL batch #{} group={} size={} range=[{}..{}] events:{}",
            batchIdx, groupName, events.size(),
            applicableIds.get(0), applicableIds.get(applicableIds.size() - 1),
            sb);
    }
}
