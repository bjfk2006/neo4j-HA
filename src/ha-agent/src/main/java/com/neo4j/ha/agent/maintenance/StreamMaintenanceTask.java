package com.neo4j.ha.agent.maintenance;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.metrics.HaMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XTrimParams;
import redis.clients.jedis.resps.StreamConsumerFullInfo;
import redis.clients.jedis.resps.StreamFullInfo;
import redis.clients.jedis.resps.StreamGroupFullInfo;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background Redis Stream retention task.
 *
 * <p>Problem context (BUG-038). {@link com.neo4j.ha.common.redis.StreamPublisher} issues
 * {@code XADD ... MAXLEN ~ N} on every publish. {@code MAXLEN} is a <b>consumer-agnostic</b>
 * trim policy — Redis drops the oldest messages regardless of which consumer groups still
 * have them in their PEL (Pending Entries List). When a standby's consumption lag exceeds
 * the MAXLEN window, the MAXLEN trim deletes messages that are still recorded in that
 * standby's PEL; on recovery the standby reads the PEL ids and finds empty payloads, which
 * historically caused silent data loss or crash loops.
 *
 * <p>This task runs on a fixed schedule and executes {@code XTRIM MINID} with a cutoff
 * chosen to be strictly SMALLER than the oldest position still needed by any standby's
 * consumer group. Concretely:
 *
 * <pre>
 *   for each consumer group on the stream:
 *     lastDelivered        = group.lastDeliveredId
 *     oldestPending        = group's minimum PEL id (or lastDelivered if PEL is empty)
 *     groupOldestNeeded    = min(lastDelivered, oldestPending)
 *   clusterOldestNeeded    = min over all groups
 *   cutoff                 = clusterOldestNeeded - retentionSafetyWindowMs
 *   XTRIM stream MINID ~ cutoff
 * </pre>
 *
 * <p>XADD's MAXLEN is still in place as a last-resort cap against Redis OOM — this task
 * does not <i>relax</i> anything, it only <i>tightens</i> retention in a safe direction.
 *
 * <p>Invariant: This task NEVER trims messages still referenced by any consumer group's
 * last-delivered-id or PEL. It also NEVER triggers a fullsync automatically (that would
 * be a self-amplifying recovery loop under pressure — see discussion in BUG-038). When a
 * group's PEL is "clean" (XACK'd promptly as in the happy path), the cutoff is just
 * {@code min(lastDeliveredId) - safety}, which means trimming catches up even during
 * long primary-idle periods.
 */
public class StreamMaintenanceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StreamMaintenanceTask.class);

    private final JedisPool jedisPool;
    private final List<String> streamKeys;
    private final long safetyWindowMs;
    private final long intervalMs;
    private final ClusterStateManager clusterState;
    private final HaMetrics metrics;
    private ScheduledExecutorService scheduler;

    /** Single-stream convenience constructor. */
    public StreamMaintenanceTask(JedisPool jedisPool, String streamKey,
                                  long intervalMs, long safetyWindowMs,
                                  ClusterStateManager clusterState,
                                  HaMetrics metrics) {
        this(jedisPool, Arrays.asList(streamKey), intervalMs, safetyWindowMs, clusterState, metrics);
    }

    /**
     * Multi-stream constructor (BUG-040). The same consumer-aware trimming policy is
     * applied to every stream in the list. Typically both the incremental changes
     * stream and the fullsync batch stream share this task — anything else would leave
     * a blind spot where a stream grows unbounded until its MAXLEN bounds kick in.
     */
    public StreamMaintenanceTask(JedisPool jedisPool, List<String> streamKeys,
                                  long intervalMs, long safetyWindowMs,
                                  ClusterStateManager clusterState,
                                  HaMetrics metrics) {
        this.jedisPool = jedisPool;
        this.streamKeys = streamKeys;
        this.intervalMs = intervalMs;
        this.safetyWindowMs = safetyWindowMs;
        this.clusterState = clusterState;
        this.metrics = metrics;
    }

    public void start() {
        if (intervalMs <= 0) {
            log.info("Stream maintenance task disabled (maintenanceInterval not configured)");
            return;
        }
        if (streamKeys == null || streamKeys.isEmpty()) {
            log.info("Stream maintenance task disabled (no streams configured)");
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stream-maintenance");
            t.setDaemon(true);
            return t;
        });
        // Defer first run so the Agent is fully up and consumer groups exist.
        scheduler.scheduleWithFixedDelay(this, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("Stream maintenance task started: streams={}, interval={}ms, safetyWindow={}ms",
                streamKeys, intervalMs, safetyWindowMs);
    }

    public void stop() {
        if (scheduler != null) scheduler.shutdownNow();
    }

    @Override
    public void run() {
        try {
            runOnce();
        } catch (Exception e) {
            // Never let the scheduler swallow the task on unexpected failure.
            log.warn("Stream maintenance run failed (will retry next interval): {}", e.toString());
        }
    }

    /**
     * Runs one maintenance pass across every configured stream and returns the last
     * cutoff applied (or -1 if nothing was trimmed this cycle). The multi-stream
     * variant calls {@link #runOnceFor} sequentially so a failure on one stream does
     * not abort maintenance for the others.
     */
    long runOnce() {
        if (clusterState != null && clusterState.getStandbyNodes().isEmpty()) {
            return -1L;
        }
        long lastCutoff = -1L;
        for (String key : streamKeys) {
            try {
                long c = runOnceFor(key);
                if (c > 0) lastCutoff = c;
            } catch (Exception e) {
                log.warn("Stream maintenance failed for {}: {}", key, e.toString());
            }
        }
        return lastCutoff;
    }

    /** Per-stream maintenance pass. Same semantics as the original single-stream method. */
    long runOnceFor(String streamKey) {

        try (Jedis jedis = jedisPool.getResource()) {
            // We use XINFO STREAM FULL: it returns per-group lastDeliveredId and
            // per-consumer PEL. This is a single round-trip instead of N XINFO calls.
            StreamFullInfo info;
            try {
                info = jedis.xinfoStreamFull(streamKey);
            } catch (Exception e) {
                // Stream doesn't exist yet (cluster freshly initialized with no writes).
                log.debug("xinfoStreamFull failed — stream not yet populated: {}", e.toString());
                return -1L;
            }

            List<StreamGroupFullInfo> groups = info.getGroups();
            if (groups == null || groups.isEmpty()) {
                // No consumer groups yet — trimming by MINID could wipe the whole stream,
                // so we bail out and rely on XADD's MAXLEN alone.
                return -1L;
            }

            StreamEntryID clusterOldestNeeded = null;
            for (StreamGroupFullInfo group : groups) {
                StreamEntryID groupOldest = oldestNeededForGroup(group);
                if (groupOldest == null) continue;
                if (clusterOldestNeeded == null
                        || compareEntryIds(groupOldest, clusterOldestNeeded) < 0) {
                    clusterOldestNeeded = groupOldest;
                }
            }
            if (clusterOldestNeeded == null) {
                return -1L;
            }

            // Stream entry ids in Redis are "<ms>-<seq>". Subtract safety window from ms.
            long cutoffMs = clusterOldestNeeded.getTime() - safetyWindowMs;
            if (cutoffMs <= 0) {
                // Safety window is larger than oldest-needed — we'd potentially trim
                // nothing. Skip quietly.
                return -1L;
            }
            StreamEntryID cutoff = new StreamEntryID(cutoffMs, 0);

            long trimmed = jedis.xtrim(streamKey,
                    XTrimParams.xTrimParams().minId(cutoff.toString()).approximateTrimming());

            if (metrics != null) {
                metrics.streamRetentionCutoffMs.set(cutoffMs);
                if (trimmed > 0) {
                    metrics.streamRetentionTrimmedTotal.increment(trimmed);
                }
            }

            if (trimmed > 0) {
                log.info("Stream maintenance: XTRIM stream={} MINID={} removed {} entries",
                        streamKey, cutoff, trimmed);
            } else {
                log.debug("Stream maintenance: nothing to trim (cutoff={})", cutoff);
            }
            return cutoffMs;
        }
    }

    /**
     * Compute the oldest stream id that this consumer group still cares about.
     * For a healthy group with empty PEL this is {@code lastDeliveredId}; for a group
     * that has in-flight (delivered but unACK'd) messages it is the minimum PEL id.
     */
    private StreamEntryID oldestNeededForGroup(StreamGroupFullInfo group) {
        StreamEntryID lastDelivered = group.getLastDeliveredId();
        StreamEntryID oldestInPel = null;

        List<StreamConsumerFullInfo> consumers = group.getConsumers();
        if (consumers != null) {
            for (StreamConsumerFullInfo consumer : consumers) {
                // XINFO STREAM FULL returns each consumer's PEL as a list of [id, delivery_time_ms, delivery_count]
                // In Jedis 5.x this surfaces as getPending() -> list of maps or pojos depending on version.
                // Be defensive about the shape.
                var pending = consumer.getPending();
                if (pending == null || pending.isEmpty()) continue;
                for (var entry : pending) {
                    StreamEntryID id = extractEntryId(entry);
                    if (id == null) continue;
                    if (oldestInPel == null || compareEntryIds(id, oldestInPel) < 0) {
                        oldestInPel = id;
                    }
                }
            }
        }

        if (oldestInPel != null && lastDelivered != null) {
            return compareEntryIds(oldestInPel, lastDelivered) < 0 ? oldestInPel : lastDelivered;
        }
        if (oldestInPel != null) return oldestInPel;
        return lastDelivered;
    }

    /**
     * Extract a StreamEntryID from a PEL entry. Jedis 5.x returns each PEL entry either
     * as a {@code List<Object>} ({id, time, count}) or as a typed pojo depending on the
     * minor version. We probe defensively.
     */
    private static StreamEntryID extractEntryId(Object pelEntry) {
        if (pelEntry == null) return null;
        if (pelEntry instanceof StreamEntryID sid) return sid;
        if (pelEntry instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            if (first instanceof StreamEntryID sid) return sid;
            if (first instanceof String s) {
                try { return new StreamEntryID(s); } catch (Exception ignored) {}
            }
            if (first instanceof byte[] bytes) {
                try { return new StreamEntryID(new String(bytes)); } catch (Exception ignored) {}
            }
        }
        // Reflection fallback for pojo shapes like StreamPendingEntry{id, ...}
        try {
            var m = pelEntry.getClass().getMethod("getId");
            Object id = m.invoke(pelEntry);
            if (id instanceof StreamEntryID sid) return sid;
            if (id instanceof String s) return new StreamEntryID(s);
        } catch (Exception ignored) { /* no usable getter */ }
        return null;
    }

    /** Compare two stream entry ids lexicographically by (time, seq). */
    private static int compareEntryIds(StreamEntryID a, StreamEntryID b) {
        int c = Long.compare(a.getTime(), b.getTime());
        if (c != 0) return c;
        return Long.compare(a.getSequence(), b.getSequence());
    }
}
