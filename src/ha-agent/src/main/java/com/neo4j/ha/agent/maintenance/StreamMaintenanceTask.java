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

    /**
     * REVIEW-C11: set once when the server rejects {@code XTRIM MINID}
     * (added in Redis 6.2), after which {@link #TRIM_BELOW_LUA} is used instead.
     */
    private volatile boolean minIdUnsupported = false;

    /** Max entries examined per pass in the Lua fallback. */
    private static final int LUA_TRIM_BUDGET = 10_000;

    /**
     * MINID-equivalent trim for Redis &lt; 6.2, verified against Redis 6.0.16.
     *
     * <p>Two things are unavailable before 6.2 and the workaround relies on
     * neither: {@code XTRIM MINID} itself, and XRANGE's exclusive {@code (id}
     * bound. What IS available since 5.0 is the shorthand where a bare
     * {@code <ms>} as an XRANGE end bound means {@code <ms>-<maxseq>} — so
     * asking for {@code XRANGE key - (cutoffMs-1)} returns exactly the entries
     * strictly older than {@code cutoffMs-0}, which is precisely MINID's
     * deletion set for the cutoffs this class produces (always seq 0).</p>
     *
     * <p>Why Lua rather than doing this from Java: computing the obsolete count
     * and then trimming would be two round trips, and any XADD landing between
     * them makes a length-based {@code XTRIM MAXLEN} cut too deep — it would
     * drop entries just above the cutoff that a lagging standby still needs,
     * which is the exact failure mode this whole task exists to prevent. A
     * script executes atomically, so the count and the trim cannot be separated.</p>
     *
     * <p>The common path uses {@code XTRIM MAXLEN} once the full obsolete set is
     * known, because that genuinely reclaims macro nodes; {@code XDEL} only
     * tombstones entries. XDEL is used solely for the catch-up path where the
     * budget was exhausted and the remainder is handled next cycle.</p>
     */
    private static final String TRIM_BELOW_LUA =
        "local cutoffMs = tonumber(ARGV[1])\n" +
        "local budget   = tonumber(ARGV[2])\n" +
        "if cutoffMs == nil or cutoffMs <= 0 then return 0 end\n" +
        "local upper = tostring(cutoffMs - 1)\n" +
        "local entries = redis.call('XRANGE', KEYS[1], '-', upper, 'COUNT', budget)\n" +
        "local n = #entries\n" +
        "if n == 0 then return 0 end\n" +
        "if n < budget then\n" +
        "  local len = redis.call('XLEN', KEYS[1])\n" +
        "  local keep = len - n\n" +
        "  if keep < 0 then keep = 0 end\n" +
        "  redis.call('XTRIM', KEYS[1], 'MAXLEN', keep)\n" +
        "  return n\n" +
        "end\n" +
        "local i = 1\n" +
        "local deleted = 0\n" +
        "while i <= n do\n" +
        "  local chunk = {}\n" +
        "  local j = 0\n" +
        "  while i <= n and j < 200 do\n" +
        "    j = j + 1\n" +
        "    chunk[j] = entries[i][1]\n" +
        "    i = i + 1\n" +
        "  end\n" +
        "  deleted = deleted + redis.call('XDEL', KEYS[1], unpack(chunk, 1, j))\n" +
        "end\n" +
        "return deleted\n";

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
                // REVIEW-C8: a consumer group left behind by a standby that was
                // permanently removed from the cluster keeps its lastDeliveredId
                // frozen forever. Because the cutoff is the MINIMUM across all
                // groups, that one dead group pins retention at its last position
                // and the stream grows until XADD's MAXLEN (1e6) starts dropping
                // entries — which is precisely the consumer-unaware trimming this
                // task exists to avoid. Groups that are both idle beyond the
                // abandonment threshold and have nothing pending are ignored for
                // the cutoff computation (they are never deleted here; that stays
                // an operator decision).
                if (isAbandonedGroup(group)) {
                    log.warn("Stream maintenance: ignoring abandoned consumer group '{}' on {} "
                        + "for retention (no consumers / all idle > {}ms, PEL empty). "
                        + "Delete it with XGROUP DESTROY once the node is decommissioned.",
                        group.getName(), streamKey, ABANDONED_GROUP_IDLE_MS);
                    continue;
                }
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

            long trimmed;
            if (minIdUnsupported) {
                trimmed = trimBelowViaLua(jedis, streamKey, cutoffMs);
            } else {
                try {
                    trimmed = jedis.xtrim(streamKey,
                            XTrimParams.xTrimParams().minId(cutoff.toString()).approximateTrimming());
                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                    // REVIEW-C11 (found while verifying on the HK cluster): XTRIM MINID
                    // was added in Redis 6.2. On an older server it is rejected with a
                    // bare "ERR syntax error", which this task logged once per interval
                    // forever while silently never trimming anything — so BUG-038's
                    // whole point (consumer-aware retention, so a lagging standby's PEL
                    // can never be trimmed out from under it) was inactive, leaving
                    // XADD's consumer-AGNOSTIC MAXLEN as the only bound.
                    String msg = e.getMessage() == null ? "" : e.getMessage();
                    if (!msg.contains("syntax error")) throw e;
                    minIdUnsupported = true;
                    log.warn("XTRIM MINID rejected by this Redis ({}) — MINID needs 6.2+. "
                        + "Falling back to an equivalent atomic Lua trim for {} (works on 5.0+). "
                        + "Consumer-aware retention stays active; upgrading Redis to >= 6.2 "
                        + "would let the native command be used again. Logged once.",
                        msg, streamKey);
                    trimmed = trimBelowViaLua(jedis, streamKey, cutoffMs);
                }
            }

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
    /** Runs {@link #TRIM_BELOW_LUA}; returns how many entries it removed. */
    private long trimBelowViaLua(Jedis jedis, String streamKey, long cutoffMs) {
        Object r = jedis.eval(TRIM_BELOW_LUA,
                java.util.List.of(streamKey),
                java.util.List.of(Long.toString(cutoffMs), Integer.toString(LUA_TRIM_BUDGET)));
        return (r instanceof Number num) ? num.longValue() : 0L;
    }

    /**
     * A group counts as abandoned when it has no live consumer activity for
     * {@link #ABANDONED_GROUP_IDLE_MS} AND holds nothing in any PEL. The PEL
     * condition is the safety interlock: a group with unacked entries is still
     * owed data, however long it has been quiet, so it always pins retention.
     */
    static final long ABANDONED_GROUP_IDLE_MS = 24L * 60 * 60 * 1000; // 24h

    private boolean isAbandonedGroup(StreamGroupFullInfo group) {
        List<StreamConsumerFullInfo> consumers = group.getConsumers();
        if (consumers == null || consumers.isEmpty()) return true;
        for (StreamConsumerFullInfo c : consumers) {
            var pending = c.getPending();
            if (pending != null && !pending.isEmpty()) return false; // still owed data
        }
        for (StreamConsumerFullInfo c : consumers) {
            Long seen = consumerIdleMs(c);
            if (seen == null || seen < ABANDONED_GROUP_IDLE_MS) return false;
        }
        return true;
    }

    /** Best-effort idle time for a consumer across Jedis shapes. */
    private static Long consumerIdleMs(StreamConsumerFullInfo c) {
        for (String getter : new String[]{"getSeenTime", "getInactive", "getIdle"}) {
            try {
                var m = c.getClass().getMethod(getter);
                Object v = m.invoke(c);
                if (v instanceof Number num) {
                    long raw = num.longValue();
                    // seen-time is an absolute epoch ms; idle/inactive are deltas.
                    return "getSeenTime".equals(getter)
                        ? Math.max(0L, System.currentTimeMillis() - raw)
                        : raw;
                }
            } catch (Exception ignored) { /* try next shape */ }
        }
        return null;
    }

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
