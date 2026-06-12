package com.neo4j.ha.common.redis;

import com.neo4j.ha.common.model.SyncMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;

import java.util.Map;
import java.util.Optional;

public class CheckpointManager {

    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);

    private static final String CDC_CHECKPOINT_PREFIX = "neo4j:ha:cdc-checkpoint:";
    private static final String SYNC_CHECKPOINT_PREFIX = "neo4j:ha:sync-checkpoint:";
    /**
     * Pending post-failover reconciliation intent (BUG-080). Written at failover
     * time by FailoverOrchestrator when the old primary is unreachable (crashed
     * / kill -9); consumed by OldPrimaryRecovery once the node comes back as a
     * STANDBY and is reachable via bolt. Encodes "this node has stranded
     * afterAsync writes on its disk beyond cursor X; reverse-reconcile them onto
     * whichever node is primary at recovery time". Deleted after one recovery
     * pass regardless of success — lingering stranded data is observable via
     * rel-gap-diag.sh and must not re-trigger on every restart.
     */
    private static final String PENDING_RECONCILE_PREFIX = "neo4j:ha:pending-reconcile:";

    private final JedisPool jedisPool;

    public CheckpointManager(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // === CDC Collector (primary node) — keyset pagination composite cursor ===

    /**
     * Persist the CDC keyset cursor for a primary node.
     *
     * <p>BUG-057: schema v2 splits the single {@code (lastTs, lastElementId)}
     * cursor into three: {@code (lastNodeTs, lastNodeEid)},
     * {@code (lastRelTs, lastRelEid)} and {@code (lastDeleteTs,
     * lastDeleteEid)}. Node and relationship scans each advance their own
     * cursor so a {@code "5:..."} relationship eid can no longer silently
     * drop "same-millisecond" nodes on the next poll. Legacy
     * {@code lastTs}/{@code lastElementId} fields are kept in the Redis HASH
     * as {@code max} / composite values so a partially-rolled-back reader
     * still sees a sensible monotonic cursor.</p>
     */
    public void saveCdcCheckpoint(String nodeId,
                                   long lastNodeTs, String lastNodeEid,
                                   long lastRelTs, String lastRelEid,
                                   long lastDeleteTs, String lastDeleteEid,
                                   String lastStreamId) {
        // Legacy compatibility: the v1 fields are derived so old readers
        // (running pre-BUG-057 code against a v2 checkpoint) still see a
        // monotonic cursor. v2 readers ignore them.
        long legacyLastTs = Math.max(lastNodeTs, lastRelTs);
        String legacyEid;
        if (lastRelTs > lastNodeTs) legacyEid = lastRelEid != null ? lastRelEid : "";
        else if (lastNodeTs > lastRelTs) legacyEid = lastNodeEid != null ? lastNodeEid : "";
        else legacyEid = (lastRelEid != null && !lastRelEid.isEmpty())
            ? lastRelEid
            : (lastNodeEid != null ? lastNodeEid : "");

        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> hash = new java.util.HashMap<>();
            hash.put("lastNodeTs", String.valueOf(lastNodeTs));
            hash.put("lastNodeEid", lastNodeEid != null ? lastNodeEid : "");
            hash.put("lastRelTs", String.valueOf(lastRelTs));
            hash.put("lastRelEid", lastRelEid != null ? lastRelEid : "");
            hash.put("lastDeleteTs", String.valueOf(lastDeleteTs));
            hash.put("lastDeleteEid", lastDeleteEid != null ? lastDeleteEid : "");
            hash.put("lastStreamId", lastStreamId != null ? lastStreamId : "");
            // Legacy (v1) fields for back-compat.
            hash.put("lastTs", String.valueOf(legacyLastTs));
            hash.put("lastElementId", legacyEid);
            // Wall-clock write time; used by HaAgent.evaluateServiceStates to tell whether
            // the CDC loop is actively progressing vs. resumed from cold and still scanning
            // historical data (BUG-033).
            hash.put("updatedAt", String.valueOf(System.currentTimeMillis()));
            // Schema version tag so future migrations can detect what they're reading.
            hash.put("schemaVersion", "2");
            jedis.hset(CDC_CHECKPOINT_PREFIX + nodeId, hash);
        }
    }

    public Optional<CdcCheckpoint> loadCdcCheckpoint(String nodeId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(CDC_CHECKPOINT_PREFIX + nodeId);
            if (data == null || data.isEmpty()) {
                return Optional.empty();
            }

            // BUG-057 v1→v2 back-compat. If the v2 fields (lastNodeTs, lastRelTs)
            // are missing, this checkpoint was written by a pre-BUG-057 writer;
            // seed both node and rel cursors from the legacy (lastTs,
            // lastElementId) pair. Putting the same eid on both cursors is safe
            // because the next poll's node query only trusts the eid when
            // _updated_at == lastTs, and in that tied case the old cursor
            // would have shown the same degenerate behaviour — we are no
            // worse than v1. On the very next successful poll the cursors
            // will diverge into their correct independent positions.
            long legacyLastTs = Long.parseLong(data.getOrDefault("lastTs", "0"));
            String legacyEid = data.getOrDefault("lastElementId", "");

            boolean hasV2 = data.containsKey("lastNodeTs") || data.containsKey("lastRelTs");
            long lastNodeTs;
            String lastNodeEid;
            long lastRelTs;
            String lastRelEid;
            if (hasV2) {
                lastNodeTs = Long.parseLong(data.getOrDefault("lastNodeTs", "0"));
                lastNodeEid = data.getOrDefault("lastNodeEid", "");
                lastRelTs = Long.parseLong(data.getOrDefault("lastRelTs", "0"));
                lastRelEid = data.getOrDefault("lastRelEid", "");
            } else {
                // v1 → v2 migration: seed both cursors from the shared legacy value.
                // The shared eid could have any "kind" prefix ("4:" or "5:"); assigning
                // it unchanged to both cursors reproduces v1 behaviour exactly on the
                // first post-upgrade poll, and they diverge afterwards.
                lastNodeTs = legacyLastTs;
                lastNodeEid = legacyEid;
                lastRelTs = legacyLastTs;
                lastRelEid = legacyEid;
                log.info("CDC checkpoint for {} is v1 schema; seeding v2 node+rel cursors from " +
                    "legacy (lastTs={}, lastElementId='{}')", nodeId, legacyLastTs, legacyEid);
            }

            return Optional.of(new CdcCheckpoint(
                lastNodeTs, lastNodeEid,
                lastRelTs, lastRelEid,
                Long.parseLong(data.getOrDefault("lastDeleteTs", "0")),
                data.getOrDefault("lastDeleteEid", ""),
                data.getOrDefault("lastStreamId", ""),
                Long.parseLong(data.getOrDefault("updatedAt", "0"))
            ));
        }
    }

    // === Sync Applier (standby node) — Stream consumer checkpoint ===

    public void saveSyncCheckpoint(String nodeId, String lastStreamId,
                                    long lastEventTs, SyncMode mode) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(SYNC_CHECKPOINT_PREFIX + nodeId, Map.of(
                "lastStreamId", lastStreamId != null ? lastStreamId : "",
                "lastEventTs", String.valueOf(lastEventTs),
                "syncMode", mode.name(),
                "updatedAt", String.valueOf(System.currentTimeMillis())
            ));
        }
    }

    public Optional<SyncCheckpoint> loadSyncCheckpoint(String nodeId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(SYNC_CHECKPOINT_PREFIX + nodeId);
            if (data == null || data.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new SyncCheckpoint(
                data.getOrDefault("lastStreamId", ""),
                Long.parseLong(data.getOrDefault("lastEventTs", "0")),
                SyncMode.valueOf(data.getOrDefault("syncMode", "INCREMENTAL")),
                Long.parseLong(data.getOrDefault("lastFullSyncAt", "0")),
                Long.parseLong(data.getOrDefault("pendingCount", "0"))
            ));
        }
    }

    /**
     * Copy the CDC checkpoint from one nodeId key to another.
     * Used on failover/switchover so the new primary's CDC loop resumes from the
     * previous primary's position instead of rescanning the whole database (M7).
     * No-op if the source checkpoint does not exist.
     */
    public void copyCdcCheckpoint(String fromNodeId, String toNodeId) {
        if (fromNodeId == null || toNodeId == null || fromNodeId.equals(toNodeId)) return;
        Optional<CdcCheckpoint> src = loadCdcCheckpoint(fromNodeId);
        if (src.isEmpty()) {
            log.info("No CDC checkpoint to copy from {}", fromNodeId);
            return;
        }
        CdcCheckpoint cp = src.get();
        saveCdcCheckpoint(toNodeId,
            cp.lastNodeTs(), cp.lastNodeEid(),
            cp.lastRelTs(), cp.lastRelEid(),
            cp.lastDeleteTs(), cp.lastDeleteEid(),
            cp.lastStreamId());
        log.info("Copied CDC checkpoint from {} → {} (lastNodeTs={}, lastRelTs={}, lastStreamId={})",
            fromNodeId, toNodeId, cp.lastNodeTs(), cp.lastRelTs(), cp.lastStreamId());
    }

    public boolean isCheckpointValid(String streamKey, String lastStreamId) {
        if (lastStreamId == null || lastStreamId.isEmpty()) {
            return false;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            var entries = jedis.xrange(streamKey,
                new StreamEntryID(lastStreamId),
                new StreamEntryID(lastStreamId), 1);
            return entries != null && !entries.isEmpty();
        }
    }

    /**
     * CDC keyset checkpoint (v2, BUG-057). Node and relationship cursors are
     * independent so a relationship eid can never poison the next poll's node
     * scan. Legacy callers can still access a monotonic "overall" timestamp
     * via {@link #lastTs()}.
     */
    public record CdcCheckpoint(
        long lastNodeTs,
        String lastNodeEid,
        long lastRelTs,
        String lastRelEid,
        long lastDeleteTs,
        String lastDeleteEid,
        String lastStreamId,
        /** Wall-clock time when this checkpoint was last written (BUG-033). */
        long updatedAt
    ) {
        /** Legacy composite last-ts across (node, rel). Freshness checks use this. */
        public long lastTs() {
            return Math.max(lastNodeTs, lastRelTs);
        }

        /**
         * Legacy composite last-eid — the one corresponding to whichever of
         * (node, rel) has the larger ts. Kept so older readers (and diagnostic
         * dumps) still compile without touching the v2 field names.
         */
        public String lastElementId() {
            if (lastRelTs > lastNodeTs) return lastRelEid;
            if (lastNodeTs > lastRelTs) return lastNodeEid;
            return lastRelEid != null && !lastRelEid.isEmpty() ? lastRelEid : lastNodeEid;
        }
    }

    public record SyncCheckpoint(
        String lastStreamId,
        long lastEventTs,
        SyncMode syncMode,
        long lastFullSyncAt,
        long pendingCount
    ) {}

    // === Pending post-failover reconciliation (BUG-080) ===

    /**
     * Snapshot of the old primary's CDC cursor at the moment of failover, plus
     * some audit fields. Stored on a PER-OLD-PRIMARY key and read when that
     * node recovers. We snapshot the cursor ourselves (rather than re-reading
     * {@code cdc-checkpoint:<oldId>} at recovery time) because the CDC key may
     * have been touched between failover and recovery — {@code copyCdcCheckpoint}
     * moves the value onto the new primary, and a second failover/recovery
     * cycle could overwrite it.
     *
     * @param lastRelTs       rel cursor at failover time; reconciler scans rels
     *                        with {@code r._updated_at > lastRelTs} on the old
     *                        primary's disk.
     * @param lastDeleteTs    delete-event cursor at failover time.
     * @param lastNodeTs      node cursor at failover time; not used by the
     *                        scan today but carried for forward compat and for
     *                        diagnostic dumps.
     * @param failoverTs      wall-clock ms the pending intent was recorded.
     *                        Diagnostics only.
     * @param newPrimaryAtFailover  nodeId of whichever standby was promoted.
     *                        Diagnostics only — recovery uses {@code
     *                        ClusterStateManager#getPrimaryNodeId} because the
     *                        cluster may have failed over again in the meantime.
     */
    public record PendingReconcile(
        long lastRelTs,
        long lastDeleteTs,
        long lastNodeTs,
        long failoverTs,
        String newPrimaryAtFailover
    ) {}

    /**
     * One-week TTL on the pending-reconcile key. Safety net for the
     * FailoverOrchestrator happy-path where {@code tryCleanupOldPrimary}
     * succeeds and {@code markPendingCleanup} never fires — without a TTL
     * the key would linger forever. One week is long enough that any
     * realistic node-recovery timeline consumes the key first, short
     * enough that orphans self-clean before the next operator audit.
     */
    private static final long PENDING_RECONCILE_TTL_MS = 7L * 24 * 3600 * 1000L;

    public void savePendingReconcile(String oldPrimaryId, PendingReconcile p) {
        String key = PENDING_RECONCILE_PREFIX + oldPrimaryId;
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> hash = new java.util.HashMap<>();
            hash.put("lastRelTs", String.valueOf(p.lastRelTs()));
            hash.put("lastDeleteTs", String.valueOf(p.lastDeleteTs()));
            hash.put("lastNodeTs", String.valueOf(p.lastNodeTs()));
            hash.put("failoverTs", String.valueOf(p.failoverTs()));
            hash.put("newPrimaryAtFailover",
                p.newPrimaryAtFailover() != null ? p.newPrimaryAtFailover() : "");
            hash.put("schemaVersion", "1");
            jedis.hset(key, hash);
            jedis.pexpire(key, PENDING_RECONCILE_TTL_MS);
        }
    }

    public Optional<PendingReconcile> loadPendingReconcile(String oldPrimaryId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(PENDING_RECONCILE_PREFIX + oldPrimaryId);
            if (data == null || data.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new PendingReconcile(
                Long.parseLong(data.getOrDefault("lastRelTs", "0")),
                Long.parseLong(data.getOrDefault("lastDeleteTs", "0")),
                Long.parseLong(data.getOrDefault("lastNodeTs", "0")),
                Long.parseLong(data.getOrDefault("failoverTs", "0")),
                data.getOrDefault("newPrimaryAtFailover", "")
            ));
        }
    }

    public void deletePendingReconcile(String oldPrimaryId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(PENDING_RECONCILE_PREFIX + oldPrimaryId);
        }
    }

    /**
     * Return the wall-clock time (ms) when the SyncApplier last persisted a checkpoint for
     * the given node, or 0 if no checkpoint exists. Used to distinguish "already caught up and
     * idle" from "stale checkpoint, applier not running" in serviceState evaluation.
     */
    public long getSyncCheckpointUpdatedAt(String nodeId) {
        try (Jedis jedis = jedisPool.getResource()) {
            String v = jedis.hget(SYNC_CHECKPOINT_PREFIX + nodeId, "updatedAt");
            if (v == null || v.isEmpty()) return 0L;
            try {
                return Long.parseLong(v);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }
    }
}
