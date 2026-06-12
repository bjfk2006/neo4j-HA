package com.neo4j.ha.cdc.heal;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically scan the primary for "naked" relationships — those whose
 * {@code _elementId} property is {@code NULL} because the
 * {@code cdc-rel-timestamp} APOC trigger (running in
 * {@code phase:'afterAsync'}) failed to flush its stamp task.
 *
 * <p><b>Why this exists — BUG-062</b></p>
 *
 * <p>APOC {@code phase:'afterAsync'} enqueues stamp tasks in a bounded async
 * executor. Under sustained write load we empirically observe ~0.5% of rel
 * stamp tasks being silently dropped (see BUG-062 root cause analysis in
 * {@code ha-agent-design.md}). The affected relationships end up in this
 * state:
 * <pre>
 *   properties(r) = {createdAt: ..., writer: ...}   ← only client-written
 *   r._elementId   IS NULL
 *   r._updated_at  IS NULL
 *   r._created_at  IS NULL
 * </pre>
 *
 * <p>These "naked" rels are permanently invisible to keyset-paginated CDC
 * polling because the driving predicate is
 * {@code WHERE r._updated_at > $lastTs} and {@code NULL > anything = false}.
 * They would never reach the Redis stream and never replicate to standbys.
 *
 * <p><b>Why {@code before}-phase triggers can't solve this</b></p>
 *
 * <p>Neo4j 5.x/6.x APOC has a long-standing bug (BUG-055, verified still
 * present on Neo4j 2026.02.3) where {@code $createdRelationships} is not
 * populated when a trigger runs in {@code phase:'before'}. That rules out
 * moving the stamp into the transaction itself.
 *
 * <p><b>Design</b></p>
 *
 * <p>Runs as an <b>independent scheduled thread</b>, not piggybacked on the
 * CDC poll loop. Rationale: CDC poll is on the hot path with per-tick
 * batch/keyset accounting; adding another cypher would complicate cursor
 * management. Healing is a bounded tail-latency background repair and
 * naturally fits a separate cadence (default 5s).
 *
 * <h2>Heal cypher</h2>
 *
 * <p>For each user relationship type (excluding internal types starting with
 * {@code _}):
 * <pre>{@code
 *   MATCH (a)-[r:<type>]->(b)
 *   WHERE r.createdAt > $cursor              -- fast-path: range index seek
 *     AND r.createdAt < $now - watermarkMs   -- only rels stable for >watermark
 *     AND r._elementId IS NULL               -- filter: only naked
 *   WITH r, a, b
 *   ORDER BY r.createdAt ASC
 *   LIMIT $batchSize
 *   SET r._elementId  = elementId(r),
 *       r._created_at = coalesce(r._created_at, r.createdAt, timestamp()),
 *       r._updated_at = timestamp()           -- healed NOW so keyset > $lastTs
 *   RETURN r.createdAt AS maxCreatedAt
 * }</pre>
 *
 * <p>Two decisions worth justifying:
 * <ul>
 *   <li><b>{@code _updated_at = timestamp()} (now), not {@code r.createdAt}</b>:
 *     CDC's next poll must see the healed rel. If we set {@code _updated_at}
 *     back in time, the CDC cursor has already advanced past that value and
 *     the rel stays invisible. Using {@code timestamp()} guarantees
 *     {@code _updated_at > $lastTs} on the very next poll.
 *   </li>
 *   <li><b>{@code r.createdAt < $now - watermarkMs}</b>: avoids racing the
 *     afterAsync executor on still-in-flight stamps. Default watermark is
 *     5000ms — larger than APOC's observed worst-case flush lag (~1-2s)
 *     under load.
 *   </li>
 * </ul>
 *
 * <h2>Fast-path vs slow-path</h2>
 *
 * <p>If the primary has a range index on {@code r.createdAt} (auto-created
 * by {@code IndexInstaller}, see BUG-062), the heal cypher is a pure index
 * seek — O(naked rels since last cursor), independent of total rel count.
 *
 * <p>If the client does not write {@code r.createdAt} at all, the heal
 * cypher degrades to a type scan filtered by {@code _elementId IS NULL},
 * which on Neo4j 2026.x can still use the {@code r._elementId} range index
 * (Neo4j supports {@code IS NULL} predicates on range indexes). Worst case
 * it is a relationship type scan — still bounded by total rels of that
 * type, not the whole graph.
 *
 * <h2>Cursor management</h2>
 *
 * <p>The healer maintains its own in-memory {@code cursor} (max
 * {@code createdAt} healed so far). The cursor is <b>not persisted</b> to
 * Redis: on agent restart we start from cursor=0 and rescan the whole
 * indexed range once. That rescan is cheap (range index seek + existence
 * filter) and the healed rels have already been fixed, so the
 * {@code _elementId IS NULL} filter immediately rejects them.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>{@link #start(Driver, String)} is called once at CDC collector start
 * and on every {@code switchTarget}. {@link #stop()} is called during
 * {@code CdcCollector.stop()} to ensure no heal runs concurrent with the
 * switchover quiescence sequence.
 */
public class NakedRelationshipHealer {

    private static final Logger log = LoggerFactory.getLogger(NakedRelationshipHealer.class);

    /** Skip rels younger than this — gives APOC afterAsync a chance to finish. */
    private static final long WATERMARK_MS = 5_000;

    /** Cap the work per scheduled tick so we never hog the primary. */
    private static final int BATCH_SIZE = 500;

    /** How often to run a scan. */
    private static final long INTERVAL_MS = 5_000;

    /** Refresh cached relationship type list this often. */
    private static final long TYPE_REFRESH_INTERVAL_MS = 30_000;

    /** Fast-path cypher (uses r.createdAt range index). */
    private static final String HEAL_CYPHER_FAST = """
        MATCH (a)-[r:%s]->(b)
        WHERE r.createdAt > $cursor
          AND r.createdAt < $maxCreatedAt
          AND r._elementId IS NULL
        WITH r, a, b
        ORDER BY r.createdAt ASC
        LIMIT $batchSize
        WITH r, elementId(r) AS eid, r.createdAt AS origCreated
        SET r._elementId  = eid,
            r._created_at = coalesce(r._created_at, origCreated, timestamp()),
            r._updated_at = timestamp()
        RETURN origCreated AS maxCreatedAt
        """;

    /**
     * Slow-path cypher (no {@code r.createdAt} property; client contract not
     * followed). Uses the {@code _elementId} range index's {@code IS NULL}
     * predicate support. Keyed-off on {@code elementId(r)} as a secondary
     * sort so repeated calls always make progress even when all naked rels
     * were committed in the same millisecond.
     */
    private static final String HEAL_CYPHER_SLOW = """
        MATCH (a)-[r:%s]->(b)
        WHERE r._elementId IS NULL
          AND elementId(r) > $eidCursor
        WITH r, a, b
        ORDER BY elementId(r) ASC
        LIMIT $batchSize
        WITH r, elementId(r) AS eid
        SET r._elementId  = eid,
            r._created_at = coalesce(r._created_at, r.createdAt, timestamp()),
            r._updated_at = timestamp()
        RETURN eid AS maxEid
        """;

    private volatile Driver driver;
    private volatile String database;
    private volatile boolean running;
    private ScheduledExecutorService scheduler;

    /** Per-rel-type cursor on r.createdAt (fast-path). */
    private final Map<String, Long> createdAtCursors = new HashMap<>();

    /** Per-rel-type cursor on elementId(r) (slow-path). */
    private final Map<String, String> eidCursors = new HashMap<>();

    /** Cached list of user relationship types. */
    private volatile List<String> cachedTypes = List.of();
    private volatile long lastTypeRefreshMs = 0;

    /** Counters for observability. */
    private long totalHealed = 0;
    private long totalScans = 0;

    public synchronized void start(Driver primaryDriver, String database) {
        if (running) {
            log.warn("NakedRelationshipHealer already running; ignoring start()");
            return;
        }
        this.driver = primaryDriver;
        this.database = database;
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cdc-naked-rel-healer");
            t.setDaemon(true);
            return t;
        });
        // Stagger first run by WATERMARK_MS so we don't race primary bootstrap.
        scheduler.scheduleWithFixedDelay(this::scanOnce,
            WATERMARK_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("NakedRelationshipHealer started on database '{}' " +
            "(interval={}ms, watermark={}ms, batch={})",
            database, INTERVAL_MS, WATERMARK_MS, BATCH_SIZE);
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        log.info("NakedRelationshipHealer stopped (lifetime healed={}, scans={})",
            totalHealed, totalScans);
    }

    /**
     * Reset per-rel-type cursors when the primary changes. The new primary's
     * {@code elementId()} namespace is different so an old cursor is
     * meaningless; and healed-on-old-primary state replicates via CDC, so
     * re-scanning from 0 on NEW is safe and cheap (all already-healed rels
     * have {@code _elementId IS NOT NULL} and are filtered out by the
     * predicate).
     */
    public synchronized void onPrimaryChanged() {
        createdAtCursors.clear();
        eidCursors.clear();
    }

    private void scanOnce() {
        if (!running || driver == null) return;
        try {
            totalScans++;
            List<String> types = refreshTypes();
            if (types.isEmpty()) return;

            long start = System.currentTimeMillis();
            long nowMs = System.currentTimeMillis();
            long healedThisScan = 0;
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                for (String relType : types) {
                    healedThisScan += healOneType(session, relType, nowMs);
                }
            }
            if (healedThisScan > 0) {
                totalHealed += healedThisScan;
                long duration = System.currentTimeMillis() - start;
                log.info("NakedRelationshipHealer healed {} rel(s) in this scan " +
                    "({}ms, lifetime={})", healedThisScan, duration, totalHealed);
            } else {
                long duration = System.currentTimeMillis() - start;
                if (duration > 200) {
                    log.warn("NakedRelationshipHealer scan took {}ms with 0 rels healed " +
                        "(total rel types: {}); indexes may be missing or cold", duration, types.size());
                } else {
                    log.debug("NakedRelationshipHealer scan: 0 rels healed, {}ms", duration);
                }
            }
        } catch (Exception e) {
            log.warn("NakedRelationshipHealer scan failed (will retry next tick): {}", e.toString());
        }
    }

    private long healOneType(Session session, String relType, long nowMs) {
        String sanitized = sanitizeRelType(relType);
        long cursor = createdAtCursors.getOrDefault(relType, 0L);
        long maxCreatedAt = nowMs - WATERMARK_MS;
        if (maxCreatedAt <= cursor) {
            // Nothing could have possibly become "stable" since last scan.
            return 0;
        }
        try {
            String cypher = HEAL_CYPHER_FAST.formatted(sanitized);
            var records = session.run(cypher, Map.of(
                "cursor", cursor,
                "maxCreatedAt", maxCreatedAt,
                "batchSize", BATCH_SIZE
            )).list();
            if (!records.isEmpty()) {
                // Advance cursor to the largest createdAt healed this batch.
                long newCursor = records.stream()
                    .mapToLong(r -> r.get("maxCreatedAt").asLong())
                    .max().orElse(cursor);
                createdAtCursors.put(relType, newCursor);
                return records.size();
            }
            // Empty result from fast path. Could mean:
            //   (a) no naked rels in the range (normal 99.9%+ case), OR
            //   (b) client doesn't write r.createdAt, so r.createdAt IS NULL
            //       and the fast-path range predicate never matches.
            //
            // Detect (b) with a cheap sentinel query and fall back to slow path.
            if (needsSlowPath(session, relType, sanitized)) {
                return healOneTypeSlow(session, relType, sanitized);
            }
            return 0;
        } catch (org.neo4j.driver.exceptions.ClientException ce) {
            // Most common: user hasn't set r.createdAt on any rel, so the
            // range predicate in the fast cypher triggers planner to reject.
            // (Shouldn't happen with our syntax, but be defensive.)
            log.debug("Fast-path heal for '{}' failed ({}); falling back to slow-path",
                relType, ce.code());
            return healOneTypeSlow(session, relType, sanitized);
        }
    }

    private boolean needsSlowPath(Session session, String relType, String sanitized) {
        // Ask Neo4j: does ANY rel of this type have r.createdAt set?
        // If the answer is "no" for this full type, there's no point in the
        // fast path and we must scan by _elementId IS NULL instead. This is
        // a single index-backed EXISTS-style query, cheap to repeat.
        try {
            String cypher = ("""
                MATCH ()-[r:%s]->()
                WHERE r.createdAt IS NOT NULL
                RETURN 1
                LIMIT 1
                """).formatted(sanitized);
            var records = session.run(cypher).list();
            return records.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    private long healOneTypeSlow(Session session, String relType, String sanitized) {
        String eidCursor = eidCursors.getOrDefault(relType, "");
        try {
            String cypher = HEAL_CYPHER_SLOW.formatted(sanitized);
            var records = session.run(cypher, Map.of(
                "eidCursor", eidCursor,
                "batchSize", BATCH_SIZE
            )).list();
            if (records.isEmpty()) return 0;
            // Advance cursor by lexicographic max of elementId.
            String newCursor = records.stream()
                .map(r -> r.get("maxEid").asString())
                .max(String::compareTo)
                .orElse(eidCursor);
            eidCursors.put(relType, newCursor);
            log.info("NakedRelationshipHealer (slow-path) healed {} '{}' rel(s); " +
                "client is not writing r.createdAt — consider contract migration",
                records.size(), relType);
            return records.size();
        } catch (Exception e) {
            log.warn("Slow-path heal for '{}' failed: {}", relType, e.toString());
            return 0;
        }
    }

    private List<String> refreshTypes() {
        long now = System.currentTimeMillis();
        if (now - lastTypeRefreshMs < TYPE_REFRESH_INTERVAL_MS && !cachedTypes.isEmpty()) {
            return cachedTypes;
        }
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            List<String> fresh = session.run(
                "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
            ).list(r -> r.get("relationshipType").asString());
            fresh = fresh.stream()
                .filter(t -> t != null && !t.isEmpty() && !t.startsWith("_"))
                .toList();
            this.cachedTypes = fresh;
            this.lastTypeRefreshMs = now;
            return fresh;
        } catch (Exception e) {
            log.warn("NakedRelationshipHealer: failed to refresh rel type list ({}); " +
                "reusing previous cache of {} types", e.getMessage(), cachedTypes.size());
            return cachedTypes;
        }
    }

    private static String sanitizeRelType(String t) {
        if (t.matches("[A-Za-z_][A-Za-z0-9_]*")) return t;
        return "`" + t.replace("`", "``") + "`";
    }

    /** Expose counters for future metrics integration / tests. */
    public long getTotalHealed() { return totalHealed; }
    public long getTotalScans() { return totalScans; }

    /** Return the watermark for reference (for logging/tests). */
    public static long getWatermarkMs() { return WATERMARK_MS; }
}
