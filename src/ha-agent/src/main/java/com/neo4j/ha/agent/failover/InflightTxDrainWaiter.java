package com.neo4j.ha.agent.failover;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2.5 of the switchover/failover pipeline (BUG-056).
 *
 * <p>{@link com.neo4j.ha.agent.routing.HaProxyUpdater#blockWrites} returning
 * does NOT mean the OLD primary is fully quiescent:
 * <ul>
 *   <li>{@code set server ... state maint} only stops HAProxy from routing
 *       NEW connections to the OLD primary.</li>
 *   <li>{@code shutdown sessions server ...} TCP-RSTs the existing Bolt
 *       connections — but Neo4j's kernel holds write transactions server-side;
 *       transactions already in-flight continue to commit regardless of whether
 *       the originating Bolt connection still exists.</li>
 * </ul>
 *
 * <p>These "tail commits" produce node/relationship rows on the OLD primary
 * with {@code _elementId} / {@code _updated_at} stamped by APOC triggers, but
 * because {@link com.neo4j.ha.cdc.CdcCollector#stop()}'s final-drain loop uses
 * "pollingState no longer advancing" as its exit condition, the drain may
 * finish <i>before</i> those tail commits land — orphan writes are born.
 *
 * <p>This waiter polls Neo4j's {@code SHOW TRANSACTIONS} view until all
 * <i>external</i> write transactions on the target database are gone (or a
 * hard deadline elapses). It runs on the OLD primary driver between
 * {@code haProxyUpdater.blockWrites(...)} and {@code cdcCollector.stop()}.
 *
 * <p>Exit conditions (ALL must hold):
 * <ul>
 *   <li>2 consecutive polls returning 0 external write transactions</li>
 *   <li>total elapsed time {@code >= minWaitMs} (default 200ms) — this gives
 *       in-flight kernel commits that started just before blockWrites time
 *       to finish even if the first SHOW TRANSACTIONS observed 0</li>
 * </ul>
 *
 * <p>Hard upper bound {@code maxWaitMs} (default 3s) — if reached with
 * non-zero count, log ERROR and return anyway. Caller continues the pipeline;
 * BUG-048's fencing token + read-side fence still protect against leaked
 * events producing orphan data in the Stream (they just won't protect the
 * OLD's own local storage — that's what reverse-sync / fullsync is for).</p>
 */
public class InflightTxDrainWaiter {

    private static final Logger log = LoggerFactory.getLogger(InflightTxDrainWaiter.class);

    /** Default: give in-flight tx 200ms guaranteed wiggle room. */
    public static final long DEFAULT_MIN_WAIT_MS = 200L;

    /** Default: never block the switchover more than 3s on this. */
    public static final long DEFAULT_MAX_WAIT_MS = 3000L;

    /** Sleep between SHOW TRANSACTIONS polls. */
    private static final long POLL_INTERVAL_MS = 50L;

    /**
     * Query returning the count of external write transactions on {@code $db}.
     *
     * <p>Filters:
     * <ul>
     *   <li>{@code currentQuery IS NOT NULL} — exclude idle/housekeeping tx</li>
     *   <li>{@code database = $db} — only the app database</li>
     *   <li>{@code username <> 'neo4j'} false-positive guard disabled: the
     *       agent itself logs in as {@code neo4j}; instead we exclude the
     *       SHOW TRANSACTIONS query itself by matching its current query text</li>
     *   <li>{@code NOT currentQuery STARTS WITH 'SHOW TRANSACTIONS'} —
     *       exclude our own monitor session</li>
     * </ul>
     *
     * <p>{@code SHOW TRANSACTIONS} is available on Neo4j 5.x and returns rows
     * for every active tx the caller is authorized to see. The agent connects
     * as the {@code neo4j} admin user so it sees everything.
     */
    private static final String COUNT_QUERY =
        "SHOW TRANSACTIONS " +
        "YIELD database, currentQuery, status, transactionId " +
        "WHERE database = $db " +
        "  AND currentQuery IS NOT NULL " +
        "  AND NOT currentQuery STARTS WITH 'SHOW TRANSACTIONS' " +
        "RETURN count(*) AS n";

    private final long minWaitMs;
    private final long maxWaitMs;

    public InflightTxDrainWaiter() {
        this(DEFAULT_MIN_WAIT_MS, DEFAULT_MAX_WAIT_MS);
    }

    public InflightTxDrainWaiter(long minWaitMs, long maxWaitMs) {
        if (minWaitMs < 0) throw new IllegalArgumentException("minWaitMs < 0");
        if (maxWaitMs < minWaitMs) throw new IllegalArgumentException("maxWaitMs < minWaitMs");
        this.minWaitMs = minWaitMs;
        this.maxWaitMs = maxWaitMs;
    }

    /**
     * Block until OLD primary has no external write transactions OR deadline.
     *
     * @param oldPrimaryDriver driver connected to the OLD primary
     * @param database         target database name (usually {@code neo4j})
     * @return elapsed milliseconds actually waited (best-effort info; callers
     *         typically log this).
     */
    public long await(Driver oldPrimaryDriver, String database) {
        if (oldPrimaryDriver == null) {
            log.warn("InflightTxDrainWaiter: no OLD primary driver, skipping drain wait");
            return 0L;
        }
        long start = System.currentTimeMillis();
        long deadline = start + maxWaitMs;
        int consecutiveZero = 0;
        long lastCount = -1;

        while (true) {
            long now = System.currentTimeMillis();
            long elapsed = now - start;

            long count;
            try {
                count = pollCount(oldPrimaryDriver, database);
            } catch (Exception e) {
                // Neo4j briefly unavailable during the switchover is plausible;
                // don't spin forever — treat as "drained" and let outer loop
                // fall through (CdcCollector.stop() is defence-in-depth).
                log.warn("InflightTxDrainWaiter: poll failed after {}ms: {} — "
                    + "treating as drained", elapsed, e.toString());
                return elapsed;
            }
            lastCount = count;

            if (count == 0) {
                consecutiveZero++;
                // Exit only if both conditions hold: elapsed >= minWait AND 2
                // consecutive zero readings. The 2-reading rule prevents a
                // tx that's in the tiny gap between commit and "tx row gone"
                // from slipping past.
                if (consecutiveZero >= 2 && elapsed >= minWaitMs) {
                    log.info("InflightTxDrainWaiter: drained in {}ms "
                        + "(consecutiveZero={}, db={})", elapsed, consecutiveZero, database);
                    return elapsed;
                }
            } else {
                consecutiveZero = 0;
            }

            if (now >= deadline) {
                log.error("InflightTxDrainWaiter: timed out after {}ms with "
                    + "{} external write tx still in-flight on db={} — "
                    + "proceeding anyway, but orphan writes are possible. "
                    + "Recommend /cluster/fullsync on the demoted node.",
                    elapsed, lastCount, database);
                return elapsed;
            }

            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("InflightTxDrainWaiter: interrupted after {}ms", elapsed);
                return elapsed;
            }
        }
    }

    private long pollCount(Driver driver, String database) {
        try (Session session = driver.session()) {
            Result r = session.run(COUNT_QUERY, java.util.Map.of("db", database));
            if (!r.hasNext()) return 0;
            return r.single().get("n").asLong(0);
        }
    }
}
