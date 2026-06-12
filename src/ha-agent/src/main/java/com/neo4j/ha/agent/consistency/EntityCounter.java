package com.neo4j.ha.agent.consistency;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Counts nodes / relationships / per-label distribution on a single Neo4j
 * node. Each cypher call is bounded by a wall-clock timeout so a slow / hung
 * standby cannot stall the whole stats response.
 */
public class EntityCounter {

    private static final Logger log = LoggerFactory.getLogger(EntityCounter.class);

    private static final long DEFAULT_QUERY_TIMEOUT_MS = 10_000L;

    /**
     * Shared executor. Cached pool with reasonable cap so concurrent
     * {@code /api/cluster/data-stats} requests don't fork unbounded threads.
     * 60s keep-alive lets idle threads die between rare invocations.
     */
    private static final ExecutorService SHARED_EXECUTOR = new ThreadPoolExecutor(
        0, 32, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        r -> {
            Thread t = new Thread(r, "entity-counter");
            t.setDaemon(true);
            return t;
        });

    private final String database;
    private final long timeoutMs;

    public EntityCounter(String database) {
        this(database, DEFAULT_QUERY_TIMEOUT_MS);
    }

    public EntityCounter(String database, long timeoutMs) {
        this.database = database;
        this.timeoutMs = timeoutMs;
    }

    public record CountResult(
        Long nodeCount,
        Long relCount,
        Map<String, Long> byLabel,
        long queryDurationMs,
        String error
    ) {
        public boolean isSuccess() { return error == null; }
    }

    public CountResult count(Driver driver) {
        long start = System.currentTimeMillis();
        if (driver == null) {
            return new CountResult(null, null, Map.of(), 0L, "driver not available");
        }

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            Long nodeCount = runWithTimeout(() -> session.run(
                "MATCH (n) RETURN count(n) AS c").single().get("c").asLong());

            Long relCount = runWithTimeout(() -> session.run(
                "MATCH ()-[r]->() RETURN count(r) AS c").single().get("c").asLong());

            // BUG-protected: labels(n) on a node deleted mid-tx throws, so wrap result
            // collection in a list snapshot, not a streaming consume.
            Map<String, Long> byLabel = runWithTimeout(() -> {
                var rows = session.run(
                    "MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS c "
                  + "ORDER BY c DESC LIMIT 100").list();
                Map<String, Long> map = new LinkedHashMap<>();
                for (var r : rows) {
                    String label = r.get("l").asString();
                    // Skip HA-internal labels that polluted the count.
                    if (label.startsWith("_CDC")) continue;
                    map.put(label, r.get("c").asLong());
                }
                return map;
            });

            long dur = System.currentTimeMillis() - start;
            return new CountResult(nodeCount, relCount, byLabel, dur, null);
        } catch (TimeoutException te) {
            long dur = System.currentTimeMillis() - start;
            log.warn("EntityCounter timed out after {}ms", dur);
            return new CountResult(null, null, Map.of(), dur, "timeout after " + timeoutMs + "ms");
        } catch (Exception e) {
            long dur = System.currentTimeMillis() - start;
            return new CountResult(null, null, Map.of(), dur, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private <T> T runWithTimeout(Callable<T> task) throws Exception {
        Future<T> f = SHARED_EXECUTOR.submit(task);
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            // Cancel the underlying task. Best-effort: the Bolt I/O may not be
            // interruptible — the Neo4j Java driver still respects the cancel
            // flag at session-close time, freeing the slot eventually.
            f.cancel(true);
            throw te;
        } catch (ExecutionException ee) {
            // Unwrap the underlying exception so handler can categorize.
            Throwable cause = ee.getCause();
            if (cause instanceof Exception ex2) throw ex2;
            throw new RuntimeException(cause);
        }
    }
}
