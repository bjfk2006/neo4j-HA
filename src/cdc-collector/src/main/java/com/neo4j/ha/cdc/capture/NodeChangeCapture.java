package com.neo4j.ha.cdc.capture;

import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.common.model.ChangeEventType;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detects node changes via Cypher polling using keyset pagination on {@code (_updated_at,
 * _elementId)}.
 *
 * <h3>BUG-045 fix — why we query per-label instead of {@code MATCH (n)}</h3>
 *
 * <p>The original implementation used a single {@code MATCH (n) WHERE n._updated_at > ...}
 * query. Neo4j 5.x range indexes are <b>label-scoped</b> (there is no "global property
 * index"), so a label-less {@code MATCH (n)} predicate cannot use the
 * {@code FOR (n:TestNode) ON (n._updated_at)} range index — the planner degrades to an
 * {@code AllNodesScan} + filter + sort. Once the primary database has more than a few
 * thousand nodes this makes every poll O(N log N) instead of O(log N + batchSize),
 * which in turn makes CDC miss bursts of writes (checkpoint advances past events that
 * were never published because the batch was dominated by older churn).</p>
 *
 * <p>The fix: at startup we enumerate user labels via {@code CALL db.labels()},
 * cache them (periodic refresh to pick up newly created labels), and for each label
 * run a dedicated label-scoped query that the planner can serve via
 * {@code NodeIndexSeekByRange}. Results from all labels are merged in-memory and
 * then trimmed to {@code batchSize} by the global {@code (ts, eid)} key. This is
 * safe because each per-label query is bounded by {@code batchSize}, so total work
 * is {@code O(labels × batchSize)} regardless of database size.</p>
 */
public class NodeChangeCapture {

    private static final Logger log = LoggerFactory.getLogger(NodeChangeCapture.class);

    // Per-label, index-friendly keyset query. Uses `n:Label` so the planner picks the
    // range index `FOR (n:Label) ON (n._updated_at)`.
    private static final String PER_LABEL_QUERY_TEMPLATE = """
        MATCH (n:%3$s)
        WHERE n.%1$s > $lastTs
           OR (n.%1$s = $lastTs AND n.%2$s > $lastEid)
        RETURN n, labels(n) AS labels, properties(n) AS props, n.%2$s AS eid
        ORDER BY n.%1$s ASC, n.%2$s ASC
        LIMIT $batchSize
        """;

    private final String timestampField;
    private final String createdAtField;
    private final String elementIdField;

    /** Cached user label list. Refreshed every {@link #LABEL_REFRESH_INTERVAL_MS} ms. */
    private volatile List<String> cachedLabels = List.of();
    private final AtomicLong lastLabelRefreshMs = new AtomicLong(0);
    private static final long LABEL_REFRESH_INTERVAL_MS = 30_000; // 30 s

    public NodeChangeCapture(String timestampField, String createdAtField, String elementIdField) {
        this.timestampField = timestampField;
        this.createdAtField = createdAtField;
        this.elementIdField = elementIdField;
    }

    public List<RawChange> detectChanges(Session session, long lastTs, String lastElementId, int batchSize) {
        List<String> labels = getLabels(session);
        if (labels.isEmpty()) {
            return List.of();
        }

        List<RawChange> merged = new ArrayList<>();
        for (String label : labels) {
            String sanitized = sanitizeLabel(label);
            String cypher = PER_LABEL_QUERY_TEMPLATE.formatted(
                timestampField, elementIdField, sanitized);
            try {
                var result = session.run(cypher, Map.of(
                    "lastTs", lastTs,
                    "lastEid", lastElementId != null ? lastElementId : "",
                    "batchSize", batchSize
                ));
                for (Record record : result.list()) {
                    Map<String, Object> props = record.get("props").asMap();
                    List<String> recLabels = record.get("labels").asList(Value::asString);
                    String eid = record.get("eid").asString();

                    long updatedAt = toLong(props.get(timestampField));
                    long createdAt = toLong(props.get(createdAtField));

                    ChangeEventType type = (createdAt == updatedAt)
                        ? ChangeEventType.NODE_CREATED
                        : ChangeEventType.NODE_UPDATED;

                    merged.add(new RawChange(type, eid, recLabels, props, null, null, null, null, updatedAt));
                }
            } catch (Exception e) {
                // Query failure for one label must not block the whole poll. Log and continue.
                log.warn("Node change detection failed for label '{}': {}", label, e.getMessage());
            }
        }

        // Global keyset order + global batch cap. Across labels we may have collected up
        // to (labels × batchSize) rows; trim to batchSize by the global keyset.
        merged.sort(
            Comparator.<RawChange>comparingLong(RawChange::timestamp)
                .thenComparing(RawChange::elementId));
        if (merged.size() > batchSize) {
            merged = new ArrayList<>(merged.subList(0, batchSize));
        }

        log.debug("Detected {} node changes across {} labels", merged.size(), labels.size());
        return merged;
    }

    /**
     * Return the cached user-label list, refreshing from {@code db.labels()} if stale.
     * Labels starting with {@code _} are considered system/internal (e.g. {@code
     * _CDCDeleteEvent}) and are excluded from CDC capture.
     */
    private List<String> getLabels(Session session) {
        long now = System.currentTimeMillis();
        long last = lastLabelRefreshMs.get();
        if (now - last < LABEL_REFRESH_INTERVAL_MS && !cachedLabels.isEmpty()) {
            return cachedLabels;
        }
        // Best-effort refresh. If it fails we keep the previous cache.
        try {
            List<String> fresh = session.run("CALL db.labels() YIELD label RETURN label")
                .list(r -> r.get("label").asString());
            fresh = fresh.stream()
                .filter(l -> l != null && !l.isEmpty() && !l.startsWith("_"))
                .toList();
            this.cachedLabels = fresh;
            this.lastLabelRefreshMs.set(now);
            log.debug("Refreshed label cache: {} user labels", fresh.size());
        } catch (Exception e) {
            log.warn("Failed to refresh label cache ({}); using previous cache of {} labels",
                e.getMessage(), cachedLabels.size());
        }
        return cachedLabels;
    }

    private long toLong(Object value) {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        return 0;
    }

    /**
     * Quote a label for safe inclusion in Cypher. We mirror the logic used in
     * {@code IndexInstaller}/{@code IndexManager} so indexes and captures reference
     * the same name shape.
     */
    private static String sanitizeLabel(String label) {
        if (label.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return label;
        }
        return "`" + label.replace("`", "``") + "`";
    }
}
