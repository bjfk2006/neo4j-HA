package com.neo4j.ha.cdc.capture;

import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.common.model.ChangeEventType;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detect relationship changes via Cypher polling with keyset pagination on
 * {@code (_updated_at, _elementId)}.
 *
 * <p>BUG-045 fix: see {@link NodeChangeCapture} for the rationale behind per-type
 * query + in-memory merge. For relationships we enumerate types via
 * {@code CALL db.relationshipTypes()} and execute a type-scoped query per type so
 * the planner can use {@code FOR ()-[r:Type]-() ON (r._updated_at)} range indexes.</p>
 */
public class RelationshipChangeCapture {

    private static final Logger log = LoggerFactory.getLogger(RelationshipChangeCapture.class);

    // BUG-053 fix: use the cluster-stable `_elementId` PROPERTY of the endpoint
    // nodes, not the local `elementId(a)` / `elementId(b)`. Same root cause as
    // BUG-052: after switchover, the new primary''s local node id differs from
    // every standby''s — sync-applier on the standby looks up endpoints by
    // `_elementId` property, so publishing the local id makes the
    // `MATCH (a {_elementId: $startNodeId})` step fail and the relationship
    // MERGE silently drops. Coalesce falls back to local id only for legacy
    // nodes that never carried `_elementId` (pre-trigger data).
    private static final String PER_TYPE_QUERY_TEMPLATE = """
        MATCH (a)-[r:%3$s]->(b)
        WHERE r.%1$s > $lastTs
           OR (r.%1$s = $lastTs AND r.%2$s > $lastRelEid)
        RETURN r, type(r) AS relType, properties(r) AS props, r.%2$s AS eid,
               coalesce(a.%2$s, elementId(a)) AS startEid,
               coalesce(b.%2$s, elementId(b)) AS endEid,
               labels(a) AS startLabels, labels(b) AS endLabels
        ORDER BY r.%1$s ASC, r.%2$s ASC
        LIMIT $batchSize
        """;

    private final String timestampField;
    private final String createdAtField;
    private final String elementIdField;

    private volatile List<String> cachedTypes = List.of();
    private final AtomicLong lastTypeRefreshMs = new AtomicLong(0);
    private static final long TYPE_REFRESH_INTERVAL_MS = 30_000;

    public RelationshipChangeCapture(String timestampField, String createdAtField, String elementIdField) {
        this.timestampField = timestampField;
        this.createdAtField = createdAtField;
        this.elementIdField = elementIdField;
    }

    public List<RawChange> detectChanges(Session session, long lastTs, String lastRelEid, int batchSize) {
        List<String> types = getTypes(session);
        if (types.isEmpty()) {
            return List.of();
        }

        List<RawChange> merged = new ArrayList<>();
        for (String rt : types) {
            String sanitized = sanitizeRelType(rt);
            String cypher = PER_TYPE_QUERY_TEMPLATE.formatted(
                timestampField, elementIdField, sanitized);
            try {
                var result = session.run(cypher, Map.of(
                    "lastTs", lastTs,
                    "lastRelEid", lastRelEid != null ? lastRelEid : "",
                    "batchSize", batchSize
                ));
                for (Record record : result.list()) {
                    Map<String, Object> props = record.get("props").asMap();
                    String relType = record.get("relType").asString();
                    String eid = record.get("eid").asString();
                    String startEid = record.get("startEid").asString();
                    String endEid = record.get("endEid").asString();
                    List<String> startLabels = record.get("startLabels").asList(Value::asString);
                    List<String> endLabels = record.get("endLabels").asList(Value::asString);

                    long updatedAt = toLong(props.get(timestampField));
                    long createdAt = toLong(props.get(createdAtField));

                    ChangeEventType type = (createdAt == updatedAt)
                        ? ChangeEventType.RELATIONSHIP_CREATED
                        : ChangeEventType.RELATIONSHIP_UPDATED;

                    merged.add(new RawChange(type, eid, Collections.emptyList(), props, null,
                        relType, startEid, endEid, updatedAt, startLabels, endLabels));
                }
            } catch (Exception e) {
                log.warn("Relationship change detection failed for type '{}': {}", rt, e.getMessage());
            }
        }

        merged.sort(
            Comparator.<RawChange>comparingLong(RawChange::timestamp)
                .thenComparing(RawChange::elementId));
        if (merged.size() > batchSize) {
            merged = new ArrayList<>(merged.subList(0, batchSize));
        }

        log.debug("Detected {} relationship changes across {} types", merged.size(), types.size());
        return merged;
    }

    private List<String> getTypes(Session session) {
        long now = System.currentTimeMillis();
        long last = lastTypeRefreshMs.get();
        if (now - last < TYPE_REFRESH_INTERVAL_MS && !cachedTypes.isEmpty()) {
            return cachedTypes;
        }
        try {
            List<String> fresh = session.run(
                    "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                .list(r -> r.get("relationshipType").asString());
            fresh = fresh.stream()
                .filter(t -> t != null && !t.isEmpty() && !t.startsWith("_"))
                .toList();
            this.cachedTypes = fresh;
            this.lastTypeRefreshMs.set(now);
            log.debug("Refreshed relationship type cache: {} user types", fresh.size());
        } catch (Exception e) {
            log.warn("Failed to refresh relationship type cache ({}); using previous cache of {} types",
                e.getMessage(), cachedTypes.size());
        }
        return cachedTypes;
    }

    private long toLong(Object value) {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        return 0;
    }

    private static String sanitizeRelType(String t) {
        if (t.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return t;
        }
        return "`" + t.replace("`", "``") + "`";
    }
}
