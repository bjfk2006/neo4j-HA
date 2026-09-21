package com.neo4j.ha.agent.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.agent.audit.FailoverAuditLog;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GET /api/audit — merged view of:
 *   - {@code neo4j:ha:ui-audit} Redis Stream (login / op events, written by UiAuditLog)
 *   - {@code neo4j:ha:failover-history} Redis List (failover SUCCESS/FAILED records)
 *
 * <p>Query params:
 * <ul>
 *   <li>{@code limit} — page size, default 50, max 500</li>
 *   <li>{@code since} — exclusive stream id (for UI auto-continuation polling).
 *       List entries (failover-history) are returned only when this param is
 *       absent — they have no streamable ordering. The UI is expected to use
 *       {@code since} for incremental loads after the initial page.</li>
 * </ul>
 */
public class AuditController {

    private static final Logger log = LoggerFactory.getLogger(AuditController.class);

    private static final String UI_AUDIT_STREAM = "neo4j:ha:ui-audit";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JedisPool jedisPool;
    private final FailoverAuditLog failoverAuditLog;

    public AuditController(JedisPool jedisPool, FailoverAuditLog failoverAuditLog) {
        this.jedisPool = jedisPool;
        this.failoverAuditLog = failoverAuditLog;
    }

    public void getAudit(Context ctx) {
        int limit = parseLimit(ctx.queryParam("limit"));
        String since = ctx.queryParam("since");

        List<Map<String, Object>> entries = new ArrayList<>();

        try {
            entries.addAll(readUiAudit(limit, since));
        } catch (Exception e) {
            log.warn("Failed to read ui-audit stream: {}", e.toString());
        }

        // Only include failover-history on first page (since==null), since List
        // entries have no stream id we can resume from.
        if (since == null || since.isBlank()) {
            try {
                entries.addAll(readFailoverHistory(limit));
            } catch (Exception e) {
                log.warn("Failed to read failover-history list: {}", e.toString());
            }
        }

        entries.sort(Comparator.comparingLong(
            (Map<String, Object> m) -> ((Number) m.getOrDefault("ts", 0L)).longValue()
        ).reversed());

        if (entries.size() > limit) {
            entries = entries.subList(0, limit);
        }

        ctx.json(Map.of(
            "entries", entries,
            "count", entries.size()
        ));
    }

    private List<Map<String, Object>> readUiAudit(int limit, String since) {
        List<Map<String, Object>> out = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            // XRANGE start end COUNT limit; we want newest-first so use XREVRANGE
            // when since is null. With since, use XRANGE (since, +) ascending to
            // get only new entries.
            List<StreamEntry> entries;
            if (since == null || since.isBlank()) {
                // Jedis 5.x positional xrevrange(key, end, start, count).
                entries = jedis.xrevrange(UI_AUDIT_STREAM,
                    StreamEntryID.MAXIMUM_ID, StreamEntryID.MINIMUM_ID, limit);
            } else {
                StreamEntryID sinceId;
                try { sinceId = new StreamEntryID(since); }
                catch (Exception parse) { sinceId = StreamEntryID.MINIMUM_ID; }
                // xrange is inclusive on both ends, so we ask for limit+1 and
                // strip the entry that exactly equals `since` if present.
                entries = jedis.xrange(UI_AUDIT_STREAM,
                    sinceId, StreamEntryID.MAXIMUM_ID, limit + 1);
                if (!entries.isEmpty() && entries.get(0).getID().equals(sinceId)) {
                    entries = entries.subList(1, entries.size());
                }
            }
            for (StreamEntry e : entries) {
                Map<String, Object> m = new HashMap<>();
                m.put("source", "ui-audit");
                m.put("id", e.getID().toString());
                Map<String, String> f = e.getFields();
                long ts;
                try { ts = Long.parseLong(f.getOrDefault("ts", "0")); }
                catch (NumberFormatException nf) { ts = 0L; }
                m.put("ts", ts);
                m.put("type", f.getOrDefault("type", "unknown"));
                Map<String, String> details = new HashMap<>(f);
                details.remove("ts");
                details.remove("type");
                m.put("details", details);
                out.add(m);
            }
        }
        return out;
    }

    /**
     * REVIEW-C7: parse the JSON written by {@code FailoverAuditLog} and use the
     * event's REAL {@code endTime} as the sort key.
     *
     * <p>Previously the stored form was {@code FailoverEvent.toString()}, which
     * is unparseable, so this method fabricated a timestamp
     * ({@code now - index}). Consequence: every historical failover rendered as
     * having happened "just now" and, because the merged list is sorted by
     * {@code ts} descending, they always floated above the genuinely recent
     * UI-audit entries. The one view an operator opens to reconstruct an
     * incident timeline was the one view that could not show a timeline.</p>
     *
     * <p>Entries written by an older build are still legacy {@code toString()}
     * text; those are surfaced as before (raw + synthetic ts) but explicitly
     * flagged so nobody trusts their ordering.</p>
     */
    private List<Map<String, Object>> readFailoverHistory(int limit) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String raw : failoverAuditLog.getHistory(limit)) {
            Map<String, Object> m = new HashMap<>();
            m.put("source", "failover-history");
            m.put("type", "failover.record");
            try {
                JsonNode n = MAPPER.readTree(raw);
                m.put("id", text(n, "eventId"));
                long endTime = n.hasNonNull("endTime") ? n.get("endTime").asLong() : 0L;
                m.put("ts", endTime);
                Map<String, Object> details = new LinkedHashMap<>();
                details.put("failedNodeId", text(n, "failedNodeId"));
                details.put("newPrimaryId", text(n, "newPrimaryId"));
                details.put("fencingToken", n.hasNonNull("fencingToken")
                    ? n.get("fencingToken").asLong() : 0L);
                details.put("startTime", n.hasNonNull("startTime")
                    ? n.get("startTime").asLong() : 0L);
                details.put("endTime", endTime);
                details.put("durationMs", n.hasNonNull("startTime") && endTime > 0
                    ? endTime - n.get("startTime").asLong() : null);
                details.put("result", text(n, "result"));
                details.put("reason", text(n, "reason"));
                m.put("details", details);
            } catch (Exception parseFailure) {
                // Pre-REVIEW-C7 entry: opaque record text, no usable timestamp.
                m.put("id", null);
                m.put("ts", 0L);
                m.put("details", Map.of(
                    "raw", raw,
                    "legacyFormat", true,
                    "note", "written before REVIEW-C7; timestamp unknown, ordering unreliable"));
            }
            out.add(m);
        }
        return out;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static int parseLimit(String s) {
        if (s == null) return 50;
        try {
            int n = Integer.parseInt(s);
            if (n < 1) return 1;
            if (n > 500) return 500;
            return n;
        } catch (NumberFormatException e) {
            return 50;
        }
    }
}
