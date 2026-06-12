package com.neo4j.ha.agent.http;

import com.neo4j.ha.agent.consistency.DiffEngine;
import com.neo4j.ha.agent.http.auth.AuthFilter;
import com.neo4j.ha.agent.http.auth.Principal;
import com.neo4j.ha.agent.http.auth.UiAuditLog;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.model.NodeRole;
import io.javalin.http.Context;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 2 of the data-consistency feature.
 *
 * <p>GET /api/cluster/data-diff?scope=recent&limit=1000&type=both&nodeId=node-03</p>
 *
 * <p>Per-standby rate limiting: a given standby may only be scanned once
 * every 60 seconds (independent of who triggers it). Returns 429 with
 * Retry-After when on cooldown.</p>
 */
public class DataDiffController {

    private static final Logger log = LoggerFactory.getLogger(DataDiffController.class);

    private static final long COOLDOWN_MS = 60_000L;
    private static final int DEFAULT_LIMIT = 1000;
    private static final int MIN_LIMIT = 100;
    private static final int MAX_LIMIT = 10_000;

    /**
     * Threshold above which RECENT scope without a label is rejected. The
     * per-label `_updated_at` index can't be used by a label-less query, so
     * RECENT on a large graph degrades into a full-graph scan that pins the
     * primary's IO. Operator must narrow with `scope=label&scopeArg=<L>` or
     * accept a much smaller limit.
     */
    private static final long LARGE_GRAPH_THRESHOLD_NODES = 100_000L;
    private static final int LARGE_GRAPH_RECENT_LIMIT_CAP = 200;

    private final ClusterStateManager clusterState;
    private final String database;
    private final UiAuditLog uiAuditLog;

    /** Per-standby last-scan timestamp for cooldown enforcement. */
    private final Map<String, AtomicLong> lastScanByNode = new ConcurrentHashMap<>();

    /**
     * Last full-fan-out scan timestamp (nodeId not specified, i.e. compared
     * against ALL standbys). 30s global cooldown so an attacker / runaway
     * client can't repeatedly fork the heaviest variant.
     */
    private final AtomicLong lastFullScan = new AtomicLong(0L);
    private static final long FULL_SCAN_COOLDOWN_MS = 30_000L;

    public DataDiffController(ClusterStateManager clusterState, String database,
                              UiAuditLog uiAuditLog) {
        this.clusterState = clusterState;
        this.database = database;
        this.uiAuditLog = uiAuditLog;
    }

    public void getDiff(Context ctx) {
        // Parse + validate query params.
        DiffEngine.Scope scope = parseScope(ctx.queryParam("scope"));
        DiffEngine.Kind kind = parseKind(ctx.queryParam("type"));
        int limit = parseLimit(ctx.queryParam("limit"));
        String scopeArg = ctx.queryParam("scopeArg"); // label name if scope=label
        String nodeId = ctx.queryParam("nodeId");      // optional: a specific standby

        // Pre-flight: refuse RECENT scope on large graphs unless caller agreed
        // to a small limit. Label-less ORDER BY _updated_at cannot use the
        // per-label index in IndexInstaller, degrading to full-graph scan.
        if (scope == DiffEngine.Scope.RECENT) {
            Long nodeCount = peekPrimaryNodeCount();
            if (nodeCount != null && nodeCount > LARGE_GRAPH_THRESHOLD_NODES
                    && limit > LARGE_GRAPH_RECENT_LIMIT_CAP) {
                ctx.status(400).json(Map.of(
                    "error", "scope_too_wide",
                    "message", String.format(
                        "Graph has %d nodes > %d threshold; RECENT scope on a label-less "
                      + "scan would full-scan the primary. Either narrow with "
                      + "scope=label&scopeArg=<LABEL>, or reduce limit to <= %d.",
                        nodeCount, LARGE_GRAPH_THRESHOLD_NODES, LARGE_GRAPH_RECENT_LIMIT_CAP),
                    "graphNodeCount", nodeCount,
                    "maxAllowedLimitForRecent", LARGE_GRAPH_RECENT_LIMIT_CAP
                ));
                return;
            }
        }

        // Cooldown:
        //  - per-standby: 60s if a specific nodeId is targeted
        //  - global full-scan: 30s otherwise (covers ALL standbys, more expensive)
        long now = System.currentTimeMillis();
        if (nodeId != null && !nodeId.isBlank()) {
            AtomicLong last = lastScanByNode.computeIfAbsent(nodeId, k -> new AtomicLong(0));
            long since = now - last.get();
            if (since < COOLDOWN_MS) {
                long retryAfter = COOLDOWN_MS - since;
                ctx.status(429)
                    .header("Retry-After", String.valueOf(retryAfter / 1000))
                    .json(Map.of(
                        "error", "cooldown",
                        "message", "Diff scan on cooldown for " + nodeId
                                 + "; retry in " + retryAfter + " ms",
                        "retryAfterMs", retryAfter
                    ));
                return;
            }
            last.set(now);
        } else {
            long since = now - lastFullScan.get();
            if (since < FULL_SCAN_COOLDOWN_MS) {
                long retryAfter = FULL_SCAN_COOLDOWN_MS - since;
                ctx.status(429)
                    .header("Retry-After", String.valueOf(retryAfter / 1000))
                    .json(Map.of(
                        "error", "cooldown",
                        "message", "Full-fan-out diff scan on cooldown; retry in "
                                 + retryAfter + " ms, or pass nodeId=<standby> to target a single node",
                        "retryAfterMs", retryAfter
                    ));
                return;
            }
            lastFullScan.set(now);
        }

        // Resolve primary + standby set.
        String primaryId = clusterState.getPrimaryNodeId();
        Driver primaryDriver = primaryId == null ? null : clusterState.getDriver(primaryId);
        Map<String, Driver> standbys = new java.util.LinkedHashMap<>();
        for (var n : clusterState.getAllNodes()) {
            if (n.role() == NodeRole.STANDBY) {
                if (nodeId != null && !nodeId.isBlank() && !n.id().equals(nodeId)) continue;
                Driver d = clusterState.getDriver(n.id());
                if (d != null) standbys.put(n.id(), d);
            }
        }

        if (standbys.isEmpty()) {
            ctx.status(400).json(Map.of(
                "error", "bad_request",
                "message", nodeId != null
                    ? "No matching standby node: " + nodeId
                    : "Cluster has no standby nodes to diff against"
            ));
            return;
        }

        // Audit: data-diff scans are read-only but expensive and disclose
        // property contents. Record the actor + parameters before doing work.
        if (uiAuditLog != null) {
            Principal p = AuthFilter.principal(ctx);
            String actor = p == null ? "unknown" : p.displayActor();
            String requestId = UUID.randomUUID().toString();
            ctx.header("X-Request-Id", requestId);
            String params = String.format("scope=%s,scopeArg=%s,limit=%d,type=%s,nodeId=%s",
                scope.name().toLowerCase(),
                scopeArg == null ? "" : scopeArg,
                limit,
                kind.name().toLowerCase(),
                nodeId == null ? "*" : nodeId);
            uiAuditLog.logOperation("consistency-scan", actor, ctx.ip(), params, requestId);
        }

        DiffEngine engine = new DiffEngine(database);
        long t0 = System.currentTimeMillis();
        var diffs = engine.diff(primaryDriver, standbys, scope, scopeArg, limit, kind);
        long scanMs = System.currentTimeMillis() - t0;

        // Build JSON response.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ts", System.currentTimeMillis());
        body.put("primary", primaryId);
        body.put("scope", scope.name().toLowerCase());
        if (scopeArg != null) body.put("scopeArg", scopeArg);
        body.put("limit", limit);
        body.put("type", kind.name().toLowerCase());
        body.put("scanDurationMs", scanMs);

        Map<String, Object> diffOut = new LinkedHashMap<>();
        for (var entry : diffs.entrySet()) {
            var nd = entry.getValue();
            Map<String, Object> sNode = new LinkedHashMap<>();
            if (nd.error() != null) {
                sNode.put("error", nd.error());
            } else {
                sNode.put("matched", nd.matched());
                sNode.put("missing", toJsonList(nd.missing()));
                sNode.put("extra", toJsonList(nd.extra()));
                sNode.put("propDiff", toJsonList(nd.propDiff()));
                sNode.put("counts", Map.of(
                    "missing", nd.missing().size(),
                    "extra", nd.extra().size(),
                    "propDiff", nd.propDiff().size()
                ));
            }
            diffOut.put(entry.getKey(), sNode);
        }
        body.put("diff", diffOut);
        ctx.json(body);
    }

    private static java.util.List<Map<String, Object>> toJsonList(java.util.List<DiffEngine.DiffEntry> entries) {
        java.util.List<Map<String, Object>> out = new java.util.ArrayList<>(entries.size());
        for (var e : entries) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("elementId", e.elementId());
            m.put("kind", e.kind());
            m.put("labels", e.labels());
            if (e.primaryProps() != null)  m.put("primaryProps", e.primaryProps());
            if (e.standbyProps() != null)  m.put("standbyProps", e.standbyProps());
            if (e.primaryHash() != null)   m.put("primaryHash", e.primaryHash());
            if (e.standbyHash() != null)   m.put("standbyHash", e.standbyHash());
            if (e.delta() != null && !e.delta().isEmpty()) m.put("delta", e.delta());
            out.add(m);
        }
        return out;
    }

    private static DiffEngine.Scope parseScope(String s) {
        if (s == null) return DiffEngine.Scope.RECENT;
        return switch (s.toLowerCase()) {
            case "label" -> DiffEngine.Scope.LABEL;
            case "random" -> DiffEngine.Scope.RANDOM;
            default -> DiffEngine.Scope.RECENT;
        };
    }

    private static DiffEngine.Kind parseKind(String s) {
        if (s == null) return DiffEngine.Kind.BOTH;
        return switch (s.toLowerCase()) {
            case "node" -> DiffEngine.Kind.NODE;
            case "rel"  -> DiffEngine.Kind.REL;
            default     -> DiffEngine.Kind.BOTH;
        };
    }

    private static int parseLimit(String s) {
        if (s == null) return DEFAULT_LIMIT;
        try {
            int n = Integer.parseInt(s);
            if (n < MIN_LIMIT) return MIN_LIMIT;
            if (n > MAX_LIMIT) return MAX_LIMIT;
            return n;
        } catch (NumberFormatException e) {
            return DEFAULT_LIMIT;
        }
    }

    /** Cheap, single-query node count on the primary. Returns null on failure. */
    private Long peekPrimaryNodeCount() {
        String primaryId = clusterState.getPrimaryNodeId();
        if (primaryId == null) return null;
        Driver d = clusterState.getDriver(primaryId);
        if (d == null) return null;
        try (var s = d.session(org.neo4j.driver.SessionConfig.forDatabase(database))) {
            return s.run("MATCH (n) RETURN count(n) AS c").single().get("c").asLong();
        } catch (Exception e) {
            log.warn("data-diff pre-flight: primary node-count peek failed: {}", e.toString());
            return null;
        }
    }
}
