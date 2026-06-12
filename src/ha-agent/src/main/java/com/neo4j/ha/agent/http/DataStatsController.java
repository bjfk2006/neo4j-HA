package com.neo4j.ha.agent.http;

import com.neo4j.ha.agent.consistency.EntityCounter;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.model.NodeInfo;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Phase 1 of the data-consistency feature.
 *
 * <p>GET /api/cluster/data-stats — returns per-node count(n) / count(r) /
 * label distribution. All nodes are queried in parallel; slow nodes timeout
 * independently (10s wall-clock each) and are reported as {@code error}
 * fields rather than failing the whole request.</p>
 */
public class DataStatsController {

    private static final Logger log = LoggerFactory.getLogger(DataStatsController.class);

    private final ClusterStateManager clusterState;
    private final String database;

    public DataStatsController(ClusterStateManager clusterState, String database) {
        this.clusterState = clusterState;
        this.database = database;
    }

    public void getStats(Context ctx) {
        long start = System.currentTimeMillis();
        List<NodeInfo> nodes = clusterState.getAllNodes();
        EntityCounter counter = new EntityCounter(database);

        ExecutorService pool = Executors.newFixedThreadPool(
            Math.max(1, Math.min(nodes.size(), 8)),
            r -> { Thread t = new Thread(r, "data-stats"); t.setDaemon(true); return t; }
        );

        try {
            Map<String, Future<EntityCounter.CountResult>> futures = new HashMap<>();
            for (NodeInfo node : nodes) {
                var driver = clusterState.getDriver(node.id());
                futures.put(node.id(), pool.submit(() -> counter.count(driver)));
            }

            List<Map<String, Object>> nodeResults = new ArrayList<>();
            Long nodeCountMax = null, nodeCountMin = null;
            Long relCountMax = null, relCountMin = null;

            for (NodeInfo node : nodes) {
                EntityCounter.CountResult r;
                try {
                    r = futures.get(node.id()).get(15, TimeUnit.SECONDS);
                } catch (Exception e) {
                    r = new EntityCounter.CountResult(null, null, Map.of(), 0L,
                        "future failed: " + e.getMessage());
                }

                Map<String, Object> nodeJson = new LinkedHashMap<>();
                nodeJson.put("id", node.id());
                nodeJson.put("role", node.role().name());
                nodeJson.put("health", node.health().name());
                nodeJson.put("nodeCount", r.nodeCount());
                nodeJson.put("relCount", r.relCount());
                nodeJson.put("byLabel", r.byLabel());
                nodeJson.put("queryDurationMs", r.queryDurationMs());
                if (r.error() != null) nodeJson.put("error", r.error());
                nodeResults.add(nodeJson);

                if (r.nodeCount() != null) {
                    nodeCountMax = nodeCountMax == null ? r.nodeCount() : Math.max(nodeCountMax, r.nodeCount());
                    nodeCountMin = nodeCountMin == null ? r.nodeCount() : Math.min(nodeCountMin, r.nodeCount());
                }
                if (r.relCount() != null) {
                    relCountMax = relCountMax == null ? r.relCount() : Math.max(relCountMax, r.relCount());
                    relCountMin = relCountMin == null ? r.relCount() : Math.min(relCountMin, r.relCount());
                }
            }

            Map<String, Object> nodeDrift = new LinkedHashMap<>();
            nodeDrift.put("max", nodeCountMax);
            nodeDrift.put("min", nodeCountMin);
            nodeDrift.put("drift", (nodeCountMax == null || nodeCountMin == null) ? null : nodeCountMax - nodeCountMin);

            Map<String, Object> relDrift = new LinkedHashMap<>();
            relDrift.put("max", relCountMax);
            relDrift.put("min", relCountMin);
            relDrift.put("drift", (relCountMax == null || relCountMin == null) ? null : relCountMax - relCountMin);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ts", System.currentTimeMillis());
            body.put("primary", clusterState.getPrimaryNodeId());
            body.put("nodes", nodeResults);
            body.put("diff", Map.of("nodeCount", nodeDrift, "relCount", relDrift));
            body.put("scanDurationMs", System.currentTimeMillis() - start);
            ctx.json(body);
        } finally {
            pool.shutdownNow();
        }
    }
}
