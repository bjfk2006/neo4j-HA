package com.neo4j.ha.agent.http;

import com.neo4j.ha.agent.backup.BackupCoordinator;
import com.neo4j.ha.agent.failover.FailoverOrchestrator;
import com.neo4j.ha.agent.http.auth.AuthFilter;
import com.neo4j.ha.agent.http.auth.Principal;
import com.neo4j.ha.agent.http.auth.UiAuditLog;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.cdc.CdcCollector;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.metrics.MetricsRegistry;
import com.neo4j.ha.common.model.NodeInfo;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Admin HTTP server. Hosts:
 * <ul>
 *   <li>The Web UI static bundle (when packaged) at {@code /} +
 *       {@code /assets/**}</li>
 *   <li>The {@code /api/**} REST endpoints used by the UI and external
 *       admin clients</li>
 *   <li>Backward-compatible {@code /cluster/**} {@code /health}
 *       {@code /metrics} paths used by existing curl scripts</li>
 * </ul>
 *
 * <p>Auth is enforced by {@link AuthFilter} in a global {@code before}
 * handler; writes additionally call {@link AuthFilter#requireWriter} from
 * inside their handler for RBAC.</p>
 */
public class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private final int port;
    private final ClusterStateManager clusterState;
    private final FailoverOrchestrator failoverOrchestrator;
    private final CdcCollector cdcCollector;
    private final BackupCoordinator backupCoordinator;

    // New deps (nullable when UI disabled)
    private final AuthFilter authFilter;
    private final AuthController authController;
    private final AuditController auditController;
    private final MetricsSummaryController metricsSummaryController;
    private final DataStatsController dataStatsController;
    private final DataDiffController dataDiffController;
    private final UiAuditLog uiAuditLog;
    private final HaMetrics metrics;
    private final boolean uiEnabled;

    private Javalin app;

    public AdminHttpServer(int port,
                           ClusterStateManager clusterState,
                           FailoverOrchestrator failoverOrchestrator,
                           CdcCollector cdcCollector,
                           BackupCoordinator backupCoordinator,
                           AuthFilter authFilter,
                           AuthController authController,
                           AuditController auditController,
                           MetricsSummaryController metricsSummaryController,
                           DataStatsController dataStatsController,
                           DataDiffController dataDiffController,
                           UiAuditLog uiAuditLog,
                           HaMetrics metrics,
                           boolean uiEnabled) {
        this.port = port;
        this.clusterState = clusterState;
        this.failoverOrchestrator = failoverOrchestrator;
        this.cdcCollector = cdcCollector;
        this.backupCoordinator = backupCoordinator;
        this.authFilter = authFilter;
        this.authController = authController;
        this.auditController = auditController;
        this.metricsSummaryController = metricsSummaryController;
        this.dataStatsController = dataStatsController;
        this.dataDiffController = dataDiffController;
        this.uiAuditLog = uiAuditLog;
        this.metrics = metrics;
        this.uiEnabled = uiEnabled;
    }

    public void start() {
        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            if (uiEnabled) {
                // Serve the Vue build output. Resources are looked up on the
                // classpath at /static (i.e. src/ha-agent/src/main/resources/static).
                // hostedPath="/" makes /index.html the root, /assets/* the JS/CSS.
                config.staticFiles.add(staticFiles -> {
                    staticFiles.hostedPath = "/";
                    staticFiles.directory = "/static";
                    staticFiles.location = Location.CLASSPATH;
                });
                // SPA fallback: when the browser refreshes a client-side route
                // like /login, /dashboard, /audit, the server otherwise 404s.
                // Send index.html for any GET that doesn't match an API path or
                // a static file, letting Vue Router resolve it client-side.
                config.spaRoot.addFile("/", "/static/index.html", Location.CLASSPATH);
            }
        });

        // Global auth filter (whitelists static + login + health + metrics).
        app.before(authFilter::handle);

        // Request metrics (path-level Counter + Timer).
        app.after(ctx -> {
            try {
                long durMs = -1L;
                Object startedAt = ctx.attribute("__startedAt");
                if (startedAt instanceof Long l) durMs = System.currentTimeMillis() - l;
                metrics.recordUiApiRequest(
                    ctx.matchedPath() != null ? ctx.matchedPath() : ctx.path(),
                    ctx.method().name(),
                    ctx.status().getCode(),
                    Math.max(0L, durMs));
            } catch (Exception e) {
                // metrics must not affect responses
            }
        });
        app.before(ctx -> ctx.attribute("__startedAt", System.currentTimeMillis()));

        // === Public / unauthenticated ===
        app.get("/health", ctx -> ctx.json(Map.of("status", "UP")));
        app.get("/metrics", ctx -> {
            ctx.contentType("text/plain");
            ctx.result(MetricsRegistry.scrape());
        });

        // === /api/auth/* ===
        if (uiEnabled) {
            app.post("/api/login", authController::login);
            app.post("/api/logout", authController::logout);
            app.get("/api/me", authController::me);
        }

        // === /api/cluster/* (auth required; writes need ADMIN role) ===
        app.get("/api/cluster/status", this::handleClusterStatus);
        app.get("/api/cluster/nodes/{id}", this::handleNodeDetail);
        app.post("/api/cluster/failover", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            if (nodeId == null) nodeId = clusterState.getPrimaryNodeId();
            final String finalNodeId = nodeId;
            auditOp(ctx, "failover", "nodeId=" + finalNodeId);
            new Thread(() -> failoverOrchestrator.executeFailover(finalNodeId, false),
                "admin-failover").start();
            ctx.json(Map.of("status", "failover_initiated", "targetNode", finalNodeId));
        });
        app.post("/api/cluster/switchover", ctx -> {
            AuthFilter.requireWriter(ctx);
            String targetNodeId = ctx.queryParam("targetNodeId");
            auditOp(ctx, "switchover", "targetNodeId=" + targetNodeId);
            new Thread(() -> failoverOrchestrator.executeSwitchover(targetNodeId),
                "admin-switchover").start();
            ctx.json(Map.of(
                "status", "switchover_initiated",
                "targetNode", targetNodeId != null ? targetNodeId : "auto-select"
            ));
        });
        app.post("/api/cluster/fullsync", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            if (nodeId == null || nodeId.isBlank()) {
                ctx.status(400).json(Map.of("error", "bad_request",
                    "message", "nodeId is required"));
                return;
            }
            final String finalNodeId = nodeId;
            auditOp(ctx, "fullsync", "nodeId=" + finalNodeId);
            new Thread(() -> cdcCollector.createFullSyncCoordinator().startFullSync(finalNodeId),
                "admin-fullsync").start();
            ctx.json(Map.of("status", "fullsync_triggered", "targetNode", finalNodeId));
        });
        app.post("/api/cluster/backup/prepare", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            auditOp(ctx, "backup.prepare", "nodeId=" + nodeId);
            var result = backupCoordinator.prepare(nodeId);
            ctx.json(Map.of("status", "prepared",
                "prepareTime", result.prepareTime().toString()));
        });
        app.post("/api/cluster/backup/complete", ctx -> {
            AuthFilter.requireWriter(ctx);
            auditOp(ctx, "backup.complete", "");
            backupCoordinator.complete();
            ctx.json(Map.of("status", "completed"));
        });
        app.get("/api/cluster/backup/status", ctx -> ctx.json(Map.of(
            "state", backupCoordinator.getState().name(),
            "lastBackupTime", backupCoordinator.getLastBackupTime() != null
                ? backupCoordinator.getLastBackupTime().toString() : "never"
        )));

        // === /api/audit, /api/metrics-summary ===
        app.get("/api/audit", auditController::getAudit);
        app.get("/api/metrics-summary", metricsSummaryController::getSummary);

        // === /api/cluster/data-stats, /api/cluster/data-diff (Phase 1+2) ===
        app.get("/api/cluster/data-stats", dataStatsController::getStats);
        app.get("/api/cluster/data-diff", dataDiffController::getDiff);

        // === Backward-compat aliases (old /cluster/* paths still in use by existing curl) ===
        // These re-use the SAME handlers, just under their legacy paths.
        app.get("/cluster/status", this::handleClusterStatus);
        app.get("/cluster/nodes/{id}", this::handleNodeDetail);
        app.post("/cluster/failover", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            if (nodeId == null) nodeId = clusterState.getPrimaryNodeId();
            final String finalNodeId = nodeId;
            auditOp(ctx, "failover", "nodeId=" + finalNodeId);
            new Thread(() -> failoverOrchestrator.executeFailover(finalNodeId, false),
                "admin-failover-legacy").start();
            ctx.json(Map.of("status", "failover_initiated", "targetNode", finalNodeId));
        });
        app.post("/cluster/switchover", ctx -> {
            AuthFilter.requireWriter(ctx);
            String targetNodeId = ctx.queryParam("targetNodeId");
            auditOp(ctx, "switchover", "targetNodeId=" + targetNodeId);
            new Thread(() -> failoverOrchestrator.executeSwitchover(targetNodeId),
                "admin-switchover-legacy").start();
            ctx.json(Map.of("status", "switchover_initiated",
                "targetNode", targetNodeId != null ? targetNodeId : "auto-select"));
        });
        app.post("/cluster/fullsync", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            if (nodeId == null || nodeId.isBlank()) {
                ctx.status(400).json(Map.of("error", "nodeId is required"));
                return;
            }
            final String finalNodeId = nodeId;
            auditOp(ctx, "fullsync", "nodeId=" + finalNodeId);
            new Thread(() -> cdcCollector.createFullSyncCoordinator().startFullSync(finalNodeId),
                "admin-fullsync-legacy").start();
            ctx.json(Map.of("status", "fullsync_triggered", "targetNode", finalNodeId));
        });
        app.post("/cluster/backup/prepare", ctx -> {
            AuthFilter.requireWriter(ctx);
            String nodeId = ctx.queryParam("nodeId");
            auditOp(ctx, "backup.prepare", "nodeId=" + nodeId);
            var result = backupCoordinator.prepare(nodeId);
            ctx.json(Map.of("status", "prepared",
                "prepareTime", result.prepareTime().toString()));
        });
        app.post("/cluster/backup/complete", ctx -> {
            AuthFilter.requireWriter(ctx);
            auditOp(ctx, "backup.complete", "");
            backupCoordinator.complete();
            ctx.json(Map.of("status", "completed"));
        });
        app.get("/cluster/backup/status", ctx -> ctx.json(Map.of(
            "state", backupCoordinator.getState().name(),
            "lastBackupTime", backupCoordinator.getLastBackupTime() != null
                ? backupCoordinator.getLastBackupTime().toString() : "never"
        )));

        app.start(port);
        log.info("Admin HTTP server started on port {} (ui={})", port, uiEnabled);
    }

    private void handleClusterStatus(Context ctx) {
        // v2: which node (if any) is currently in backup-maintenance window.
        // UI uses this to render a 🔧 maint badge on the affected node.
        final String backupNodeId = backupCoordinator != null
            ? backupCoordinator.getPreparedNodeId() : null;
        final String backupState = backupCoordinator != null
            ? backupCoordinator.getState().name() : "IDLE";

        List<NodeInfo> nodes = clusterState.getAllNodes();
        List<Map<String, Object>> nodeList = nodes.stream().map(n -> {
            Map<String, Object> m = new HashMap<>();
            m.put("id", n.id());
            m.put("role", n.role().name());
            m.put("health", n.health().name());
            m.put("serviceState", n.serviceState().name());
            m.put("boltUri", n.boltUri());
            m.put("syncLagMs", n.syncLagMs());
            m.put("inBackup", n.id().equals(backupNodeId));
            return m;
        }).toList();
        ctx.json(Map.of(
            "primaryNode", clusterState.getPrimaryNodeId(),
            "backupState", backupState,
            "backupNodeId", backupNodeId == null ? "" : backupNodeId,
            "nodes", nodeList
        ));
    }

    private void handleNodeDetail(Context ctx) {
        String nodeId = ctx.pathParam("id");
        NodeInfo info = clusterState.getNodeInfo(nodeId);
        if (info == null) {
            ctx.status(404).json(Map.of("error", "Node not found: " + nodeId));
            return;
        }
        ctx.json(info);
    }

    private void auditOp(Context ctx, String op, String params) {
        if (uiAuditLog == null) return;
        Principal p = AuthFilter.principal(ctx);
        String actor = p == null ? "unknown" : p.displayActor();
        String ip = ctx.ip();
        String requestId = UUID.randomUUID().toString();
        ctx.header("X-Request-Id", requestId);
        uiAuditLog.logOperation(op, actor, ip, params, requestId);
    }

    public void stop() {
        if (app != null) {
            app.stop();
            log.info("Admin HTTP server stopped");
        }
    }
}
