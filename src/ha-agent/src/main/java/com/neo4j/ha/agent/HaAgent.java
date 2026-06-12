package com.neo4j.ha.agent;

import com.neo4j.ha.agent.audit.FailoverAuditLog;
import com.neo4j.ha.agent.backup.BackupCoordinator;
import com.neo4j.ha.agent.bootstrap.ApocTriggerInstaller;
import com.neo4j.ha.agent.bootstrap.ClusterInitializer;
import com.neo4j.ha.agent.bootstrap.IndexInstaller;
import com.neo4j.ha.agent.failover.FencingTokenManager;
import com.neo4j.ha.agent.failover.FailoverOrchestrator;
import com.neo4j.ha.agent.failover.StandbySelector;
import com.neo4j.ha.agent.health.HealthChecker;
import com.neo4j.ha.agent.http.AdminHttpServer;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.lifecycle.GracefulShutdown;
import com.neo4j.ha.agent.recovery.OldPrimaryRecovery;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.agent.routing.*;
import com.neo4j.ha.cdc.CdcCollector;
import com.neo4j.ha.cdc.CdcCollectorConfig;
import com.neo4j.ha.cdc.publish.PublishBuffer;
import com.neo4j.ha.cdc.publish.StreamPublishService;
import com.neo4j.ha.common.config.ConfigLoader;
import com.neo4j.ha.common.config.ConfigValidator;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.metrics.MetricsRegistry;
import com.neo4j.ha.common.model.NodeHealth;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeServiceState;
import com.neo4j.ha.common.neo4j.Neo4jClientFactory;
import com.neo4j.ha.common.neo4j.Neo4jHealthChecker;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.common.redis.RedisClientFactory;
import com.neo4j.ha.common.redis.StreamConsumer;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.serialization.EventSerializer;
import com.neo4j.ha.sync.SyncApplier;
import com.neo4j.ha.sync.SyncApplierConfig;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HaAgent {

    private static final Logger log = LoggerFactory.getLogger(HaAgent.class);

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "config/agent/ha-agent.yml";
        log.info("Neo4j HA Agent starting with config: {}", configPath);

        // 1. Load and validate config
        HaConfig config = ConfigLoader.load(configPath);
        var errors = ConfigValidator.validate(config);
        if (!errors.isEmpty()) {
            log.error("Configuration errors: {}", errors);
            System.exit(1);
        }

        // 2. Initialize connections
        long connTimeoutMs = HaConfig.parseDuration(config.cluster().neo4j().connectionAcquisitionTimeout());
        Neo4jClientFactory neo4jFactory = new Neo4jClientFactory(
            config.cluster().neo4j().maxConnectionPoolSize(), connTimeoutMs);
        RedisClientFactory redisFactory = new RedisClientFactory(config.redis());
        JedisPool jedisPool = redisFactory.createPool();

        // 3. Metrics
        var registry = MetricsRegistry.init();
        HaMetrics metrics = new HaMetrics(registry);

        // 4. Cluster initialization
        ClusterStateManager clusterState = new ClusterStateManager();
        NodeRegistry nodeRegistry = new NodeRegistry(jedisPool,
            config.registry().key(), config.registry().updateIntervalMs(), clusterState);
        ClusterInitializer initializer = new ClusterInitializer(
            config, neo4jFactory, clusterState, nodeRegistry);
        initializer.init();

        // 5. Primary node bootstrap
        Driver primaryDriver = clusterState.getPrimaryDriver();
        String primaryNodeId = clusterState.getPrimaryNodeId();
        String database = config.cluster().nodes().get(0).neo4j().database();

        ApocTriggerInstaller triggerInstaller = new ApocTriggerInstaller();
        try {
            triggerInstaller.ensureInstalled(primaryDriver, database);
        } catch (Exception e) {
            log.error("APOC trigger installation failed after retries. " +
                "Agent will continue but CDC delete capture may not work until triggers are installed. " +
                "Error: {}", e.getMessage());
        }

        IndexInstaller indexInstaller = new IndexInstaller(metrics);
        indexInstaller.ensureIndexes(primaryDriver, database);

        // 6. Start data sync
        CheckpointManager checkpointManager = new CheckpointManager(jedisPool);
        CdcCollectorConfig cdcConfig = CdcCollectorConfig.from(config);
        EventSerializer serializer = new EventSerializer();
        StreamPublisher streamPublisher = new StreamPublisher(jedisPool, serializer,
            config.stream().maxLen(), config.failover().fencingToken().key());
        PublishBuffer publishBuffer = new PublishBuffer(serializer,
            config.buffer().dir(), config.buffer().maxFiles());
        FencingTokenManager fencingTokenManager = new FencingTokenManager(jedisPool,
            config.failover().fencingToken().key());
        long currentToken = fencingTokenManager.getCurrentToken();

        StreamPublishService publishService = new StreamPublishService(
            streamPublisher, publishBuffer, config.stream().changes(), currentToken, metrics);

        CdcCollector cdcCollector = new CdcCollector(cdcConfig, checkpointManager,
            publishService, streamPublisher, publishBuffer, metrics);
        cdcCollector.start(primaryDriver, primaryNodeId, database, currentToken);

        SyncApplierConfig syncConfig = SyncApplierConfig.from(config);
        JedisPool blockingPool = redisFactory.createBlockingPool(syncConfig.blockTimeoutMs());
        StreamConsumer streamConsumer = new StreamConsumer(jedisPool, blockingPool);
        SyncApplier syncApplier = new SyncApplier(syncConfig, streamConsumer, checkpointManager, metrics);
        Map<String, Driver> standbyDrivers = clusterState.getStandbyDrivers();
        syncApplier.start(standbyDrivers, database, currentToken);

        // 7. Health checker
        HealthChecker healthChecker = new HealthChecker(clusterState, new Neo4jHealthChecker(),
            metrics, config.failover());

        // 8. HAProxy management
        HaProxySocketClient socketClient = new HaProxySocketClient();
        List<HaProxyInstance> haproxyInstances = config.cluster().haproxy().instances().stream()
            .map(i -> new HaProxyInstance(i.id(), i.adminSocket()))
            .toList();
        HaProxyUpdater haProxyUpdater = new HaProxyUpdater(haproxyInstances, socketClient,
            config.cluster().haproxy().primaryBackend(),
            config.cluster().haproxy().readBackend(), metrics);
        long stateSyncMs = HaConfig.parseDuration(config.cluster().haproxy().stateSyncInterval());
        HaProxyStateSyncer stateSyncer = new HaProxyStateSyncer(haproxyInstances, clusterState,
            socketClient, config.cluster().haproxy().primaryBackend(), metrics, stateSyncMs);
        stateSyncer.start();

        // 9. Backup coordinator (created before failover orchestrator so it can be wired)
        // v2: BackupCoordinator now coordinates SyncApplier + HealthChecker +
        // HaProxyStateSyncer + HaProxyUpdater so docker-stopping a standby for
        // file-system backup doesn't trigger onNodeDown cascade or HAProxy
        // reconciler "fixing" the deliberately-drained read backend.
        long maxBackupMs = HaConfig.parseDuration(config.backup().maxDuration());
        BackupCoordinator backupCoordinator = new BackupCoordinator(
            syncApplier, checkpointManager, metrics, maxBackupMs,
            healthChecker, stateSyncer, haProxyUpdater, clusterState);

        // 10. Failover orchestrator
        FailoverAuditLog auditLog = new FailoverAuditLog(jedisPool);
        StandbySelector standbySelector = new StandbySelector(clusterState, healthChecker, checkpointManager);
        FailoverOrchestrator failoverOrchestrator = new FailoverOrchestrator(
            healthChecker, fencingTokenManager, cdcCollector, syncApplier,
            standbySelector, triggerInstaller, indexInstaller, haProxyUpdater,
            stateSyncer,
            nodeRegistry, clusterState, auditLog, metrics, checkpointManager,
            database,
            config.failover().confirmationWaitMs(),
            config.failover().minIntervalMs(),
            config.failover().maxAutoPerHour()
        );
        failoverOrchestrator.setBackupCoordinator(backupCoordinator);

        // Recovery handler
        OldPrimaryRecovery recovery = new OldPrimaryRecovery(
            clusterState, syncApplier, cdcCollector, checkpointManager, nodeRegistry,
            indexInstaller, auditLog, metrics, config.stream().changes()
        );

        // M2 fix: serialize failover and recovery tasks on a single-thread executor to avoid
        // concurrent executeFailover invocations (which would double-increment the fencing
        // token and corrupt cluster state) and to ensure recovery runs after failover settles.
        AtomicBoolean failoverInFlight = new AtomicBoolean(false);
        ExecutorService failoverExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ha-failover");
            t.setDaemon(true);
            return t;
        });

        healthChecker.setListener(new HealthChecker.HealthChangeListener() {
            @Override
            public void onNodeDown(String nodeId) {
                if (!nodeId.equals(clusterState.getPrimaryNodeId())) return;
                if (!failoverInFlight.compareAndSet(false, true)) {
                    log.warn("Failover already in flight, skipping duplicate trigger for {}", nodeId);
                    return;
                }
                log.error("Primary node {} is DOWN, initiating failover", nodeId);
                failoverExecutor.submit(() -> {
                    try {
                        failoverOrchestrator.executeFailover(nodeId);
                    } finally {
                        failoverInFlight.set(false);
                    }
                });
            }

            @Override
            public void onNodeRecovered(String nodeId) {
                if (nodeRegistry.isPendingCleanup(nodeId)) {
                    log.info("Old primary {} recovered, initiating recovery", nodeId);
                    failoverExecutor.submit(() -> recovery.execute(nodeId));
                    return;
                }
                // BUG-074: a standby that transitions DOWN → HEALTHY (e.g., container
                // restart during a chaos run) leaves its consumer-group PEL stuffed with
                // events that arrived while the Bolt endpoint was unreachable. The
                // sync-applier's consumeLoop reads with XREADGROUP ">", so those PEL
                // entries are never redelivered and the standby permanently diverges
                // from the primary (observed as missing CREATEs and "delete leaks" in
                // the chaos-test integrity report). Asking SyncApplier to drain the
                // PEL on its next iteration replays the backlog via XREADGROUP "0-0".
                // For the old-primary recovery path above, OldPrimaryRecovery already
                // triggers a fullsync (or incremental catch-up with its own rate limit),
                // so we deliberately do NOT double-schedule here.
                if (!nodeId.equals(clusterState.getPrimaryNodeId())) {
                    syncApplier.schedulePendingRecovery(nodeId);
                }
            }
        });
        healthChecker.start();

        // 11. Node registry periodic update
        nodeRegistry.startPeriodicUpdate();

        // 12. Scheduled maintenance tasks
        ScheduledExecutorService maintenanceScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ha-maintenance");
            t.setDaemon(true);
            return t;
        });

        // M1: Wire BackupCoordinator.checkTimeout to scheduler
        maintenanceScheduler.scheduleAtFixedRate(() -> {
            try {
                backupCoordinator.checkTimeout();
            } catch (Exception e) {
                log.warn("Backup timeout check failed", e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        // M5: ServiceState auto evaluation + HAProxy read backend linkage
        long syncLagThresholdMs = config.serviceState() != null && config.serviceState().syncLagThreshold() > 0
            ? config.serviceState().syncLagThreshold() : 5000L;
        long stableDurationMs = config.serviceState() != null && config.serviceState().stableDurationMs() > 0
            ? config.serviceState().stableDurationMs() : 30000L;
        long checkIntervalMs = config.serviceState() != null && config.serviceState().checkIntervalMs() > 0
            ? config.serviceState().checkIntervalMs() : 5000L;
        final Map<String, Long> stableSinceByNode = new ConcurrentHashMap<>();
        maintenanceScheduler.scheduleAtFixedRate(() -> {
            try {
                evaluateServiceStates(clusterState, haProxyUpdater, checkpointManager,
                    syncLagThresholdMs, stableDurationMs, stableSinceByNode, metrics,
                    backupCoordinator);
            } catch (Exception e) {
                log.warn("Service state evaluation failed", e);
            }
        }, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);

        // 12c. Stream retention maintenance (BUG-038 + BUG-040). Runs XTRIM MINID
        // periodically on BOTH the incremental changes stream AND the fullsync batch
        // stream, with a cutoff that never exceeds any consumer group's oldest unacked
        // or last-delivered position. XADD's MAXLEN remains as the OOM safety net;
        // this task makes trimming consumer-aware so no one's PEL can outlive the stream.
        HaConfig.StreamConfig streamCfg = config.stream();
        long streamMaintenanceMs = streamCfg != null ? streamCfg.maintenanceIntervalMs() : 0L;
        long retentionSafetyMs = streamCfg != null ? streamCfg.retentionSafetyWindowMs() : 300_000L;
        java.util.List<String> maintainedStreams = new java.util.ArrayList<>();
        if (streamCfg != null) {
            if (streamCfg.changes() != null && !streamCfg.changes().isBlank()) {
                maintainedStreams.add(streamCfg.changes());
            }
            if (streamCfg.fullsync() != null && !streamCfg.fullsync().isBlank()) {
                maintainedStreams.add(streamCfg.fullsync());
            }
        } else {
            maintainedStreams.add("neo4j:cdc:neo4j:changes");
            maintainedStreams.add("neo4j:cdc:neo4j:fullsync");
        }
        com.neo4j.ha.agent.maintenance.StreamMaintenanceTask streamMaintenance =
            new com.neo4j.ha.agent.maintenance.StreamMaintenanceTask(
                jedisPool,
                maintainedStreams,
                streamMaintenanceMs,
                retentionSafetyMs,
                clusterState,
                metrics);
        streamMaintenance.start();

        // 13. Admin HTTP server (with optional UI)
        var uiConfig = config.admin().ui();
        boolean uiEnabled = uiConfig != null && uiConfig.isEnabled();
        com.neo4j.ha.agent.http.auth.UserStore userStore =
            new com.neo4j.ha.agent.http.auth.UserStore(uiConfig);
        long sessionTtlMs = uiConfig != null && uiConfig.session() != null
            ? uiConfig.session().ttlMs() : 8 * 3_600_000L;
        int maxPerUser = uiConfig != null && uiConfig.session() != null
            ? uiConfig.session().maxPerUserOrDefault() : 3;
        com.neo4j.ha.agent.http.auth.SessionManager sessionManager =
            new com.neo4j.ha.agent.http.auth.SessionManager(sessionTtlMs, maxPerUser);
        int maxFailuresPerMin = uiConfig != null && uiConfig.rateLimit() != null
            ? uiConfig.rateLimit().maxFailuresPerMinuteOrDefault() : 5;
        long lockDurationMs = uiConfig != null && uiConfig.rateLimit() != null
            ? uiConfig.rateLimit().lockDurationMs() : 10 * 60_000L;
        com.neo4j.ha.agent.http.auth.RateLimiter rateLimiter =
            new com.neo4j.ha.agent.http.auth.RateLimiter(maxFailuresPerMin, lockDurationMs);
        com.neo4j.ha.agent.http.auth.UiAuditLog uiAuditLog =
            new com.neo4j.ha.agent.http.auth.UiAuditLog(jedisPool);
        com.neo4j.ha.agent.http.auth.AuthFilter authFilter =
            new com.neo4j.ha.agent.http.auth.AuthFilter(sessionManager,
                config.admin().auth() != null ? config.admin().auth().token() : null);
        com.neo4j.ha.agent.http.AuthController authCtrl =
            new com.neo4j.ha.agent.http.AuthController(
                userStore, sessionManager, rateLimiter, uiAuditLog, metrics, sessionTtlMs);
        com.neo4j.ha.agent.http.AuditController auditCtrl =
            new com.neo4j.ha.agent.http.AuditController(jedisPool, auditLog);
        com.neo4j.ha.agent.http.MetricsSummaryController metricsCtrl =
            new com.neo4j.ha.agent.http.MetricsSummaryController(metrics, jedisPool,
                config.failover().fencingToken().key(), config.stream().changes());
        com.neo4j.ha.agent.http.DataStatsController dataStatsCtrl =
            new com.neo4j.ha.agent.http.DataStatsController(clusterState, database);
        com.neo4j.ha.agent.http.DataDiffController dataDiffCtrl =
            new com.neo4j.ha.agent.http.DataDiffController(clusterState, database, uiAuditLog);

        AdminHttpServer httpServer = new AdminHttpServer(
            config.admin().port(),
            clusterState, failoverOrchestrator, cdcCollector, backupCoordinator,
            authFilter, authCtrl, auditCtrl, metricsCtrl,
            dataStatsCtrl, dataDiffCtrl,
            uiAuditLog, metrics, uiEnabled);
        httpServer.start();

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
            new GracefulShutdown(cdcCollector, syncApplier, recovery, neo4jFactory, redisFactory)));

        log.info("Neo4j HA Agent started successfully. Primary: {}", primaryNodeId);

        // Keep main thread alive
        Thread.currentThread().join();
    }

    /**
     * Evaluates each standby node's service state independently using per-node checkpoint lag.
     * SYNCING → ONLINE when node's sync lag stays below threshold for stableDuration.
     * ONLINE → SYNCING when node's sync lag exceeds threshold.
     *
     * BUG-033 guard: we require the primary's CDC to have **recently written** its checkpoint
     * before trusting `primaryLastTs` as the reference. On Agent cold-start the CDC loop
     * resumes from an old on-disk checkpoint and may take minutes to catch up on the primary's
     * historical data; during that period `primaryLastTs` is far behind wall clock and the
     * standby's own `lastEventTs` equals it, giving a misleading "lag=0" that prematurely
     * promotes the standby to ONLINE. When real writes arrive afterward, `primaryLastTs`
     * jumps forward while slow standbys lag, flipping them ONLINE → SYNCING and churning HAProxy.
     * By requiring CDC activity within the last (syncLagThreshold * 5 + stableDuration)
     * window we defer promotion until the CDC pipeline is demonstrably live.
     */
    private static void evaluateServiceStates(ClusterStateManager clusterState,
                                               HaProxyUpdater haProxyUpdater,
                                               CheckpointManager checkpointManager,
                                               long syncLagThresholdMs,
                                               long stableDurationMs,
                                               Map<String, Long> stableSinceByNode,
                                               HaMetrics metrics,
                                               com.neo4j.ha.agent.backup.BackupCoordinator backupCoordinator) {
        long now = System.currentTimeMillis();
        String primaryNodeId = clusterState.getPrimaryNodeId();
        var primaryCp = primaryNodeId != null
            ? checkpointManager.loadCdcCheckpoint(primaryNodeId)
            : java.util.Optional.<CheckpointManager.CdcCheckpoint>empty();
        long primaryLastTs = primaryCp.map(CheckpointManager.CdcCheckpoint::lastTs).orElse(0L);
        long primaryCpUpdatedAt = primaryCp.map(CheckpointManager.CdcCheckpoint::updatedAt).orElse(0L);

        // "CDC is actively progressing" means the primary's checkpoint was written recently
        // enough that its lastTs reflects the true head of the change stream, not a stale
        // cursor left over from a previous Agent lifetime.
        long cdcFreshnessWindowMs = syncLagThresholdMs * 5L + stableDurationMs;
        boolean cdcActive = primaryCpUpdatedAt > 0 && (now - primaryCpUpdatedAt) < cdcFreshnessWindowMs;

        // C3 fix: compute MAX standby lag here (primary CDC ts - standby sync ts) and
        // publish as the authoritative neo4j_ha_sync_lag_ms metric. Previously SyncApplier
        // wrote `now - lastEventTs` which drifts with wall clock in low-write periods
        // (BUG-016 regression).
        long maxLagMs = 0L;
        boolean anyStandby = false;

        String backupNode = backupCoordinator != null
            ? backupCoordinator.getPreparedNodeId() : null;
        for (var node : clusterState.getStandbyNodes()) {
            // M-3 fix: skip nodes currently in backup maintenance. Their
            // HAProxy state and ServiceState are owned by BackupCoordinator;
            // this 1s reconciler must not race against the deliberate drain.
            // m-A fix: drop any stale stableSinceByNode timestamp from before
            // the backup window so post-backup re-entry into SYNCING starts
            // a fresh stableDuration countdown instead of inheriting a long-
            // stale baseline that would mark ONLINE too soon.
            if (backupNode != null && backupNode.equals(node.id())) {
                stableSinceByNode.remove(node.id());
                continue;
            }
            NodeServiceState current = node.serviceState();
            String nodeId = node.id();
            String serverId = node.boltUri().replace("bolt://", "").split(":")[0];

            long nodeLagMs = checkpointManager.loadSyncCheckpoint(nodeId)
                .map(cp -> Math.max(0L, primaryLastTs - cp.lastEventTs()))
                .orElse(Long.MAX_VALUE);

            if (nodeLagMs != Long.MAX_VALUE) {
                anyStandby = true;
                maxLagMs = Math.max(maxLagMs, nodeLagMs);
                // BUG-076: publish the per-node lag into NodeInfo so
                // GET /cluster/status reflects reality instead of a stale 0.
                // Skip when the checkpoint is missing (nodeLagMs == MAX_VALUE
                // = unknown); keep the last known value rather than clobbering
                // with a bogus sentinel.
                clusterState.updateSyncLag(nodeId, nodeLagMs);
            }

            if (current == NodeServiceState.SYNCING) {
                // BUG-033: do not promote to ONLINE while CDC pipeline is still bootstrapping,
                // regardless of how small the (primaryLastTs - standbyLastTs) difference looks.
                if (!cdcActive) {
                    stableSinceByNode.remove(nodeId);
                    continue;
                }
                if (nodeLagMs < syncLagThresholdMs) {
                    long stableSince = stableSinceByNode.computeIfAbsent(nodeId, k -> now);
                    if (now - stableSince >= stableDurationMs) {
                        clusterState.setServiceState(nodeId, NodeServiceState.ONLINE);
                        haProxyUpdater.enableReadBackend(serverId);
                        log.info("Node {} transitioned SYNCING → ONLINE (lag {}ms < {}ms for {}ms)",
                            nodeId, nodeLagMs, syncLagThresholdMs, stableDurationMs);
                        stableSinceByNode.remove(nodeId);
                    }
                } else {
                    stableSinceByNode.remove(nodeId);
                }
            } else if (current == NodeServiceState.ONLINE) {
                // Don't trigger an ONLINE→SYNCING churn based on a stale primary checkpoint
                // (e.g. primary CDC briefly paused or restarted). Only act on fresh lag.
                if (!cdcActive) continue;
                if (nodeLagMs > syncLagThresholdMs * 3) {
                    clusterState.setServiceState(nodeId, NodeServiceState.SYNCING);
                    haProxyUpdater.disableReadBackend(serverId);
                    log.warn("Node {} transitioned ONLINE → SYNCING (lag {}ms > {}ms)",
                        nodeId, nodeLagMs, syncLagThresholdMs * 3);
                    stableSinceByNode.remove(nodeId);
                }
            }
        }

        if (anyStandby) {
            metrics.syncLagMs.set(maxLagMs);
        }

        // BUG-073: transition primary to ONLINE when healthy. ClusterInitializer
        // defaults every node (including the primary) to serviceState=SYNCING on
        // reachable startup; the standby loop above is the only code path that
        // promotes to ONLINE, so the primary was left stuck in SYNCING forever.
        // This made /cluster/status confusing (primary permanently "syncing"
        // while actually serving writes) and broke any caller that treated
        // "all nodes ONLINE" as the readiness criterion (e.g. the chaos test
        // runner precheck). Unlike a standby, a primary has no catch-up
        // lifecycle — being primary IS being up-to-date — so the rule is
        // simply: primary is ONLINE iff health=HEALTHY. We don't touch
        // serviceState when health is degraded; HealthChecker + Failover
        // own those transitions.
        if (primaryNodeId != null) {
            NodeInfo primaryInfo = clusterState.getNodeInfo(primaryNodeId);
            if (primaryInfo != null
                    && primaryInfo.health() == NodeHealth.HEALTHY
                    && primaryInfo.serviceState() != NodeServiceState.ONLINE) {
                NodeServiceState from = primaryInfo.serviceState();
                clusterState.setServiceState(primaryNodeId, NodeServiceState.ONLINE);
                log.info("Primary {} transitioned {} → ONLINE (health=HEALTHY)",
                    primaryNodeId, from);
            }
            // BUG-076: primary has no replication lag by definition (it IS
            // the source of the CDC stream). Force-pin to 0 so /cluster/status
            // doesn't show a stale value carried over from a previous role
            // (e.g. this node was a lagging standby before being promoted).
            if (primaryInfo != null && primaryInfo.syncLagMs() != 0L) {
                clusterState.updateSyncLag(primaryNodeId, 0L);
            }
        }
    }
}
