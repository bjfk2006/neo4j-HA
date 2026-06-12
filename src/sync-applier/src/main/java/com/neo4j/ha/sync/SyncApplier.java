package com.neo4j.ha.sync;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.common.redis.StreamConsumer;
import com.neo4j.ha.common.serialization.EventDeserializer;
import com.neo4j.ha.sync.applier.ChangeApplier;
import com.neo4j.ha.sync.applier.IndexManager;
import com.neo4j.ha.sync.consumer.FullSyncConsumer;
import com.neo4j.ha.sync.consumer.IncrementalConsumer;
import com.neo4j.ha.sync.consumer.PendingRecovery;
import com.neo4j.ha.sync.fullsync.BulkImporter;
import com.neo4j.ha.sync.fullsync.DatabaseCleaner;
import com.neo4j.ha.sync.fullsync.FullSyncReceiver;
import com.neo4j.ha.sync.validation.DuplicateDetector;
import com.neo4j.ha.sync.validation.FencingTokenFilter;
import com.neo4j.ha.sync.validation.OrderValidator;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SyncApplier {

    private static final Logger log = LoggerFactory.getLogger(SyncApplier.class);

    private final SyncApplierConfig config;
    private final StreamConsumer streamConsumer;
    private final CheckpointManager checkpointManager;
    private final HaMetrics metrics;
    private final EventDeserializer deserializer;

    private final Map<String, Driver> standbyDrivers = new ConcurrentHashMap<>();
    private final IndexManager indexManager = new IndexManager();
    private final Map<String, IncrementalConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<String, FullSyncReceiver> fullSyncReceivers = new ConcurrentHashMap<>();
    // BUG-074: standbys whose PEL must be drained on the next consumeLoop iteration.
    // Populated by (a) HaAgent.onNodeRecovered when a standby transitions DOWN→HEALTHY,
    // and (b) consumeLoop itself when consumeOnce fails (transient Bolt outage), so that
    // un-ACK'd messages from the failed batch are retried from the consumer group's PEL.
    // Before this, a standby outage left the PEL backlog unreachable until ha-agent
    // restart, because consumeOnce always reads with XREADGROUP ">" (new only).
    private final java.util.Set<String> pendingRecoveryRequests =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    // Per-node "not-before" epoch ms gate so a persistently-failing node does not
    // hot-loop PEL replay (each PEL replay issues a Bolt write that may itself fail,
    // blocking OTHER standbys' consumers from progressing). 0L means "replay immediately".
    private final Map<String, Long> pendingRecoveryNotBefore = new ConcurrentHashMap<>();
    private static final long PENDING_RECOVERY_RETRY_BACKOFF_MS = 10_000L;
    private ExecutorService executor;
    private volatile boolean running;
    private volatile boolean paused;
    private volatile boolean processing;
    private String database;
    private volatile long currentFencingToken;

    public SyncApplier(SyncApplierConfig config, StreamConsumer streamConsumer,
                        CheckpointManager checkpointManager, HaMetrics metrics) {
        this.config = config;
        this.streamConsumer = streamConsumer;
        this.checkpointManager = checkpointManager;
        this.metrics = metrics;
        this.deserializer = new EventDeserializer();
    }

    public void start(Map<String, Driver> drivers, String database) {
        start(drivers, database, 0);
    }

    public void start(Map<String, Driver> drivers, String database, long currentFencingToken) {
        this.standbyDrivers.clear();
        this.consumers.clear();
        this.fullSyncReceivers.clear();
        // BUG-074: scheduled PEL replay state is tied to the pre-restart standbyDrivers
        // map; a fresh start() rebuilds per-node components and immediately runs the
        // startup PendingRecovery below, which supersedes anything previously scheduled.
        this.pendingRecoveryRequests.clear();
        this.pendingRecoveryNotBefore.clear();
        this.standbyDrivers.putAll(drivers);
        this.database = database;
        this.currentFencingToken = currentFencingToken;

        // Ensure one consumer group per standby node so each standby receives full event stream.
        for (String nodeId : standbyDrivers.keySet()) {
            streamConsumer.ensureGroup(config.changesStreamKey(), groupForNode(nodeId));
        }

        // Create per-node components to avoid cross-node state pollution
        DatabaseCleaner dbCleaner = new DatabaseCleaner();
        for (var entry : standbyDrivers.entrySet()) {
            String nodeId = entry.getKey();

            ChangeApplier changeApplier = new ChangeApplier(indexManager, nodeId, database, metrics);
            FencingTokenFilter fencingFilter = new FencingTokenFilter();
            fencingFilter.updateToken(currentFencingToken);
            DuplicateDetector dedupDetector = new DuplicateDetector(config.duplicateDetectorMaxSize());
            OrderValidator orderValidator = new OrderValidator();

            IncrementalConsumer consumer = new IncrementalConsumer(
                streamConsumer, deserializer, changeApplier, fencingFilter,
                dedupDetector, orderValidator, checkpointManager, metrics, config
            );
            consumers.put(nodeId, consumer);

            BulkImporter bulkImporter = new BulkImporter(indexManager, nodeId);
            FullSyncConsumer fullSyncConsumer = new FullSyncConsumer(
                streamConsumer, deserializer, bulkImporter, config.fullsyncStreamKey(),
                config.consumerGroup() + "-" + nodeId
            );
            FullSyncReceiver receiver = new FullSyncReceiver(dbCleaner, fullSyncConsumer, database);
            fullSyncReceivers.put(nodeId, receiver);
        }

        // Ensure indexes on all reachable standby nodes
        for (var entry : standbyDrivers.entrySet()) {
            try (Session session = entry.getValue().session(SessionConfig.forDatabase(database))) {
                indexManager.ensureIndexesForAllLabels(session, entry.getKey());
            } catch (Exception e) {
                log.warn("Skipping index ensure for unreachable standby {}: {}", entry.getKey(), e.getMessage());
            }
        }

        // Recover pending messages per reachable node
        for (var entry : standbyDrivers.entrySet()) {
            String nodeId = entry.getKey();
            try {
                ChangeApplier recoveryApplier = new ChangeApplier(indexManager, nodeId, database, metrics);
                PendingRecovery recovery = new PendingRecovery(
                    streamConsumer, deserializer, recoveryApplier,
                    config.changesStreamKey(), groupForNode(nodeId), nodeId, metrics
                );
                recovery.recover(entry.getValue());
            } catch (Exception e) {
                log.warn("Skipping pending recovery for unreachable standby {}: {}", nodeId, e.getMessage());
            }
        }

        // Start consume loop
        this.running = true;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "sync-applier");
            t.setDaemon(true);
            return t;
        });
        executor.submit(this::consumeLoop);

        log.info("Sync Applier started with {} standby targets", standbyDrivers.size());
    }

    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Sync Applier stopped");
    }

    public void pause() {
        paused = true;
        log.info("Sync Applier paused");
    }

    public void resume() {
        paused = false;
        log.info("Sync Applier resumed");
    }

    public void drainPending() {
        log.info("Draining pending messages...");
        long deadline = System.currentTimeMillis() + 30_000;
        while (processing && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (processing) {
            log.warn("Drain timed out after 30s, current batch may still be in-flight");
        } else {
            log.info("Drain complete");
        }
    }

    public void addTarget(Driver driver, String nodeId) {
        standbyDrivers.put(nodeId, driver);
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            indexManager.ensureIndexesForAllLabels(session, nodeId);
        }

        // Create per-node components for the new target
        ChangeApplier changeApplier = new ChangeApplier(indexManager, nodeId, database, metrics);
        FencingTokenFilter fencingFilter = new FencingTokenFilter();
        fencingFilter.updateToken(currentFencingToken);
        DuplicateDetector dedupDetector = new DuplicateDetector(config.duplicateDetectorMaxSize());
        OrderValidator orderValidator = new OrderValidator();
        IncrementalConsumer consumer = new IncrementalConsumer(
            streamConsumer, deserializer, changeApplier, fencingFilter,
            dedupDetector, orderValidator, checkpointManager, metrics, config
        );
        consumers.put(nodeId, consumer);

        streamConsumer.ensureGroup(config.changesStreamKey(), groupForNode(nodeId));

        // Recover any pending messages for this node (symmetry with start(); fixes M16
        // where a re-added standby's PEL events were left unprocessed).
        try {
            ChangeApplier recoveryApplier = new ChangeApplier(indexManager, nodeId, database, metrics);
            PendingRecovery recovery = new PendingRecovery(
                streamConsumer, deserializer, recoveryApplier,
                config.changesStreamKey(), groupForNode(nodeId), nodeId, metrics
            );
            recovery.recover(driver);
        } catch (Exception e) {
            log.warn("Pending recovery skipped for re-added standby {}: {}", nodeId, e.getMessage());
        }

        DatabaseCleaner dbCleaner = new DatabaseCleaner();
        BulkImporter bulkImporter = new BulkImporter(indexManager, nodeId);
        FullSyncConsumer fullSyncConsumer = new FullSyncConsumer(
            streamConsumer, deserializer, bulkImporter, config.fullsyncStreamKey(),
            config.consumerGroup() + "-" + nodeId
        );
        FullSyncReceiver receiver = new FullSyncReceiver(dbCleaner, fullSyncConsumer, database);
        fullSyncReceivers.put(nodeId, receiver);

        // BUG-074: addTarget() already ran PendingRecovery above, so discard any stale
        // replay schedule from a previous incarnation of this nodeId to avoid an
        // immediate second (redundant) replay on the next consumeLoop iteration.
        pendingRecoveryRequests.remove(nodeId);
        pendingRecoveryNotBefore.remove(nodeId);

        log.info("Added sync target: {}", nodeId);
    }

    // NOTE: a `triggerFullSync(nodeId)` method used to live here as a no-op stub that
    // merely logged "Full sync triggered". It was retained accidentally after BUG-024
    // was fixed and is now removed (BUG-041) to prevent future callers from mistakenly
    // relying on it. The canonical way to start a full sync is:
    //     cdcCollector.createFullSyncCoordinator().startFullSync(nodeId)
    // which is invoked from AdminHttpServer /cluster/fullsync and from
    // OldPrimaryRecovery (with rate-limit per BUG-039).

    private void consumeLoop() {
        while (running) {
            if (paused) {
                try { Thread.sleep(100); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                continue;
            }

            try {
                processing = true;
                for (var entry : standbyDrivers.entrySet()) {
                    if (!running || paused) break;
                    String nodeId = entry.getKey();
                    IncrementalConsumer consumer = consumers.get(nodeId);
                    FullSyncReceiver receiver = fullSyncReceivers.get(nodeId);
                    if (consumer == null || receiver == null) continue;

                    // BUG-074: drain any PEL entries that piled up while this standby was
                    // unreachable (or while a previous consumeOnce failed mid-batch) BEFORE
                    // issuing the next XREADGROUP ">" — otherwise those un-ACK'd events are
                    // never redelivered and the standby permanently diverges from the
                    // primary. See HaAgent.onNodeRecovered and the catch block below for
                    // the two schedulers that can populate pendingRecoveryRequests.
                    maybeRunScheduledPelReplay(nodeId, entry.getValue());

                    try {
                        consumer.consumeOnce(entry.getValue(), nodeId, groupForNode(nodeId),
                            new IncrementalConsumer.FullSyncCallback() {
                                @Override
                                public void onFullSyncStart(ChangeEvent event) {
                                    // BUG-071: FULL_SYNC_START is published to the shared
                                    // changesStreamKey, so every standby's consumer group
                                    // receives it. FullSyncCoordinator.publishControlEvent
                                    // embeds the intended target in `entity.elementId`
                                    // (src/cdc-collector/.../FullSyncCoordinator.java:70-83).
                                    // Before this guard, every standby would clean its own
                                    // database and re-import on every recovery/fullsync call,
                                    // turning a single-target recovery into an N-way wipe.
                                    // Skipping non-target events keeps the receiver state
                                    // machine untouched (stays in IDLE) on the other standbys.
                                    if (!isForThisTarget(event, nodeId)) {
                                        return;
                                    }
                                    receiver.onFullSyncStart(event, entry.getValue(), nodeId);
                                }
                                @Override
                                public void onFullSyncEnd(ChangeEvent event) {
                                    // Symmetric BUG-071 guard. Without it, standbys that
                                    // (mis)received a FULL_SYNC_START with the previous fix
                                    // off would still see an END later and log warnings like
                                    // "Full sync end received in unexpected state: IDLE".
                                    if (!isForThisTarget(event, nodeId)) {
                                        return;
                                    }
                                    receiver.onFullSyncEnd(event);
                                }
                            });
                    } catch (Exception nodeErr) {
                        // BUG-074: a failed consumeOnce means XACK did not run for the batch
                        // Redis already delivered to this consumer group — those entries now
                        // sit in the PEL and will NOT be re-read by the next XREADGROUP ">".
                        // Flag this node for a PEL replay on the next iteration so the un-
                        // ACK'd events are retried via XREADGROUP "0-0". The backoff prevents
                        // a Bolt-down standby from hot-looping and starving the other targets.
                        pendingRecoveryRequests.add(nodeId);
                        pendingRecoveryNotBefore.put(nodeId,
                            System.currentTimeMillis() + PENDING_RECOVERY_RETRY_BACKOFF_MS);
                        log.warn("consumeOnce for standby {} failed; scheduled PEL replay after {}ms. cause={}",
                            nodeId, PENDING_RECOVERY_RETRY_BACKOFF_MS, nodeErr.toString());
                    }

                    // Check if CATCHING_UP → IDLE transition should happen (per-node)
                    if (receiver.isCatchingUp()) {
                        var cp = checkpointManager.loadSyncCheckpoint(nodeId);
                        long lastAppliedTs = cp.map(CheckpointManager.SyncCheckpoint::lastEventTs).orElse(0L);
                        receiver.checkCatchUp(nodeId, lastAppliedTs);
                    }
                }

                // C3 fix: sync lag metric is now owned by HaAgent.evaluateServiceStates, which
                // computes (primaryCdc.lastTs - standbySync.lastEventTs) per node. The previous
                // `now - lastEventTs` formula was a BUG-016 regression that drifted with wall
                // clock during low-write periods and caused false high-lag alerts.
                processing = false;
            } catch (Exception e) {
                processing = false;
                log.error("Error in sync consume loop", e);
                try { Thread.sleep(1000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isPaused() {
        return paused;
    }

    public FullSyncReceiver getFullSyncReceiver(String nodeId) {
        return fullSyncReceivers.get(nodeId);
    }

    /**
     * @deprecated Use {@link #getFullSyncReceiver(String)} for per-node access.
     * Returns the first receiver for backward compatibility.
     */
    @Deprecated
    public FullSyncReceiver getFullSyncReceiver() {
        return fullSyncReceivers.values().stream().findFirst().orElse(null);
    }

    private String groupForNode(String nodeId) {
        return config.consumerGroup() + "-" + nodeId;
    }

    /**
     * BUG-074: request a PEL replay for {@code nodeId} on the next {@code consumeLoop}
     * iteration. Safe to invoke from any thread; typically called from
     * {@link com.neo4j.ha.agent.health.HealthChecker.HealthChangeListener#onNodeRecovered}
     * when a standby transitions DOWN → HEALTHY.
     *
     * <p>Why this is needed: while a standby is unreachable, the sync-applier keeps
     * issuing {@code XREADGROUP ">"} for it (because the consumer group still exists
     * on Redis). Each batch is delivered to the PEL and then fails to apply because
     * the standby's Bolt is down, so {@code XACK} never runs. Those PEL entries
     * stay delivered-but-unacked forever; the next {@code consumeOnce} only asks for
     * NEW messages ("{@code >}") and therefore skips them. Without this scheduler,
     * the only way to replay the backlog was a full ha-agent restart — which ran
     * {@link com.neo4j.ha.sync.consumer.PendingRecovery} from {@link #start}.</p>
     *
     * <p>Idempotent: multiple schedule calls collapse to one replay. Failure during
     * replay re-schedules with a backoff (see {@link #PENDING_RECOVERY_RETRY_BACKOFF_MS}).</p>
     */
    public void schedulePendingRecovery(String nodeId) {
        if (nodeId == null) return;
        if (!standbyDrivers.containsKey(nodeId)) {
            log.debug("Ignoring pending-recovery schedule for unknown node {}", nodeId);
            return;
        }
        // Clear any previous backoff so a fresh recovery signal (e.g., from a new
        // DOWN→HEALTHY transition) replays immediately rather than waiting out a
        // stale backoff window from the previous failed attempt.
        pendingRecoveryNotBefore.put(nodeId, 0L);
        if (pendingRecoveryRequests.add(nodeId)) {
            log.info("Scheduled PEL replay for recovered standby {}", nodeId);
        }
    }

    /** Package-private accessor used by {@link SyncApplierTest} to assert schedule state. */
    boolean isPendingRecoveryScheduled(String nodeId) {
        return pendingRecoveryRequests.contains(nodeId);
    }

    /**
     * Package-private hook used by {@link SyncApplierTest} to populate the
     * {@code standbyDrivers} map without running the heavy {@link #start} path
     * (which needs a real Redis connection, a real Bolt Driver, etc.). Production
     * code never calls this.
     */
    void injectStandbyDriverForTest(String nodeId, Driver driver) {
        standbyDrivers.put(nodeId, driver);
    }

    /**
     * Drain the per-node PEL if {@code nodeId} is scheduled and the backoff window
     * has elapsed. Called from the single sync-applier thread so it never races
     * with the subsequent {@link IncrementalConsumer#consumeOnce} on the same node.
     */
    private void maybeRunScheduledPelReplay(String nodeId, Driver driver) {
        if (!pendingRecoveryRequests.contains(nodeId)) return;
        long notBefore = pendingRecoveryNotBefore.getOrDefault(nodeId, 0L);
        if (System.currentTimeMillis() < notBefore) return;

        try {
            ChangeApplier recoveryApplier =
                new ChangeApplier(indexManager, nodeId, database, metrics);
            PendingRecovery recovery = new PendingRecovery(
                streamConsumer, deserializer, recoveryApplier,
                config.changesStreamKey(), groupForNode(nodeId), nodeId, metrics
            );
            int recovered = recovery.recover(driver);
            pendingRecoveryRequests.remove(nodeId);
            pendingRecoveryNotBefore.remove(nodeId);
            if (recovered > 0) {
                log.info("PEL replay for recovered standby {} applied {} events", nodeId, recovered);
            } else {
                log.debug("PEL replay for recovered standby {} found nothing pending", nodeId);
            }
        } catch (Exception e) {
            // Keep the flag set and apply backoff. A Bolt-still-unreachable retry every
            // 10s is preferable to log spam every iteration, and also leaves room for
            // other standbys to make progress in the meantime.
            pendingRecoveryNotBefore.put(nodeId,
                System.currentTimeMillis() + PENDING_RECOVERY_RETRY_BACKOFF_MS);
            log.warn("PEL replay for standby {} failed, retry in {}ms: {}",
                nodeId, PENDING_RECOVERY_RETRY_BACKOFF_MS, e.toString());
        }
    }

    /**
     * BUG-071: decide whether this standby should act on a FULL_SYNC control
     * event. {@link com.neo4j.ha.cdc.fullsync.FullSyncCoordinator#publishControlEvent}
     * puts the intended target standby id into {@code event.entity().elementId()}.
     * A null entity or null elementId is treated as "not for me" (fail-safe) —
     * an older primary shouldn't be able to force every standby to wipe itself
     * by publishing a malformed control event.
     *
     * <p>Package-private for unit testing.</p>
     */
    static boolean isForThisTarget(ChangeEvent event, String nodeId) {
        if (event == null || event.entity() == null) return false;
        String target = event.entity().elementId();
        return target != null && target.equals(nodeId);
    }
}
