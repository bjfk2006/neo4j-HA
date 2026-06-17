package com.neo4j.ha.cdc;

import com.neo4j.ha.cdc.heal.NakedRelationshipHealer;
import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.cdc.polling.CypherPollingStrategy;
import com.neo4j.ha.cdc.polling.PollingState;
import com.neo4j.ha.cdc.publish.PublishBuffer;
import com.neo4j.ha.cdc.publish.StreamPublishService;
import com.neo4j.ha.cdc.transform.ChangeEventBuilder;
import com.neo4j.ha.cdc.transform.DiffCalculator;
import com.neo4j.ha.cdc.fullsync.FullSyncCoordinator;
import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.redis.CheckpointManager;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.redis.StreamPublisher.FencingTokenRejectedException;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CdcCollector {

    private static final Logger log = LoggerFactory.getLogger(CdcCollector.class);

    /**
     * BUG-067: safety window for the startup sweep of orphan
     * {@code _CDCDeleteEvent} transit nodes in {@link #start}. Events whose
     * {@code timestamp} is older than {@code now() - SWEEP_SAFETY_WINDOW_MS}
     * are unambiguously leftovers from a previous primary tenure (crash / stop
     * mid-cycle / role shift) and safe to delete. 5 s is comfortably larger
     * than any trigger execution + commit + visibility window yet small enough
     * that residuals don't linger long after promotion.
     */
    private static final long SWEEP_SAFETY_WINDOW_MS = 5_000L;

    /**
     * BUG-070: how long to suppress repeated same-class pollLoop error logs.
     * At {@code pollIntervalMs=100} and primary down, the unmitigated catch
     * block produces one full stack trace every tick (~50 lines × 10/s ×
     * outage duration). 60 s cooldown reduces that to one stack per minute
     * per exception class while still preserving the first-occurrence
     * diagnostic and a periodic "still failing" signal.
     */
    private static final long ERROR_LOG_SUPPRESS_MS = 60_000L;

    /**
     * BUG-072: how often to emit a "heartbeat" CDC checkpoint save when the
     * poll batch is empty. Without this, a pristine empty cluster (freshly
     * initialized by docker/init-3node.sh with Redis wiped) never produces
     * a CDC event, so {@code saveCheckpoint()} is never called, so the
     * checkpoint's {@code updatedAt} stays at 0 forever, so
     * {@code HaAgent.evaluateServiceStates}' BUG-033 "cdcActive" guard
     * ({@code now - primaryCpUpdatedAt < 20s}) is never satisfied, so
     * every standby stays locked at {@code SYNCING} indefinitely — even
     * though {@code syncLagMs == 0} and {@code health == HEALTHY}.
     *
     * <p>The heartbeat save is cheap (one Redis HSET per tick) and preserves
     * the real cursor values ({@code lastNodeTs / lastNodeEid / ...}) untouched;
     * only {@code updatedAt} advances. 2 s strikes a balance: fast enough to
     * unblock standby promotion within the default 10 s {@code stableDurationMs}
     * window, slow enough to avoid Redis write amplification (&lt; 1 write/s).</p>
     */
    private static final long HEARTBEAT_CHECKPOINT_INTERVAL_MS = 2_000L;

    private final CdcCollectorConfig config;
    private final CheckpointManager checkpointManager;
    private final HaMetrics metrics;
    private final StreamPublishService publishService;
    private final StreamPublisher streamPublisher;
    private final PublishBuffer publishBuffer;

    private CypherPollingStrategy pollingStrategy;
    private ChangeEventBuilder eventBuilder;
    private DiffCalculator diffCalculator;
    private PollingState pollingState;
    private ScheduledExecutorService scheduler;
    // BUG-062: background repair of "naked" rels (afterAsync trigger drops).
    // Lifecycle is coupled to the CDC collector — start/stop together and
    // reset per-primary cursors on switchTarget().
    private final NakedRelationshipHealer nakedRelHealer = new NakedRelationshipHealer();
    private volatile boolean running;
    private volatile Driver currentDriver;
    private String nodeId;
    private String database;
    private long fencingToken;
    private volatile String lastStreamId;

    // BUG-070 bookkeeping for pollLoop error rate-limiter. All three fields
    // are accessed only on the single cdc-collector thread (the scheduler is
    // single-threaded), so volatility is belt-and-braces for observability.
    private volatile String lastErrorClass;
    private volatile long lastErrorLogMs;
    private volatile long suppressedErrorCount;

    // BUG-072 bookkeeping for empty-poll heartbeat checkpoint. See the
    // HEARTBEAT_CHECKPOINT_INTERVAL_MS field doc for why this exists.
    private volatile long lastHeartbeatMs;

    public CdcCollector(CdcCollectorConfig config, CheckpointManager checkpointManager,
                         StreamPublishService publishService, StreamPublisher streamPublisher,
                         PublishBuffer publishBuffer,
                         HaMetrics metrics) {
        this.config = config;
        this.checkpointManager = checkpointManager;
        this.publishService = publishService;
        this.streamPublisher = streamPublisher;
        this.publishBuffer = publishBuffer;
        this.metrics = metrics;
    }

    public void start(Driver primaryDriver, String nodeId, String database, long fencingToken) {
        this.currentDriver = primaryDriver;
        this.nodeId = nodeId;
        this.database = database;
        this.fencingToken = fencingToken;
        // Keep publisher token aligned with current CDC epoch.
        // Without this, after switchover/failover the publisher may still use
        // a stale token and Redis will reject publishes.
        this.publishService.setFencingToken(fencingToken);
        log.info("CDC publish fencing token set to {}", fencingToken);

        this.diffCalculator = new DiffCalculator(config.cacheMaxSize());
        this.pollingStrategy = new CypherPollingStrategy(primaryDriver, database, config);
        this.eventBuilder = new ChangeEventBuilder(diffCalculator, database, fencingToken);

        // Restore checkpoint
        var checkpoint = checkpointManager.loadCdcCheckpoint(nodeId);
        if (checkpoint.isPresent()) {
            var cp = checkpoint.get();
            // BUG-057: independent node/rel/delete cursors restored from v2
            // checkpoint. v1 data is auto-migrated in CheckpointManager.
            this.pollingState = new PollingState(
                cp.lastNodeTs(), cp.lastNodeEid(),
                cp.lastRelTs(), cp.lastRelEid(),
                cp.lastDeleteTs(), cp.lastDeleteEid());
            this.lastStreamId = cp.lastStreamId();
            log.info("CDC restored checkpoint: lastNodeTs={}, lastNodeEid={}, " +
                "lastRelTs={}, lastRelEid={}, lastStreamId={}",
                cp.lastNodeTs(), cp.lastNodeEid(), cp.lastRelTs(), cp.lastRelEid(),
                cp.lastStreamId());
        } else {
            this.pollingState = PollingState.initial();
            this.lastStreamId = null;
            log.info("CDC starting from beginning (no checkpoint found)");
        }

        // BUG-067: self-healing startup sweep of orphan _CDCDeleteEvent
        // transit nodes. The in-poll cleanup at pollLoop (CdcCollector L288-296)
        // only fires when `maxDeleteTs > 0` AND publish succeeded; if a previous
        // primary tenure on this local DB was interrupted between publish and
        // cleanup (crash, stop, role shift), those events become permanent
        // orphans — the next primary's cleanup runs on ITS local DB, never this
        // one's. We catch them here at promotion time: any _CDCDeleteEvent
        // whose timestamp is older than SWEEP_SAFETY_WINDOW_MS predates the
        // current tenure and is safe to delete. Best-effort: failure here must
        // NOT block CDC startup, since the poll-loop cleanup will still run
        // against fresher events.
        try (Session sweepSession = primaryDriver.session(SessionConfig.forDatabase(database))) {
            long sweepCutoffMs = System.currentTimeMillis() - SWEEP_SAFETY_WINDOW_MS;
            long swept = pollingStrategy.getDeleteCapture()
                .cleanupDeleteEvents(sweepSession, sweepCutoffMs);
            if (swept > 0) {
                log.warn("Startup sweep removed {} orphan _CDCDeleteEvent transit node(s) " +
                         "left over from a previous primary tenure on {} (BUG-067)",
                         swept, nodeId);
            }
        } catch (Exception e) {
            log.error("Startup sweep of orphan _CDCDeleteEvent failed on {} ({}); " +
                      "continuing CDC startup — in-poll cleanup will handle fresh events",
                      nodeId, e.toString());
        }

        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cdc-collector");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::pollLoop, 0, config.pollIntervalMs(), TimeUnit.MILLISECONDS);
        // BUG-062: start the background naked-rel healer on the same primary.
        // It runs on an independent schedule (default 5s) and never blocks the
        // CDC poll loop. Cursors are reset by onPrimaryChanged() in switchTarget.
        nakedRelHealer.start(primaryDriver, database);
        log.info("CDC Collector started for node {} at {}", nodeId, primaryDriver);
    }

    /**
     * Halt the scheduled poll loop and drain any remaining writes from the primary.
     *
     * <p>BUG-048: Between the last ordinary pollLoop tick and the moment callers
     * invoke {@code stop()}, up to {@code pollIntervalMs} worth of committed
     * writes may not yet have been fetched from the primary. When this is called
     * immediately after {@code HaProxyUpdater.blockWrites()} — the recommended
     * switchover order — the primary is quiescent (no further client writes can
     * commit) so it is safe and correct to run a final synchronous poll here and
     * drain those last writes into the Stream before saving the checkpoint.</p>
     *
     * <p>If the final poll fails (Neo4j unreachable, Redis reject, etc.), we log
     * and proceed: the checkpoint is not advanced, so a retry (typically on the
     * new primary after {@code switchTarget}) will re-fetch from the last saved
     * cursor.</p>
     */
    public void stop() {
        running = false;
        // BUG-062: stop heal scans BEFORE the quiescence drain. The healer
        // only writes to the primary (SET r._elementId/_updated_at/...), and
        // those writes must not race with InflightTxDrainWaiter + final
        // pollLoop() during switchover. Stopping it first guarantees the
        // subsequent drain sees a clean snapshot.
        nakedRelHealer.stop();
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // BUG-048 + BUG-056: drain residual commits from the quiesced primary.
        //
        // BUG-056 refinement: the old exit condition ("pollingState not
        // advancing") was too eager — if the first drain poll ran BEFORE
        // Neo4j's in-flight tx finished committing (blockWrites returns don't
        // imply kernel quiescence), it would see an un-advanced state and
        // exit, leaving tail commits unpublished. The new condition requires
        // BOTH:
        //   - pollingState hasn't advanced this round (steady state), AND
        //   - at least {@code MIN_DRAIN_MS} has elapsed since stop() started.
        // The minimum time is defence-in-depth on top of {@code
        // InflightTxDrainWaiter} (FailoverOrchestrator Phase 2.5). Even if
        // Phase 2.5 is skipped or fails, 300ms of additional drain polling
        // gives Neo4j kernel's tail tx room to land and be picked up.
        //
        // Only runs if we still have a live driver + polling strategy (set by
        // start()). Reset running in finally.
        if (currentDriver != null && pollingStrategy != null) {
            final long drainStart = System.currentTimeMillis();
            final long minDrainMs = 300L; // BUG-056 defence-in-depth
            final long maxDrainMs = 5000L; // hard cap on total drain time
            final int maxDrainLoops = 20;
            try {
                running = true;
                int advanced = 0;
                for (int i = 0; i < maxDrainLoops; i++) {
                    // BUG-057: watch ALL independent cursors (node / rel /
                    // delete) — checking only the legacy composite would miss
                    // the case where a node cursor advances but the rel
                    // cursor doesn't (or vice versa), because
                    // getLastTs() = max(nodeTs, relTs) keeps the max pinned.
                    long beforeNodeTs = pollingState.getLastNodeTs();
                    String beforeNodeEid = pollingState.getLastNodeEid();
                    long beforeRelTs = pollingState.getLastRelTs();
                    String beforeRelEid = pollingState.getLastRelEid();
                    long beforeDelTs = pollingState.getLastDeleteTs();
                    String beforeDelEid = pollingState.getLastDeleteEid();
                    pollLoop();
                    boolean allSame =
                        beforeNodeTs == pollingState.getLastNodeTs()
                        && java.util.Objects.equals(beforeNodeEid, pollingState.getLastNodeEid())
                        && beforeRelTs == pollingState.getLastRelTs()
                        && java.util.Objects.equals(beforeRelEid, pollingState.getLastRelEid())
                        && beforeDelTs == pollingState.getLastDeleteTs()
                        && java.util.Objects.equals(beforeDelEid, pollingState.getLastDeleteEid());
                    if (!allSame) advanced++;

                    long elapsed = System.currentTimeMillis() - drainStart;
                    if (allSame) {
                        // BUG-056: require minDrainMs floor before accepting quiet.
                        if (elapsed >= minDrainMs) break;
                        // sleep a short tick before re-polling so we don't hot-loop.
                        try { Thread.sleep(Math.max(25L, config.pollIntervalMs() / 2)); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    } else if (elapsed >= maxDrainMs) {
                        // Steady progress but we've burned our budget — stop
                        // draining anyway. Extremely unusual (means writes are
                        // still landing on OLD after blockWrites+Phase 2.5),
                        // but we refuse to block the switchover indefinitely.
                        log.warn("CDC Collector final drain hit maxDrainMs ({}ms) with "
                            + "pollingState still advancing — possible orphan writes", maxDrainMs);
                        break;
                    }
                }
                long elapsed = System.currentTimeMillis() - drainStart;
                log.info("CDC Collector final drain complete ({} additional batches, {}ms)",
                    advanced, elapsed);
            } catch (Exception e) {
                log.warn("CDC Collector final drain poll failed: {}", e.toString());
            } finally {
                running = false;
            }
        }
        saveCheckpoint();
        log.info("CDC Collector stopped");
    }

    public void switchTarget(Driver newDriver, String newNodeId, long newFencingToken) {
        // Refresh token immediately to minimize race windows during switchover.
        this.fencingToken = newFencingToken;
        this.publishService.setFencingToken(newFencingToken);
        log.info("CDC switchTarget requested: node={} token={}", newNodeId, newFencingToken);
        stop();
        // BUG-062: drop per-primary heal cursors so the new primary doesn't
        // inherit stale elementId ranges from the old primary.
        nakedRelHealer.onPrimaryChanged();
        start(newDriver, newNodeId, database, newFencingToken);
    }

    private void pollLoop() {
        if (!running) return;

        List<ChangeEvent> events = null;
        try {
            var sample = io.micrometer.core.instrument.Timer.start();

            // Detect changes
            List<RawChange> rawChanges = pollingStrategy.detectChanges(pollingState, config.batchSize());
            if (rawChanges.isEmpty()) {
                // BUG-072: emit a heartbeat checkpoint save so
                // `HaAgent.evaluateServiceStates` sees a recent updatedAt and
                // `cdcActive` becomes true even on a pristine empty cluster.
                // Cursor values (lastNodeTs/lastNodeEid/lastRelTs/lastRelEid/
                // lastDeleteTs/lastDeleteEid) are NOT modified — we just
                // republish the same pollingState so updatedAt advances.
                maybeSaveHeartbeatCheckpoint();
                sample.stop(metrics.cdcPollDuration);
                return;
            }

            // Build events (held in `events` so the FencingTokenRejected catch below
            // can buffer them instead of dropping them on the floor — BUG-047 defence
            // in depth).
            events = eventBuilder.build(rawChanges);

            // Publish
            String streamId = publishService.publishBatch(events);
            boolean publishSuccess = streamId != null;

            // C1 fix: only advance polling cursors and checkpoint when publish is confirmed.
            // If publish failed (events fell into local PublishBuffer), keep the cursors so
            // the next poll re-reads the same changes. Local buffer is best-effort persistence
            // (bounded by size + file count); it must not be trusted as the source of truth.
            if (!publishSuccess) {
                log.warn("CDC publish failed, keeping polling cursors for retry (events buffered={})", events.size());
                sample.stop(metrics.cdcPollDuration);
                return;
            }

            this.lastStreamId = streamId;

            // BUG-057: advance each cursor from the LAST record of ITS OWN
            // kind, never from the merged batch's global last. Otherwise a
            // relationship eid ("5:...") would overwrite the node cursor and
            // silently drop same-ms nodes on the next poll. `rawChanges` has
            // already been globally sorted by (ts, eid), and each capture
            // appended its results in that same order, so the last occurrence
            // of a given kind is the correct cursor target for that kind.
            rawChanges.stream()
                .filter(r -> {
                    var t = r.type();
                    return t == com.neo4j.ha.common.model.ChangeEventType.NODE_CREATED
                        || t == com.neo4j.ha.common.model.ChangeEventType.NODE_UPDATED;
                })
                .reduce((a, b) -> b)
                .ifPresent(lastNode -> {
                    pollingState.setLastNodeTs(lastNode.timestamp());
                    pollingState.setLastNodeEid(lastNode.elementId());
                });

            rawChanges.stream()
                .filter(r -> {
                    var t = r.type();
                    return t == com.neo4j.ha.common.model.ChangeEventType.RELATIONSHIP_CREATED
                        || t == com.neo4j.ha.common.model.ChangeEventType.RELATIONSHIP_UPDATED;
                })
                .reduce((a, b) -> b)
                .ifPresent(lastRel -> {
                    pollingState.setLastRelTs(lastRel.timestamp());
                    pollingState.setLastRelEid(lastRel.elementId());
                });

            // BUG-087: clean up ONLY the delete-event transit nodes actually
            // captured AND published in THIS batch, bounded by the same
            // (timestamp, _elementId) keyset the capture query pages by, then
            // advance the delete cursor to that same boundary.
            //
            // The previous code deleted every _CDCDeleteEvent with
            // `timestamp <= maxDeleteTs` right after publish. Because
            // `timestamp()` is constant within a transaction, a single
            // DETACH DELETE of more than batchSize entities creates more
            // same-timestamp transit nodes than one batch can capture; the
            // threshold cleanup then destroyed the un-captured same-timestamp
            // overflow BEFORE it was ever published, so those deletes never
            // reached standbys (standby kept data the primary had deleted →
            // standby > primary). Bounding cleanup to the captured keyset
            // prefix keeps the overflow alive for the next poll.
            //
            // `rawChanges` is globally sorted by (timestamp, elementId), and
            // RawChange.elementId() for a delete is the deleted entity's
            // `_elementId` — the exact key the capture query now orders and
            // paginates by — so the last DELETED record is the batch's
            // (maxTs, lastEid) boundary.
            rawChanges.stream()
                .filter(r -> r.type().name().contains("DELETED"))
                .reduce((a, b) -> b)
                .ifPresent(lastDelete -> {
                    try (Session session = currentDriver.session(SessionConfig.forDatabase(database))) {
                        pollingStrategy.getDeleteCapture().cleanupCapturedDeleteEvents(
                            session, lastDelete.timestamp(), lastDelete.elementId());
                    }
                    pollingState.setLastDeleteTs(lastDelete.timestamp());
                    pollingState.setLastDeleteEid(lastDelete.elementId());
                });

            // Save checkpoint
            saveCheckpoint();
            sample.stop(metrics.cdcPollDuration);

        } catch (FencingTokenRejectedException e) {
            // BUG-047 defence in depth: if a reject slips through (e.g. a future
            // code path forgets to stop CDC before increment), push the built-but-
            // -unpublished events into the local buffer rather than drop them. Any
            // subsequent CDC.start() on the new primary will refresh the publisher
            // token and `retryBuffered()` will flush them with the correct epoch.
            if (events != null && !events.isEmpty()) {
                try {
                    publishService.bufferForRetry(events);
                    log.error(
                        "Fencing token rejected mid-publish; stopping CDC Collector and " +
                        "buffering {} unpublished events for retry on next epoch", events.size(), e);
                } catch (Exception buf) {
                    log.error(
                        "Fencing token rejected AND local buffer failed — {} events dropped",
                        events.size(), buf);
                }
            } else {
                log.error("Fencing token rejected, stopping CDC Collector", e);
            }
            running = false;
        } catch (Exception e) {
            metrics.cdcPollErrors.increment();
            // BUG-070: rate-limit the stack-trace log to avoid 10 errors/sec
            // during primary outages. Full stack trace is only emitted for
            // the first occurrence of a new exception class or after
            // ERROR_LOG_SUPPRESS_MS of continuous same-class failures.
            // Intermediate failures are counted silently; the cdcPollErrors
            // metric above remains the authoritative rate signal.
            String cls = e.getClass().getName();
            long now = System.currentTimeMillis();
            boolean classChanged = !cls.equals(lastErrorClass);
            boolean suppressExpired = (now - lastErrorLogMs) >= ERROR_LOG_SUPPRESS_MS;
            if (classChanged || suppressExpired) {
                long suppressed = suppressedErrorCount;
                lastErrorClass = cls;
                lastErrorLogMs = now;
                suppressedErrorCount = 0;
                if (suppressed > 0) {
                    log.error("Error in CDC poll loop ({} similar errors suppressed in last {}s)",
                        suppressed, ERROR_LOG_SUPPRESS_MS / 1000, e);
                } else {
                    log.error("Error in CDC poll loop", e);
                }
            } else {
                suppressedErrorCount++;
            }
        }
    }

    private void saveCheckpoint() {
        // BUG-057: persist all three independent cursors.
        checkpointManager.saveCdcCheckpoint(
            nodeId,
            pollingState.getLastNodeTs(),
            pollingState.getLastNodeEid(),
            pollingState.getLastRelTs(),
            pollingState.getLastRelEid(),
            pollingState.getLastDeleteTs(),
            pollingState.getLastDeleteEid(),
            lastStreamId
        );
    }

    /**
     * BUG-072: refresh the CDC checkpoint's {@code updatedAt} field on idle
     * polls so downstream {@code HaAgent.evaluateServiceStates} sees a live
     * CDC pipeline. Gated by {@link #HEARTBEAT_CHECKPOINT_INTERVAL_MS} so an
     * idle cluster produces at most one Redis write every 2 s, not one per
     * 100 ms poll tick.
     */
    private void maybeSaveHeartbeatCheckpoint() {
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatMs < HEARTBEAT_CHECKPOINT_INTERVAL_MS) {
            return;
        }
        lastHeartbeatMs = now;
        saveCheckpoint();
    }

    public boolean isRunning() {
        return running;
    }

    public FullSyncCoordinator createFullSyncCoordinator() {
        return new FullSyncCoordinator(
            currentDriver, streamPublisher, database, config.fullsyncStreamKey(),
            config.changesStreamKey(), config.fullsyncBatchSize(),
            config.fullsyncThrottleMs(), fencingToken
        );
    }
}
