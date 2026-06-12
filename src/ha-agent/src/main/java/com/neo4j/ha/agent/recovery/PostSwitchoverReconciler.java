package com.neo4j.ha.agent.recovery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.redis.CheckpointManager; // Javadoc-only ref
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * BUG-080: Reverse-reconcile writes stranded on an OLD primary that crashed
 * during a failover, executed at recovery time once the old primary comes back
 * as a STANDBY and is reachable via bolt.
 *
 * <p><b>The gap this closes.</b> The {@code cdc-rel-timestamp} APOC trigger
 * runs in {@code phase:'afterAsync'} (see BUG-055 for why that is unavoidable
 * on Neo4j 5.x). A rel's {@code _updated_at} stamp is written in a separate tx
 * <b>after</b> the client commit has already ACK'd. During a <i>graceful</i>
 * switchover the orchestrator guarantees the gap is closed via drain
 * invariants ({@code InflightTxDrainWaiter}, {@code drainRelTriggerAfterAsync},
 * {@code CdcCollector.stop}'s final poll). During a <b>crash</b> failover
 * (kill -9, network partition, power loss) none of those drains run: the old
 * primary dies with stamped rels + {@code _CDCDeleteEvent} scratch nodes still
 * on its disk that the CDC cursor never advanced past. The checkpoint is
 * copied verbatim to the new primary and its cursor only moves forward, so
 * those events are physically stranded — visible on the old primary's disk,
 * invisible to every future CDC poll anywhere.</p>
 *
 * <p><b>When this runs.</b> {@code FailoverOrchestrator.doFailoverPhases2to10}
 * records a {@link CheckpointManager.PendingReconcile} intent in Redis right
 * after {@code cdcCollector.stop()} captures the old primary's last CDC
 * checkpoint. Much later, once the old primary is restarted and becomes
 * reachable as a STANDBY, {@code OldPrimaryRecovery.execute} Step 4.5 loads
 * that intent and calls {@link #reconcile} with the snapshotted cursor —
 * scanning the old primary's disk for rels/deletes past the failover-time
 * cursor and replaying them onto <i>whichever node is primary now</i>
 * (the cluster may have failed over again in the meantime). The new primary's
 * CDC then publishes the replay to the stream; standbys converge through the
 * normal replication path.</p>
 *
 * <p>Graceful switchover does <b>not</b> invoke this reconciler. A drain that
 * leaves stranded writes is a bug in the drain itself, not something to
 * paper over with a reverse-reconcile that would mask the bug and add switchover
 * RTO. See Phase 2.5 / 2.6 in {@code FailoverOrchestrator}.</p>
 *
 * <p><b>What this deliberately does NOT do.</b>
 * <ul>
 *   <li>Standalone node CREATE/UPDATE is not scanned: node stamping runs
 *       in {@code phase:'before'}, atomic with commit. Endpoint nodes of
 *       reconciled relationships are still resynced in {@link
 *       #replayCreate}, which covers the common case.</li>
 *   <li>Naked rels ({@code _elementId IS NULL}) are not handled here — that
 *       is {@link com.neo4j.ha.cdc.heal.NakedRelationshipHealer}'s job on
 *       whichever node is currently primary.</li>
 *   <li>No rollback on replay failure: reconcile is advisory. A failure is
 *       logged with the offending {@code _elementId}s so an operator can
 *       inspect; the cluster is no worse than it was before this class
 *       existed.</li>
 * </ul>
 */
public class PostSwitchoverReconciler {

    private static final Logger log = LoggerFactory.getLogger(PostSwitchoverReconciler.class);

    private static final int SCAN_BATCH = 2_000;

    /**
     * Hard wall-clock budget for the entire reconcile call (scan + replay).
     * Reconcile runs synchronously inside {@code OldPrimaryRecovery.execute}
     * Step 4.5, which is on the critical path of bringing a recovered node
     * back to ONLINE — every ms spent here delays the node rejoining and
     * thus the cluster's quorum margin. 10 s is generous for the expected
     * ≤dozens-of-events afterAsync gap, and short enough that a runaway
     * scan (e.g. against a node with millions of rels and a clock-skewed
     * cursor) cannot stall recovery indefinitely. Events still stranded on
     * the old primary after the budget expires are observable via
     * {@code rel-gap-diag.sh} and can be hand-reconciled.
     */
    private static final long RECONCILE_BUDGET_MS = 10_000L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ClusterStateManager clusterState;
    private final String database;

    public PostSwitchoverReconciler(ClusterStateManager clusterState, String database) {
        this.clusterState = clusterState;
        this.database = database;
    }

    /**
     * Reverse-reconcile stranded writes on {@code oldPrimaryId} onto
     * {@code newPrimaryId}, using a caller-supplied cursor pair captured at
     * failover time.
     *
     * <p>The caller must pass the cursor from the failover-time snapshot
     * (persisted in {@link CheckpointManager#savePendingReconcile}), NOT
     * {@link CheckpointManager#loadCdcCheckpoint} of the old primary — the
     * CDC key for that node may have been overwritten by {@code
     * copyCdcCheckpoint} or by a later failover. Decoupling the cursor from
     * the CDC key ensures we scan the same range the failover-time drain
     * missed, independent of subsequent cluster churn.</p>
     *
     * @param oldPrimaryId  node whose disk we read for stranded events
     * @param newPrimaryId  current primary of the cluster (may differ from
     *                      the "new primary at failover time" if another
     *                      failover happened before recovery)
     * @param tRel          rel cursor snapshotted at failover
     * @param tDel          delete cursor snapshotted at failover
     * @return number of events successfully replayed onto {@code newPrimaryId}
     */
    public int reconcile(String oldPrimaryId, String newPrimaryId, long tRel, long tDel) {
        Driver oldDriver = clusterState.getDriver(oldPrimaryId);
        Driver newDriver = clusterState.getDriver(newPrimaryId);
        if (oldDriver == null || newDriver == null) {
            log.warn("Reconcile skipped: missing driver (old={}, new={})",
                oldDriver == null ? "null" : "ok",
                newDriver == null ? "null" : "ok");
            return 0;
        }

        // Cold-start guard: if both cursors are still at 0 we've never
        // successfully published anything, so "scan for events past the
        // cursor" degenerates into "scan everything on the old primary",
        // bounded only by SCAN_BATCH. Skip — there is nothing stranded
        // that wasn't already missing before the cluster ever ran.
        if (tRel == 0L && tDel == 0L) {
            log.info("Reconcile skipped for {}: cold cursors (never published)", oldPrimaryId);
            return 0;
        }

        long deadlineMs = System.currentTimeMillis() + RECONCILE_BUDGET_MS;

        List<PendingRel> pendingRels;
        List<PendingDelete> pendingDeletes;
        try {
            pendingRels = scanPendingRels(oldDriver, tRel);
        } catch (Exception e) {
            log.warn("Reconcile: rel scan on {} failed ({}); skipping", oldPrimaryId, e.toString());
            return 0;
        }
        try {
            pendingDeletes = scanPendingDeletes(oldDriver, tDel);
        } catch (Exception e) {
            log.warn("Reconcile: delete scan on {} failed ({}); proceeding with rels only",
                oldPrimaryId, e.toString());
            pendingDeletes = List.of();
        }

        if (pendingRels.isEmpty() && pendingDeletes.isEmpty()) {
            log.info("Reconcile: no stranded events past cursor on {} (lastRelTs={}, lastDeleteTs={})",
                oldPrimaryId, tRel, tDel);
            return 0;
        }

        log.info("Reconcile: {} stranded rel create/update + {} stranded rel delete on {} " +
                 "(cursors: rel={}, del={})",
            pendingRels.size(), pendingDeletes.size(), oldPrimaryId, tRel, tDel);

        List<Event> merged = merge(pendingRels, pendingDeletes);
        long t0 = System.currentTimeMillis();
        int replayed = replayAll(newDriver, merged, deadlineMs);
        long dt = System.currentTimeMillis() - t0;
        int total = merged.size();
        if (replayed < total) {
            log.warn("Reconcile partial: replayed {}/{} event(s) onto new primary {} in {}ms " +
                "(budget {}ms); remaining {} event(s) stranded on {} — run rel-gap-diag.sh",
                replayed, total, newPrimaryId, dt, RECONCILE_BUDGET_MS,
                total - replayed, oldPrimaryId);
        } else {
            log.info("Reconcile complete: replayed {} event(s) onto new primary {} in {}ms",
                replayed, newPrimaryId, dt);
        }
        return replayed;
    }

    // ---- scan ----------------------------------------------------------

    private List<PendingRel> scanPendingRels(Driver oldDriver, long tRel) {
        // Only rels whose afterAsync stamp has landed (_elementId IS NOT NULL).
        // Naked rels are handled separately by NakedRelationshipHealer and
        // would require fabricating an elementId here, which is out of scope.
        //
        // Endpoints are captured in full (labels + properties) so the new
        // primary receives a complete node even when the endpoint itself
        // was also stranded in the same switchover window — either as a
        // brand-new node (scenario B) or as a missed property update on a
        // pre-existing node (scenario C). The CDC event payload the normal
        // sync path carries only one label per endpoint; the reconciler
        // has direct cypher access, so we can do strictly better.
        String cypher = """
            MATCH (a)-[r]->(b)
            WHERE r._updated_at > $tRel
              AND r._elementId IS NOT NULL
              AND NOT a:_CDCDeleteEvent AND NOT b:_CDCDeleteEvent
            RETURN r._elementId   AS relEid,
                   r._updated_at  AS ts,
                   type(r)        AS relType,
                   properties(r)  AS props,
                   a._elementId   AS aEid,
                   labels(a)      AS aLabels,
                   properties(a)  AS aProps,
                   b._elementId   AS bEid,
                   labels(b)      AS bLabels,
                   properties(b)  AS bProps
            ORDER BY r._updated_at ASC, r._elementId ASC
            LIMIT $batchSize
            """;
        List<PendingRel> out = new ArrayList<>();
        try (Session s = oldDriver.session(SessionConfig.forDatabase(database))) {
            var records = s.run(cypher, Map.of("tRel", tRel, "batchSize", SCAN_BATCH)).list();
            for (var rec : records) {
                PendingRel pr = new PendingRel(
                    rec.get("relEid").asString(),
                    rec.get("ts").asLong(),
                    rec.get("relType").asString(),
                    rec.get("props").asMap(),
                    rec.get("aEid").asString(null),
                    rec.get("aLabels").asList(v -> v.asString()),
                    rec.get("aProps").asMap(),
                    rec.get("bEid").asString(null),
                    rec.get("bLabels").asList(v -> v.asString()),
                    rec.get("bProps").asMap()
                );
                if (pr.aEid == null || pr.bEid == null) {
                    log.debug("Reconcile: skipping rel {} with missing endpoint _elementId", pr.relEid);
                    continue;
                }
                out.add(pr);
            }
        }
        return out;
    }

    private List<PendingDelete> scanPendingDeletes(Driver oldDriver, long tDel) {
        // Both REL_DELETED and NODE_DELETED are reconciled. The NODE path
        // is belt-and-braces: node-side triggers run in phase:'before', so
        // they are atomic with commit and the CDC drain almost always
        // catches them. But the drain loop has a bounded window and a
        // late-landing delete can still miss the cursor, so excluding
        // NODE_DELETED would create a small asymmetric gap.
        String cypher = """
            MATCH (e:_CDCDeleteEvent)
            WHERE e.eventType IN ['REL_DELETED', 'NODE_DELETED']
              AND e.timestamp > $tDel
              AND e.elementId IS NOT NULL
            RETURN e.eventType AS eventType,
                   e.elementId AS eid,
                   e.timestamp AS ts,
                   e.relType   AS relType,
                   e.labels    AS labelsJson
            ORDER BY e.timestamp ASC, e.elementId ASC
            LIMIT $batchSize
            """;
        List<PendingDelete> out = new ArrayList<>();
        try (Session s = oldDriver.session(SessionConfig.forDatabase(database))) {
            var records = s.run(cypher, Map.of("tDel", tDel, "batchSize", SCAN_BATCH)).list();
            for (var rec : records) {
                String eventType = rec.get("eventType").asString();
                String relType = "REL_DELETED".equals(eventType)
                    ? rec.get("relType").asString(null)
                    : null;
                // NODE_DELETED trigger stamps labels as a JSON list string
                // (see ApocTriggerInstaller.NODE_DELETE_TRIGGER line 464).
                // REL_DELETED has no labels field; keep null.
                List<String> labels = "NODE_DELETED".equals(eventType)
                    ? parseLabelsJson(rec.get("labelsJson").asString("[]"))
                    : null;
                out.add(new PendingDelete(
                    eventType,
                    rec.get("eid").asString(),
                    rec.get("ts").asLong(),
                    relType,
                    labels
                ));
            }
        }
        return out;
    }

    private static List<String> parseLabelsJson(String json) {
        if (json == null || json.isEmpty()) return Collections.emptyList();
        try {
            return MAPPER.readValue(json, new TypeReference<List<String>>() {});
        } catch (Exception e) {
            log.debug("Reconcile: failed to parse _CDCDeleteEvent.labels JSON '{}': {}",
                json, e.toString());
            return Collections.emptyList();
        }
    }

    // ---- merge ---------------------------------------------------------

    private List<Event> merge(List<PendingRel> rels, List<PendingDelete> deletes) {
        List<Event> merged = new ArrayList<>(rels.size() + deletes.size());
        merged.addAll(rels);
        merged.addAll(deletes);
        merged.sort(Comparator.comparingLong(Event::ts)
                              .thenComparing(Event::key));
        return merged;
    }

    // ---- replay --------------------------------------------------------

    private int replayAll(Driver newDriver, List<Event> events, long deadlineMs) {
        int replayed = 0;
        try (Session s = newDriver.session(SessionConfig.forDatabase(database))) {
            for (Event ev : events) {
                if (System.currentTimeMillis() >= deadlineMs) {
                    // Budget exhausted. Events are sorted by (ts, key), so
                    // the events we've applied so far form a monotonic
                    // prefix — standbys will converge to that prefix via
                    // the normal CDC path, and the suffix remains on the
                    // old primary for operator follow-up.
                    break;
                }
                try {
                    if (ev instanceof PendingRel pr) {
                        replayCreate(s, pr);
                    } else if (ev instanceof PendingDelete pd) {
                        replayDelete(s, pd);
                    }
                    replayed++;
                } catch (Exception e) {
                    log.warn("Reconcile: replay failed for eid={} (ts={}): {}",
                        ev.key(), ev.ts(), e.toString());
                }
            }
        }
        return replayed;
    }

    private void replayCreate(Session s, PendingRel pr) {
        // Idempotent upsert mirroring CypherTemplates.REL_MERGE semantics
        // but extended to carry the endpoint's full state from old primary:
        //   - MERGE endpoint on a single ANCHOR label by _elementId. We
        //     deliberately use only one label here to avoid the Neo4j
        //     multi-label MERGE trap (a multi-label MERGE pattern is a
        //     COMPOUND existence predicate: if any label in the pattern is
        //     missing on an otherwise-matching node, MERGE creates a NEW
        //     node, duplicating identity).
        //   - SET a = $aProps  — full replace of user+HA-internal props.
        //     Safe because every HA-internal stamp we care about
        //     (_elementId/_updated_at/_created_at/_labels) is already in
        //     $aProps as captured from old primary; this is exactly the
        //     semantics CDC's node-applier uses elsewhere.
        //   - SET a:L2:L3... — additively union the remaining labels onto
        //     the node. We do NOT REMOVE labels not in $aLabels: doing so
        //     would require reading current state first, and the normal
        //     CDC path is also additive, so this keeps semantics aligned.
        //   - Then the BUG-078/079 pattern for the rel itself (stale
        //     DELETE + CREATE).
        String aAnchor = firstUserLabel(pr.aLabels);
        String bAnchor = firstUserLabel(pr.bLabels);
        String aExtraLabels = extraLabelsClause("a", pr.aLabels, aAnchor);
        String bExtraLabels = extraLabelsClause("b", pr.bLabels, bAnchor);
        String relType = sanitizeToken(pr.relType);
        String cypher = String.format("""
            MERGE (a:%1$s {_elementId: $aEid})
            SET a = $aProps
            SET a._elementId = $aEid
            %2$s
            MERGE (b:%3$s {_elementId: $bEid})
            SET b = $bProps
            SET b._elementId = $bEid
            %4$s
            WITH a, b
            OPTIONAL MATCH ()-[stale:%5$s {_elementId: $relEid}]->()
            DELETE stale
            CREATE (a)-[r:%5$s]->(b)
            SET r = $props
            SET r._elementId = $relEid
            """, aAnchor, aExtraLabels, bAnchor, bExtraLabels, relType);

        s.run(cypher, Map.of(
            "aEid", pr.aEid,
            "bEid", pr.bEid,
            "aProps", pr.aProps,
            "bProps", pr.bProps,
            "relEid", pr.relEid,
            "props", pr.props
        )).consume();
    }

    private void replayDelete(Session s, PendingDelete pd) {
        if ("NODE_DELETED".equals(pd.eventType)) {
            // Label-scoped node MATCH by _elementId so the planner uses
            // the per-label RANGE INDEX on _elementId (see
            // IndexInstaller.java:30). Without scoping the plan degrades
            // to AllNodesScan + property filter, which on a large graph
            // inflates the switchover critical path from ms to seconds
            // per event.
            //
            // DETACH DELETE is mandatory: by the time we replay, the new
            // primary may already have rebuilt inbound/outbound edges
            // through its normal CDC path (e.g. the node's missing
            // rel-create was replayed just above us), and a plain DELETE
            // would fail with
            // Neo.DatabaseError.Transaction.TransactionHookFailed. The
            // replay is still idempotent: if the node is already gone we
            // simply match 0 rows.
            //
            // Multi-label nodes: MATCH (n:L1 {_elementId: $eid}) is
            // sufficient — the index seek on L1 pulls the single node,
            // and its other labels are irrelevant for deletion. We use
            // firstUserLabel to skip `_`-prefixed HA-internal labels;
            // the user's own label is what carries the index anyway.
            //
            // Fallback: if labels metadata is missing on the trigger
            // event (legacy or parse failure), drop to the unscoped
            // form — correctness over speed.
            String anchor = pd.labels != null && !pd.labels.isEmpty()
                ? firstUserLabel(pd.labels)
                : null;
            String cypher;
            if (anchor != null && !anchor.equals("`_ReconcileStub`")) {
                cypher = String.format("""
                    MATCH (n:%s {_elementId: $eid}) DETACH DELETE n
                    """, anchor);
            } else {
                log.debug("Reconcile: NODE_DELETED eid={} has no user label; " +
                    "falling back to unscoped DETACH DELETE", pd.eid);
                cypher = "MATCH (n {_elementId: $eid}) DETACH DELETE n";
            }
            s.run(cypher, Map.of("eid", pd.eid)).consume();
            return;
        }

        // REL_DELETED: scoping by relType when present allows the planner
        // to use the per-type relationship index on _elementId if one
        // exists; falling back to the untyped form keeps correctness when
        // relType was dropped by the BUG-066 "create-then-delete inside
        // afterAsync window" edge case.
        String cypher;
        if (pd.relType != null && !pd.relType.isEmpty()) {
            cypher = String.format("""
                MATCH ()-[r:%s {_elementId: $eid}]->()
                DELETE r
                """, sanitizeToken(pd.relType));
        } else {
            cypher = """
                MATCH ()-[r {_elementId: $eid}]->()
                DELETE r
                """;
        }
        s.run(cypher, Map.of("eid", pd.eid)).consume();
    }

    // ---- helpers -------------------------------------------------------

    private static String firstUserLabel(List<String> labels) {
        if (labels == null) return "`_ReconcileStub`";
        for (String l : labels) {
            if (l != null && !l.isEmpty() && !l.startsWith("_")) {
                return sanitizeToken(l);
            }
        }
        return "`_ReconcileStub`";
    }

    /**
     * Backtick-quote a label or relationship-type token for safe splicing
     * into a Cypher string. Cypher allows any UTF-8 identifier inside
     * backticks <em>except</em> backticks themselves (no escape sequence
     * exists), so the only two chars we must reject are the backtick and
     * the newline (newlines would let an adversarial label break out of
     * the identifier context). Everything else — spaces, colons, dashes,
     * unicode — is fine inside backticks.
     *
     * <p>Threat model is low here: a caller would already need write
     * access to the old primary to plant a malicious label. But defence
     * in depth is cheap, and letting a Cypher parse error crash the
     * switchover critical path would be worse than throwing early.
     */
    private static String sanitizeToken(String token) {
        if (token == null || token.isEmpty()) {
            return "`_ReconcileStub`";
        }
        if (token.indexOf('`') >= 0 || token.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(
                "Refusing unsafe Cypher token (contains backtick or newline): " + token);
        }
        return "`" + token + "`";
    }

    /**
     * Produce a {@code SET <var>:L2:L3:...} clause for every label other
     * than the anchor. Additive; never removes labels. Empty string if
     * there is no extra label, in which case the String.format slot
     * expands to nothing.
     */
    private static String extraLabelsClause(String var, List<String> labels, String anchor) {
        if (labels == null || labels.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String l : labels) {
            if (l == null || l.isEmpty()) continue;
            String sanitized = sanitizeToken(l);
            if (sanitized.equals(anchor)) continue;
            sb.append(':').append(sanitized);
        }
        if (sb.length() == 0) return "";
        return "SET " + var + sb;
    }

    // ---- event types ---------------------------------------------------

    private sealed interface Event permits PendingRel, PendingDelete {
        long ts();
        String key();
    }

    private record PendingRel(
        String relEid,
        long ts,
        String relType,
        Map<String, Object> props,
        String aEid,
        List<String> aLabels,
        Map<String, Object> aProps,
        String bEid,
        List<String> bLabels,
        Map<String, Object> bProps
    ) implements Event {
        @Override public String key() { return "C:" + relEid; }
    }

    private record PendingDelete(
        String eventType,   // "REL_DELETED" | "NODE_DELETED"
        String eid,
        long ts,
        String relType,     // only populated for REL_DELETED; null for NODE_DELETED
        List<String> labels // only populated for NODE_DELETED; null for REL_DELETED
    ) implements Event {
        @Override public String key() { return "D:" + eventType + ":" + eid; }
    }
}
