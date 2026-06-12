package com.neo4j.ha.sync.applier;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.ChangeEventType;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ChangeApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeApplier.class);

    private final NodeApplier nodeApplier = new NodeApplier();
    private final RelationshipApplier relApplier = new RelationshipApplier();
    private final IndexManager indexManager;
    private final String nodeKey;
    private final String database;
    private final HaMetrics metrics;

    public ChangeApplier(IndexManager indexManager, String nodeKey, String database, HaMetrics metrics) {
        this.indexManager = indexManager;
        this.nodeKey = nodeKey;
        this.database = database;
        this.metrics = metrics;
    }

    public void applyBatch(List<ChangeEvent> events, Driver standbyDriver) {
        if (events.isEmpty()) return;

        // BUG-081 plan A: detect events in this batch that share the same
        // `_elementId` (Neo4j internal rel-id reuse after DELETE — see
        // BUG-081 root-cause analysis) and split them into independent
        // sub-transactions so that the second CREATE's `OPTIONAL MATCH
        // stale {_elementId}` in CypherTemplates.REL_MERGE observes the
        // first CREATE as a *committed* rel, not an in-flight tx-local
        // write whose index entry may not yet be visible.
        //
        // Invariants:
        //   * Preserves stream order (Redis stream-id = primary commit
        //     order); we only split at duplicate boundaries, never
        //     reorder within a sub-batch.
        //   * Zero overhead in the common case: `splitByDuplicateElementId`
        //     returns the events as a single sub-batch when no duplicate
        //     exists, i.e. identical behavior to the pre-BUG-081 path.
        //   * `ensureIndexesForBatch` still runs once at session level —
        //     index creation is idempotent DDL and does not need to be
        //     re-checked per sub-batch.
        //   * If any sub-batch's executeWrite fails, we re-throw: the
        //     caller (PendingRecovery / IncrementalConsumer) must NOT
        //     ACK the stream ids for the whole batch. Already-committed
        //     sub-batches become an effectively-applied prefix; the
        //     caller's retry will re-process the entire batch, which is
        //     safe because every applier path is idempotent on
        //     `_elementId`. This matches the pre-BUG-081 atomicity
        //     contract (all-or-nothing from the ACK's perspective).
        var sample = io.micrometer.core.instrument.Timer.start();
        try (Session session = standbyDriver.session(SessionConfig.forDatabase(database))) {
            ensureIndexesForBatch(events, session);

            List<List<ChangeEvent>> subBatches = splitByDuplicateElementId(events);
            if (subBatches.size() > 1 && metrics != null) {
                metrics.batchSplitForDuplicateElementIdTotal.increment(subBatches.size() - 1);
                log.info("BUG-081: split batch of {} events into {} sub-tx(es) due to "
                    + "duplicate _elementId (Neo4j rel-id reuse)",
                    events.size(), subBatches.size());
            }

            for (List<ChangeEvent> sub : subBatches) {
                session.executeWrite(tx -> {
                    for (ChangeEvent event : sub) {
                        applyEvent(tx, event);
                    }
                    return null;
                });
            }
            metrics.syncEventsApplied.increment(events.size());
            log.debug("Applied batch of {} events in {} sub-tx(es)",
                events.size(), subBatches.size());
        } catch (Exception e) {
            metrics.syncApplyErrors.increment();
            log.error("Failed to apply batch of {} events", events.size(), e);
            throw e;
        } finally {
            sample.stop(metrics.syncApplyDuration);
        }
    }

    /**
     * Split {@code events} into ordered sub-batches such that no single
     * sub-batch contains two events with the same {@code _elementId}.
     *
     * <p>Algorithm (O(n) time, O(n) space):
     * walk the list in order, maintain a {@code seen} set of element ids
     * used by the current sub-batch, and open a new sub-batch whenever we
     * encounter an event whose element id is already in {@code seen}.</p>
     *
     * <p>Why this is the right granularity:
     * The hazard that BUG-081 triggers is confined to a <em>single</em>
     * bolt transaction (the second CREATE's OPTIONAL MATCH cannot reliably
     * see the first CREATE's uncommitted index entry). Once we commit the
     * first CREATE in its own tx, Neo4j's regular (label, _elementId)
     * range index is populated and visible to any subsequent tx. Splitting
     * more aggressively (e.g. per-event) would pay extra bolt round-trips
     * for no correctness benefit; splitting less aggressively (e.g.
     * group-by-label) still leaves the same-tx hazard intact.</p>
     *
     * <p>Events whose entity or elementId is {@code null} (e.g. control
     * events, NODE events on entities that don't carry elementId for
     * whatever reason) contribute nothing to {@code seen} and never
     * trigger a split — they ride along in whatever sub-batch they
     * happened to fall into. This is safe because the hazard is
     * specifically about elementId collision, not about tx size.</p>
     *
     * <p>Visibility: package-private to let {@code ChangeApplierTest}
     * exercise the split logic directly without going through Neo4j.</p>
     */
    static List<List<ChangeEvent>> splitByDuplicateElementId(List<ChangeEvent> events) {
        List<List<ChangeEvent>> out = new ArrayList<>();
        if (events.isEmpty()) return out;

        List<ChangeEvent> current = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (ChangeEvent ev : events) {
            String eid = (ev.entity() != null) ? ev.entity().elementId() : null;
            if (eid != null && !seen.add(eid)) {
                // Collision: this event's _elementId was already used in the
                // current sub-batch. Flush and start a fresh sub-batch with
                // this event at the head.
                out.add(current);
                current = new ArrayList<>();
                seen = new HashSet<>();
                seen.add(eid);
            }
            current.add(ev);
        }
        out.add(current);
        return out;
    }

    private void ensureIndexesForBatch(List<ChangeEvent> events, Session session) {
        for (ChangeEvent event : events) {
            if (event.entity() != null && event.entity().labels() != null) {
                for (String label : event.entity().labels()) {
                    indexManager.ensureIndex(session, nodeKey, label);
                }
            }
            if (event.entity() != null && event.entity().relationshipType() != null) {
                indexManager.ensureRelIndex(session, nodeKey, event.entity().relationshipType());
            }
            // BUG-079: REL_MERGE now `MERGE`s endpoint nodes on their labels
            // (creates stubs if the endpoint's own NODE_UPDATED hasn't been
            // applied yet — common when rel's `_updated_at` < node's). The
            // MERGE uses the `(label, _elementId)` range index, so we must
            // ensure it exists even when this batch has no NODE event for
            // that label. Without this, the first rel event referencing a
            // label would fall back to a scan, fine on small data but
            // quadratic under bursts.
            if (event.entity() != null && event.entity().startNodeLabels() != null) {
                for (String label : event.entity().startNodeLabels()) {
                    indexManager.ensureIndex(session, nodeKey, label);
                }
            }
            if (event.entity() != null && event.entity().endNodeLabels() != null) {
                for (String label : event.entity().endNodeLabels()) {
                    indexManager.ensureIndex(session, nodeKey, label);
                }
            }
        }
    }

    private void applyEvent(org.neo4j.driver.TransactionContext tx, ChangeEvent event) {
        ChangeEventType type = event.eventType();

        switch (type) {
            case NODE_CREATED -> nodeApplier.create(tx, event);
            case NODE_UPDATED -> nodeApplier.update(tx, event);
            case NODE_DELETED -> nodeApplier.delete(tx, event);
            case RELATIONSHIP_CREATED -> relApplier.create(tx, event);
            case RELATIONSHIP_UPDATED -> relApplier.update(tx, event);
            case RELATIONSHIP_DELETED -> relApplier.delete(tx, event);
            default -> log.debug("Skipping control event type: {}", type);
        }
    }
}
