package com.neo4j.ha.agent.bootstrap;

import com.neo4j.ha.common.metrics.HaMetrics;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IndexInstaller {

    private static final Logger log = LoggerFactory.getLogger(IndexInstaller.class);

    private final HaMetrics metrics;

    public IndexInstaller() {
        this(null);
    }

    public IndexInstaller(HaMetrics metrics) {
        this.metrics = metrics;
    }

    public void ensureIndexes(Driver driver, String database) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            // Get all labels
            List<String> labels = session.run("CALL db.labels() YIELD label RETURN label")
                .list(r -> r.get("label").asString());

            for (String label : labels) {
                if (label.startsWith("_")) continue; // Skip system labels

                // _updated_at index for CDC polling
                createIndexIfNotExists(session, label, "_updated_at",
                    "CREATE RANGE INDEX IF NOT EXISTS FOR (n:%s) ON (n._updated_at)".formatted(sanitize(label)));

                // BUG-083: enforce per-label uniqueness on the cluster-stable
                // `_elementId` PROPERTY. Without this constraint, two distinct
                // live nodes on the same primary can end up sharing a
                // `_elementId` value: the SyncApplier / PostSwitchoverReconciler
                // legitimately writes `SET n._elementId = $oldPrimaryEid` when
                // it MERGEs a stub on the new primary, but then a later local
                // client write — assigned a fresh internal id by Neo4j —
                // triggers `cdc-timestamp-created` which stamps
                // `n._elementId = elementId(n)`. If that fresh internal id
                // happens to equal `$oldPrimaryEid` (Neo4j's id pool is
                // independent across primaries, so collisions are rare but
                // inevitable under load), both nodes end up with the same
                // `_elementId` PROPERTY. CDC then emits two NODE_UPDATED
                // events with the same `_elementId`; standby's
                // MERGE-by-`_elementId` treats them as the same node and the
                // later event silently overwrites the earlier one's
                // properties — producing the `node_miss` divergence
                // identified by `ha-load-switchover-test.py`.
                //
                // Adding a property-uniqueness constraint converts this
                // silent corruption into a loud `ConstraintValidationFailed`
                // at write time. The colliding client transaction aborts and
                // can retry; correctness is preserved cluster-wide. The
                // `IF NOT EXISTS` clause makes constraint creation
                // idempotent across reboots and post-failover re-runs.
                //
                // Constraint creation itself fails with
                // `ConstraintValidationFailed` if the database already
                // contains a duplicate `_elementId` group — that is by
                // design: the operator must run
                // `scripts/deploy/elementid-dedup.sh` first to clear the
                // residue, then restart the agent. We catch and warn (not
                // fail) so that a startup on a dirty database still bootstraps
                // the rest of the indexes; the dup-_elementId gauge below
                // surfaces the residual count for alerting.
                //
                // We deliberately do NOT pre-create a standalone
                // `CREATE RANGE INDEX ... ON (n._elementId)` for nodes here:
                // Neo4j 5.x rejects `REQUIRE n._elementId IS UNIQUE` if a
                // non-constraint RANGE INDEX on that exact (label, prop)
                // tuple already exists ("A constraint cannot be created
                // until the index has been dropped"). The UNIQUE constraint
                // creates its own backing RANGE INDEX that serves the same
                // keyset-pagination and MERGE-lookup workloads, so the
                // standalone index was strictly redundant once the
                // constraint exists. For databases initialised by an older
                // build that did create the standalone index,
                // `dropOrphanElementIdIndex` migrates it away on the next
                // boot.
                dropOrphanElementIdIndex(session, label);
                createConstraintIfNotExists(session, label, "_elementId",
                    "CREATE CONSTRAINT %s IF NOT EXISTS FOR (n:%s) REQUIRE n._elementId IS UNIQUE"
                        .formatted(sanitize("uniq_elementid_" + label), sanitize(label)));
            }

            // BUG-083: scan for residual duplicate `_elementId` PROPERTY values
            // and surface the count via a gauge (one-shot per ensureIndexes
            // call: boot, post-failover, post-recovery). Healthy steady state
            // is 0; non-zero means either the cleanup script wasn't run after
            // an upgrade or a code path is bypassing the trigger. The scan is
            // bounded by the per-label backing RANGE INDEX provisioned by the
            // UNIQUE constraint above (Cypher `count(*)` aggregation under a
            // label scope plans to an index-only scan), so it stays cheap even
            // on large graphs. On a dirty database where constraint creation
            // failed, the orphan _elementId RANGE INDEX is still present and
            // serves the same plan.
            long dupNodes = scanDupElementIdNodes(session, labels);
            if (metrics != null) {
                metrics.dupElementIdNodes.set(dupNodes);
            }
            if (dupNodes > 0) {
                log.warn("BUG-083: found {} nodes with duplicate `_elementId` PROPERTY on '{}'. " +
                    "Run scripts/deploy/elementid-dedup.sh on the primary to clear, then restart " +
                    "the ha-agent so the UNIQUE constraint installs cleanly.", dupNodes, database);
            }

            // _CDCDeleteEvent timestamp index
            createIndexIfNotExists(session, "_CDCDeleteEvent", "timestamp",
                "CREATE RANGE INDEX IF NOT EXISTS FOR (n:_CDCDeleteEvent) ON (n.timestamp)");

            // Relationship indexes
            List<String> relTypes = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                .list(r -> r.get("relationshipType").asString());
            for (String relType : relTypes) {
                if (relType.startsWith("_")) continue; // Skip internal rel types (_PROBE_REL, etc.)
                createIndexIfNotExists(session, relType, "_updated_at",
                    "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r._updated_at)".formatted(sanitize(relType)));
                createIndexIfNotExists(session, relType, "_elementId",
                    "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r._elementId)".formatted(sanitize(relType)));
                // BUG-062: index on client-written r.createdAt for efficient naked-rel
                // heal scans (NakedRelationshipHealer). APOC cdc-rel-timestamp runs in
                // phase:'afterAsync' and very rarely drops its stamp task, leaving the
                // rel with _elementId IS NULL AND _updated_at IS NULL — those rels are
                // invisible to keyset CDC polling (NULL > $lastTs is unsatisfiable) and
                // would be permanently lost. Healer scans `r.createdAt > $cursor AND
                // r._elementId IS NULL` as a range index seek to find naked rels cheaply
                // even when total rel count is large. This index is created
                // opportunistically: if the client writes `r.createdAt` (recommended
                // contract, see ha-agent-cluster-operations.md §"Client relationship
                // write contract"), the healer takes the index-backed fast path;
                // otherwise it falls back to a full type scan.
                createIndexIfNotExists(session, relType, "createdAt",
                    "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r.createdAt)".formatted(sanitize(relType)));
            }

            log.info("Index installation complete for database '{}'", database);
        }
    }

    private void createIndexIfNotExists(Session session, String labelOrType, String property, String cypher) {
        try {
            session.run(cypher).consume();
            log.debug("Index ensured: {}({})", labelOrType, property);
        } catch (Exception e) {
            log.warn("Failed to create index for {}({}): {}", labelOrType, property, e.getMessage());
        }
    }

    // BUG-083: a database initialised by an older build (or by a manual
    // `CREATE INDEX ... ON (n._elementId)`) has a standalone, non-constraint
    // RANGE INDEX on `(label, _elementId)` that blocks UNIQUE constraint
    // creation on the same tuple ("A constraint cannot be created until the
    // index has been dropped"). We detect such an orphan via
    // `owningConstraint IS NULL` and DROP it; the subsequent constraint
    // creation will provision an equivalent backing RANGE INDEX. If a
    // constraint already exists (re-boot path), the orphan check returns
    // empty and we skip the DROP — idempotent across restarts.
    //
    // Failures here are logged-only: if the SHOW or DROP fails for some
    // reason (permissions, transient driver error), the constraint creation
    // step below will still attempt and surface the underlying problem via
    // its own warn log. The agent must keep booting — sync-applier still
    // needs `_updated_at` indexes regardless of the constraint state.
    private void dropOrphanElementIdIndex(Session session, String label) {
        try {
            String orphanName = session.run("""
                SHOW INDEXES YIELD name, labelsOrTypes, properties, owningConstraint
                WHERE labelsOrTypes = [$label]
                  AND properties = ['_elementId']
                  AND owningConstraint IS NULL
                RETURN name LIMIT 1
                """, java.util.Map.of("label", label))
                .stream()
                .map(r -> r.get("name").asString())
                .findFirst()
                .orElse(null);
            if (orphanName != null) {
                session.run("DROP INDEX %s IF EXISTS".formatted(sanitize(orphanName))).consume();
                log.info("Dropped orphan _elementId RANGE INDEX '{}' on label {} so the " +
                    "BUG-083 UNIQUE constraint can install (its backing index will be created next).",
                    orphanName, label);
            }
        } catch (Exception e) {
            log.warn("Failed to evaluate / drop orphan _elementId index for label {}: {}",
                label, e.getMessage());
        }
    }

    // BUG-083: helper for property-uniqueness constraints. The catch must NOT
    // crash the agent: on a database that already has duplicate `_elementId`
    // values, Neo4j returns `Unable to create CONSTRAINT ...` because the
    // constraint validation fails on existing data. We log loudly so the
    // operator runs the dedup script, but the rest of the bootstrap proceeds
    // (the sync-applier still needs `_updated_at` indexes etc. to function).
    private void createConstraintIfNotExists(Session session, String labelOrType, String property, String cypher) {
        try {
            session.run(cypher).consume();
            log.debug("Constraint ensured: {}({}) IS UNIQUE", labelOrType, property);
        } catch (Exception e) {
            log.warn("Failed to create UNIQUE constraint for {}({}): {} — likely existing duplicates; " +
                "run scripts/deploy/elementid-dedup.sh and restart", labelOrType, property, e.getMessage());
        }
    }

    // BUG-083: count the total number of nodes participating in any duplicate
    // `_elementId` PROPERTY group, scanned per-label. We sum over the
    // already-discovered user labels rather than running a global
    // `MATCH (n) WHERE n._elementId IS NOT NULL` scan because (a) the
    // per-label `_elementId` RANGE INDEX makes the per-label aggregation
    // index-only, and (b) skipping system labels (`_CDCDeleteEvent`, future
    // internal labels) keeps the gauge focused on user data — internal
    // transit nodes are short-lived and not subject to the same identity
    // contract.
    private long scanDupElementIdNodes(Session session, List<String> labels) {
        long total = 0;
        List<String> labelsForScan = new ArrayList<>();
        for (String label : labels) {
            if (label.startsWith("_")) continue;
            labelsForScan.add(label);
        }
        for (String label : labelsForScan) {
            try {
                Long perLabel = session.run("""
                    MATCH (n:%s) WHERE n._elementId IS NOT NULL
                    WITH n._elementId AS eid, count(*) AS c
                    WHERE c > 1
                    RETURN coalesce(sum(c), 0) AS dup_nodes
                    """.formatted(sanitize(label)))
                    .single().get("dup_nodes").asLong();
                total += perLabel;
            } catch (Exception e) {
                log.warn("Dup _elementId scan failed for label {}: {}", label, e.getMessage());
            }
        }
        return total;
    }

    private String sanitize(String name) {
        if (name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) return name;
        return "`" + name.replace("`", "``") + "`";
    }
}
