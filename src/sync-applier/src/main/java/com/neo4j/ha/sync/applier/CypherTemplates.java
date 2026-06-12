package com.neo4j.ha.sync.applier;

public final class CypherTemplates {

    private CypherTemplates() {}

    // Node create/update (idempotent MERGE)
    // %s = label list (e.g. Person:Employee)
    static final String NODE_MERGE = """
        MERGE (n:%s {_elementId: $elementId})
        SET n = $properties
        SET n._elementId = $elementId
        """;

    // Node delete (with label for index acceleration)
    static final String NODE_DELETE = """
        MATCH (n:%s {_elementId: $elementId})
        DETACH DELETE n
        """;

    // Node delete without label (fallback)
    static final String NODE_DELETE_NO_LABEL = """
        MATCH (n {_elementId: $elementId})
        DETACH DELETE n
        """;

    // Relationship create/update.
    //
    // BUG-078 (2026-04-17): Before this fix we used `MERGE (a)-[r:%s {_elementId: $eid}]->(b)`.
    // Neo4j 5.x has a silent-failure case: when the same batch contains
    // RELATIONSHIP_CREATED → RELATIONSHIP_DELETED → RELATIONSHIP_CREATED for
    // the SAME `_elementId` (which happens when primary's internal rel id gets
    // reused after the first DELETE — reproducible via direct cypher, see
    // docs/nuclear-fusion/design/modules/ha-agent-design.md "BUG-078"), the
    // second MERGE matches the pending-deleted ghost rel, and at commit both
    // the old and new ends up marked as deleted, leaving ZERO relationships.
    //
    // Fix (BUG-078): use explicit OPTIONAL MATCH + DELETE + CREATE instead of
    // MERGE for the rel itself. OPTIONAL MATCH + DELETE cleanly invalidates
    // any prior ghost before CREATE, and CREATE has no "find or create"
    // ambiguity that MERGE has.
    //
    // BUG-079 (2026-04-17): After BUG-078 shrank the diff from 20 → 3 rels,
    // the residual 3 missing rels all shared a pattern: their endpoint nodes
    // appeared in stream ONLY as NODE_UPDATED (never NODE_CREATED), because
    // CdcCollector polls by `_updated_at` and saw these nodes first in an
    // already-mutated state (`_created_at != _updated_at`). Meanwhile each
    // rel's `_updated_at` (stamped by `cdc-rel-timestamp` afterAsync right at
    // rel-commit time) is LOWER than its endpoint node's `_updated_at` (a
    // later business-level SET property bumped the node). CdcCollector sorts
    // emitted events by `_updated_at` ASC, so stream order becomes:
    //     RELATIONSHIP_CREATED (t=3)      ← rel goes out first
    //     ...
    //     NODE_UPDATED endpoints (t=5)    ← endpoints go out later
    // When sync-applier applies this batch, the rel's `MATCH (a:Label {_elementId: X})`
    // finds nothing (endpoint not yet MERGEd on standby). The query returns
    // zero rows, so `CREATE (a)-[r]->(b)` never executes. The event is then
    // XACK'd and lost forever. Symptom: primary has N rels, standbys have
    // N-3 with the missing rels' endpoints fully present on the standby.
    //
    // Fix (BUG-079): MERGE endpoint nodes instead of MATCH. If the endpoint
    // doesn't yet exist on the standby, we create a minimal stub (label +
    // _elementId) so CREATE can attach the rel. The later NODE_UPDATED event
    // then finds the stub by `_elementId` + same label and fills in all
    // properties via `SET n = $properties`. No data is lost, idempotency is
    // preserved, and cross-event ordering no longer silently drops rels.
    //
    // BUG-082 (2026-04-22): the BUG-078 stale-match originally used
    // `OPTIONAL MATCH ()-[stale:T {_elementId: X}]->()` — i.e. "find ANY rel
    // of type T with this `_elementId`, regardless of endpoints". That is
    // correct when `_elementId` is a stable app-level key, but Neo4j 5.x's
    // `_elementId` is the raw internal relationship id which gets RECYCLED
    // across transactions once a rel is deleted (observable: delete a rel,
    // create a new rel in the next tx — new rel often reuses the old id).
    //
    // Observed failure: steady-0 creates A=(49)-[:R {_eid=X}]->(50), applied
    // on standby. A is later deleted on primary; the REL_DELETED event does
    // its job and A is gone on standby. Much later, steady-3 creates
    // B=(914)-[:R {_eid=X}]->(915) — Neo4j reuses `_elementId=X`. Standby
    // receives RELATIONSHIP_CREATED for B; the old body's `OPTIONAL MATCH
    // ()-[stale {_eid=X}]->() DELETE stale` MATCHES ZERO rels (A was already
    // deleted) so the corruption is invisible here — but if A's delete event
    // was dropped/lost OR if two rels with reused `_eid` coexist mid-replay,
    // this unbounded stale-match will DELETE a completely unrelated live rel
    // that happens to carry the same recycled `_elementId`. During
    // PostSwitchoverReconciler PEL replay this fires routinely and
    // manifests as `rel_miss` on standbys with no matching `rel_extra` on
    // primary.
    //
    // Fix (BUG-082): scope the stale MATCH to this rel's specific endpoints
    // (a)-[stale]->(b). A rel's identity on the standby is now
    // (start_eid, end_eid, type, _elementId) — the `_elementId` alone is no
    // longer trusted as a global key. Reusing `_elementId` across tx with
    // different endpoints is now a no-op for the stale sweep; the CREATE
    // below still places a correct new rel between (a) and (b).
    //
    // %1$s = start labels, %2$s = end labels, %3$s = relationship type
    //
    // Neo4j 5.x: after MERGE, a following MATCH/OPTIONAL MATCH must be
    // separated by WITH (otherwise: "WITH is required between MERGE and MATCH").
    static final String REL_MERGE = """
        MERGE (a:%1$s {_elementId: $startNodeId})
        MERGE (b:%2$s {_elementId: $endNodeId})
        WITH a, b
        OPTIONAL MATCH (a)-[stale:%3$s {_elementId: $relElementId}]->(b)
        DELETE stale
        CREATE (a)-[r:%3$s]->(b)
        SET r = $properties
        SET r._elementId = $relElementId
        """;

    // Relationship delete (endpoint-scoped; preferred).
    // BUG-082: same rationale as REL_MERGE — matching by `_elementId` alone
    // is unsafe under Neo4j id-reuse. When the CDC delete event carries
    // start/end node `_elementId`s (trigger enriched after BUG-082), use
    // this endpoint-scoped form so we only delete the specific rel we meant
    // to delete, never a later rel that happens to inherit the recycled id.
    //
    // %s = relationship type
    static final String REL_DELETE_SCOPED = """
        MATCH (a {_elementId: $startNodeId})-[r:%s {_elementId: $relElementId}]->(b {_elementId: $endNodeId})
        DELETE r
        """;

    // Relationship delete (legacy, elementId-only). Kept for backwards
    // compatibility with `_CDCDeleteEvent` transit nodes written by a
    // pre-BUG-082 trigger (rolling upgrade window) where startElementId /
    // endElementId are not populated. BUG-082 applier code prefers the
    // scoped variant whenever start/end eids are present.
    static final String REL_DELETE = """
        MATCH ()-[r:%s {_elementId: $relElementId}]->()
        DELETE r
        """;
}
