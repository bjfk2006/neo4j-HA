package com.neo4j.ha.agent.bootstrap;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ApocTriggerInstaller {

    private static final Logger log = LoggerFactory.getLogger(ApocTriggerInstaller.class);

    private static final int MAX_RETRIES = 10;
    private static final long RETRY_DELAY_MS = 3000;

    // Node-side timestamps run in `phase:'before'` because they have to be
    // visible inside the same transaction (CDC polling relies on
    // `n._updated_at` being set by commit time; and the sync-applier needs
    // `n._elementId` to match on MERGE).
    //
    // BUG-055: relationship-side timestamps CANNOT run in `phase:'before'`.
    // Empirically, $createdRelationships is NEVER populated during the
    // before-commit phase in Neo4j 5.x APOC — every RELATED_TO edge created
    // via `MERGE (a)-[r:TYPE]->(b)` on the primary shows up with
    // `r._elementId IS NULL` and `r._updated_at IS NULL` after commit. That
    // makes the edge invisible to keyset-paginated CDC polling
    // (`WHERE r._updated_at > $lastTs`), so NO relationship ever gets
    // captured, published, or replicated. The fix is to run relationship
    // stamping in `phase:'afterAsync'`, which executes in a separate
    // transaction AFTER commit — at that point the new rel is fully
    // materialized and writable, and we can back-fill `_elementId` /
    // `_updated_at` / `_created_at` without ExclusiveLock contention.
    //
    // BUG-054: for nodes AND relationships, `_elementId` must be the
    // CLUSTER-STABLE identity. On the source primary that equals the local
    // `elementId(...)` (because the rel/node was just created there). When
    // sync-applier replicates it, the SOURCE primary's `_elementId` is
    // copied verbatim as a property, so standbys' local elementId differs
    // but `_elementId` agrees. We must never overwrite `_elementId` if it's
    // already set (i.e. the rel/node came in via sync-applier); only
    // initialize it on genuine local creation.
    // BUG-063: the three node-stamping branches MUST be independent triggers.
    //
    // Historical implementation used ONE trigger with chained UNWINDs:
    //
    //   UNWIND $createdNodes AS n ... SET ...
    //   WITH 1 AS _
    //   UNWIND $assignedNodeProperties AS prop ... SET ...
    //   WITH 1 AS _
    //   UNWIND $removedNodeProperties AS prop ... SET ...
    //
    // which has a fatal Cypher-semantics trap: when `$createdNodes` is an
    // empty list (i.e. the triggering tx only SETs existing nodes, no
    // CREATE), `UNWIND $createdNodes AS n` produces **zero rows**. In Cypher
    // a clause with zero input rows produces zero output rows regardless of
    // its content, so `WITH 1 AS _`, the next `UNWIND $assignedNodeProperties`,
    // and its subsequent SET all execute on the empty stream and become
    // effectively NO-OPS. Net effect: `_updated_at` was **never** bumped
    // for any SET-only transaction — `MATCH (n) ... SET n.foo = ...`
    // against an already-existing node would silently skip the stamp,
    // CDC's keyset predicate `n._updated_at > $lastTs` would never match,
    // and the change would never reach standbys. The bug was invisible in
    // the load test because its write path is `MERGE ... ON CREATE SET ...`
    // with fresh ids, so $createdNodes was non-empty on nearly every write.
    //
    // Attempt 1 to fix — ONE trigger body with three FOREACH blocks:
    //
    //   FOREACH (n IN [...] | SET ...)
    //   FOREACH (prop IN [...] | SET ...)
    //   FOREACH (prop IN [...] | SET ...)
    //
    // This failed at runtime on Neo4j 2026.02.3 + APOC 5.x — the trigger
    // body threw `42NFF` (access denied) during the readiness probe's
    // CREATE tx, suggesting one of the FOREACH filter expressions (likely
    // `NOT p.node:_CDCDeleteEvent` as a list comprehension predicate
    // evaluated against every entry in `$assignedNodeProperties` /
    // `$removedNodeProperties`) hits a Cypher-security guard we cannot
    // bypass from within a trigger body.
    //
    // Attempt 2 (current): split into THREE SEPARATE APOC triggers, each
    // with a single UNWIND on exactly ONE trigger-context list. Each
    // trigger's body is bounded and self-contained: if `$createdNodes` is
    // empty, the CREATE trigger is a full no-op (zero rows, zero SETs)
    // but the other two triggers run with their own row stream derived
    // from `$assignedNodeProperties` / `$removedNodeProperties`. This is
    // the pattern APOC documentation actually recommends; the original
    // single-trigger design was a premature "simplification".
    //
    // APOC 5.x trigger context variable types (differ from 2026.x / 6.x!):
    //
    //   $createdNodes              List<Node>
    //   $deletedNodes              List<Node>
    //   $createdRelationships      List<Relationship>
    //   $deletedRelationships      List<Relationship>
    //   $assignedLabels            Map<String, List<Node>>         (label -> [nodes])
    //   $removedLabels             Map<String, List<Node>>
    //   $assignedNodeProperties    Map<String, List<Map>>          (propKey -> [{node,key,old,new}])
    //   $removedNodeProperties     Map<String, List<Map>>          (propKey -> [{node,key,old}])
    //   $assignedRelationshipProperties   Map<String, List<Map>>   (propKey -> [{relationship,key,old,new}])
    //   $removedRelationshipProperties    Map<String, List<Map>>   (propKey -> [{relationship,key,old}])
    //
    // So any trigger body that assumes `List<Map>` for {assigned,removed}*Properties
    // must iterate over the outer Map first (by key = property name), then over the
    // inner List. Using chained UNWINDs for this collapses the row stream to 0 rows
    // when the outer Map is empty (BUG-063 root cause on single-trigger design);
    // FOREACH is safe because it's a side-effecting sub-block that doesn't affect
    // outer rows.
    //
    // The three node-stamping triggers:
    //   cdc-timestamp-created   handles $createdNodes   (List<Node>)
    //   cdc-timestamp-assigned  handles $assignedNodeProperties (Map)
    //   cdc-timestamp-removed   handles $removedNodeProperties  (Map)
    // All in phase:'before'.
    //
    // BUG-066 (2026-04-21): stamp `_labels` as a JSON string property at
    // create time. Rationale: `labels(dn)` on a node in `$deletedNodes`
    // throws `EntityNotFoundException: Node with id N has been deleted in
    // this transaction` in Neo4j 5.26 — the Cypher runtime's
    // `getLabelTokenSetForNode` checks node existence and refuses to read
    // labels for proxies marked "deleted in this tx". BUG-064/BUG-065
    // assumed `labels(dn)` was safe in `phase:'before'` (symmetric to
    // `properties(dn)` which IS safe); empirically it is not. The only
    // robust fix is to avoid calling `labels()` inside the delete trigger
    // altogether and read the labels from a pre-stamped property instead.
    // We stamp at CREATE time where `labels(n)` is unambiguously safe
    // (the node has just been created, not deleted), serialise to JSON so
    // the downstream `DeleteEventCapture.parseJsonList(...)` contract is
    // preserved bit-for-bit, and make `cdc-timestamp-assigned` skip this
    // key so trigger-side SETs do not chain-fire.
    private static final String TIMESTAMP_CREATED_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-timestamp-created',
          'UNWIND $createdNodes AS n
           WITH n WHERE NOT n:_CDCDeleteEvent
           SET n._created_at = timestamp(),
               n._updated_at = timestamp(),
               n._elementId  = coalesce(n._elementId, elementId(n)),
               n._labels     = apoc.convert.toJson(labels(n))',
          {phase: 'before'})
        """;

    // For each user-property key that was assigned in this tx, iterate the
    // list of {node,key,old,new} entries and bump _updated_at on each node.
    // The FOREACH-of-FOREACH pattern is necessary because $assignedNodeProperties
    // is Map<String, List<Map>>: outer iteration over keys, inner over the
    // entries per key. Skip the HA-internal keys and the sentinel label.
    private static final String TIMESTAMP_ASSIGNED_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-timestamp-assigned',
          'FOREACH (key IN [k IN keys($assignedNodeProperties)
                            WHERE k <> "_elementId"
                              AND k <> "_created_at"
                              AND k <> "_updated_at"
                              AND k <> "_labels"] |
             FOREACH (entry IN [e IN $assignedNodeProperties[key]
                                WHERE NOT e.node:_CDCDeleteEvent] |
               SET entry.node._updated_at = timestamp()
             )
           )',
          {phase: 'before'})
        """;

    // If a user explicitly removes _elementId (unusual but defensive), restore it.
    // Uses coalesce(..., []) to no-op when $removedNodeProperties doesn't carry
    // the _elementId key at all (the common case).
    private static final String TIMESTAMP_REMOVED_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-timestamp-removed',
          'FOREACH (entry IN [e IN coalesce($removedNodeProperties["_elementId"], [])
                              WHERE NOT e.node:_CDCDeleteEvent] |
             SET entry.node._elementId = elementId(entry.node)
           )',
          {phase: 'before'})
        """;

    // BUG-055: relationship timestamps MUST run in `afterAsync`. See
    // TIMESTAMP_TRIGGER comment above for why `before` silently drops all
    // $createdRelationships in Neo4j 5.x APOC.
    //
    // We use afterAsync (not after) because:
    //   - `after` cannot mutate newly-created entities (ExclusiveLock error)
    //   - `afterAsync` runs in a fresh transaction and can freely SET props
    // The small latency (~10ms, same as apoc.trigger.refresh) is acceptable
    // because CDC polling is already asynchronous.
    //
    // BUG-059: DELIBERATELY omit `$assignedRelationshipProperties` and
    // `$removedRelationshipProperties` branches.
    //
    // Earlier versions did `SET r._updated_at = timestamp()` on every property
    // assignment, mirroring the node trigger. For relationships under
    // `afterAsync`, that path is actively harmful:
    //
    //  1. `afterAsync` triggers run in a separate transaction AFTER commit.
    //     `timestamp()` inside them is therefore NOT the commit time — it's
    //     the time the async executor happens to dispatch the task.
    //
    //  2. APOC flushes queued afterAsync tasks even after the trigger has
    //     been `apoc.trigger.drop`'d (uninstall only removes the system-DB
    //     definition, it does not abort in-flight queued tasks). During a
    //     switchover we:
    //       (a) block writes,
    //       (b) call ApocTriggerUninstaller on the OLD primary,
    //       (c) start sync-applier on the old primary (now standby).
    //     Any `$assignedRelationshipProperties` tasks queued in window (a)
    //     flush in window (c), AFTER commit of the business tx that created
    //     them — and stamp `r._updated_at = <switchover-end-wall-clock>`.
    //     That wall-clock is 40–200 ms later than the commit time the rest
    //     of the cluster saw, so on the DEMOTED primary the same rel now
    //     has `_updated_at` != source-of-truth's `_updated_at`, while
    //     `_elementId`/`_created_at`/endpoints are identical. Integrity
    //     checks that diff `(eid, src, dst, cts, uts)` across nodes see
    //     spurious mismatches (rel_extra on demoted primary, rel_miss on
    //     others — but it's the SAME rel).
    //
    //  3. sync-applier's MERGE on replicas issues `SET r = $properties`,
    //     which carries the source's `_updated_at` verbatim. That write
    //     itself enqueues yet another assigned-props afterAsync task on
    //     the replica, which would then overwrite the replicated
    //     `_updated_at` with the replica's own wall-clock, re-diverging
    //     the cluster every round.
    //
    // The only legitimate reason we'd need `assignedRelationshipProperties`
    // is "user SETs a rel prop, CDC needs to pick up the change via
    // keyset-paginated `r._updated_at > $lastTs`". The load test never does
    // this (`relationship_loop` only MERGEs with `ON CREATE SET`), and the
    // production path for mutating a rel post-creation is to go through the
    // HA entry point, which writes on the primary where the `$createdRels`
    // branch or an explicit business-layer SET already carries the right
    // `_updated_at`. If a future workload needs in-place rel property
    // updates that must be captured by CDC, reintroduce this branch as a
    // separate `before`-phase trigger over a specific allow-list of
    // property keys, and verify it does not fire on sync-applier writes.
    //
    // `$removedRelationshipProperties` is dropped for the same reason —
    // its only use case was restoring `_elementId` after it was explicitly
    // removed, which no production workload does (the probe sentinel uses
    // its own label and is swept, not prop-removed).
    // BUG-066 (2026-04-21): stamp `_type` alongside `_elementId` for the
    // same reason nodes now stamp `_labels` — `type(dr)` on a deleted
    // relationship proxy goes through the same per-tx token-store
    // existence check as `labels(dn)` and throws
    // `EntityNotFoundException: Relationship with id N has been deleted in
    // this transaction`. Stamping at create time makes `type(r)` safe (the
    // rel was just created, not deleted) and lets `cdc-capture-rel-deletes`
    // read `dr._type` as a plain property lookup with no proxy-existence
    // check. Edge case: because this trigger runs `afterAsync`, there is a
    // sub-second window between commit and stamp where a freshly created
    // rel has `_type IS NULL`. If such a rel is deleted inside that window
    // the REL_DELETED event's `relType` field will be empty; the
    // NakedRelationshipHealer (BUG-062) already repairs `_elementId` /
    // `_updated_at` on the same schedule and can be extended to stamp
    // `_type` if this edge case is observed in production.
    private static final String REL_TIMESTAMP_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-rel-timestamp',
          'UNWIND $createdRelationships AS r
           SET r._created_at = coalesce(r._created_at, timestamp())
           SET r._updated_at = coalesce(r._updated_at, timestamp())
           SET r._elementId = coalesce(r._elementId, elementId(r))
           SET r._type      = coalesce(r._type, type(r))',
          {phase: 'afterAsync'})
        """;

    // BUG-052 fix: capture the cluster-stable identity (the node's `_elementId`
    // PROPERTY, which sync-applier copies verbatim from the source primary), NOT
    // the local Neo4j elementId. After a switchover, the new primary's local
    // elementId for a logical node differs from every other node's — the
    // sync-applier MERGE'd nodes there using the original primary's elementId.
    // Publishing the local elementId in delete events causes standby
    // `MATCH (n:TestNode {_elementId: $elementId}) DETACH DELETE n` to miss,
    // so deletes silently fail to propagate and the cluster permanently
    // diverges. Always prefer the property; fall back to local id only for
    // pre-existing nodes that never carried `_elementId` (legacy / pre-trigger).
    //
    // BUG-064 (root-caused 2026-04-20 after 0 REL_DELETED events in Redis
    // stream across an entire load-test run): the previous body reconstructed
    // deleted-entity properties by scanning
    //   $removedNodeProperties           Map<String, List<Map{node,key,old}>>
    //   $removedRelationshipProperties   Map<String, List<Map{relationship,key,old}>>
    // and filtering each inner list with `WHERE e.node = dn` / `e.relationship = dr`.
    // A direct `ping` trigger (`CREATE (:_TestPing {delRelCount:size($deletedRelationships),
    // removedRelPropKeys:size(keys($removedRelationshipProperties))})` in `phase:'before'`)
    // proved on the ACTUAL running deployment that for both explicit `DELETE r`
    // and `DETACH DELETE n` cascades, $deletedRelationships has the expected
    // size (1) and $removedRelationshipProperties has all 5 keys. Yet the
    // rel-delete trigger produced ZERO _CDCDeleteEvent rows across 228
    // node-delete transactions — every one of which cascaded at least one
    // :RELATED_TO edge on the primary. The only code path difference vs the
    // working ping trigger is the `apoc.map.fromPairs([... WHERE e.relationship = dr ...])`
    // reconstruction. APOC 5.x (5.26 exactly) silently errors on that
    // expression at runtime — either the inner-map field is named differently
    // from what the docs claim, or `e.relationship = dr` does not evaluate
    // truthy for relationship proxies that are about to be deleted. Either
    // way the error is swallowed at TRACE/DEBUG level, the trigger's CREATE
    // never executes, and rel deletes are silently dropped from CDC.
    //
    // The fix is to stop trying to reconstruct props from the trigger-context
    // maps and instead call `properties(dr)` / `properties(dn)` directly on
    // the entity. In `phase:'before'` the entity is not yet physically
    // deleted — its properties are still readable via the tx snapshot.
    //
    // CORRECTION (BUG-066, 2026-04-21): the above paragraph originally also
    // claimed `labels(dn)` / `type(dr)` work in `phase:'before'`. That is
    // FALSE — both go through `TransactionBoundReadQueryContext`'s
    // token-store existence check and throw `EntityNotFoundException` for
    // any proxy already flagged "deleted in this tx". Only `properties()`
    // and `elementId()` are safe. BUG-066 removes both of those calls from
    // the delete triggers and reads labels/type from pre-stamped
    // properties (`_labels`, `_type`) instead.
    //
    // This also sidesteps a second subtle bug:
    // the fromPairs path DROPPED properties that existed on the entity but
    // were NOT also in `$removedNodeProperties` / `$removedRelationshipProperties`
    // (APOC 5.x only populates those maps for properties that were
    // explicitly SET/REMOVE'd in the same tx, not for all the entity's
    // pre-existing props), whereas `properties(entity)` always returns the
    // full current map. For the sync-applier's MATCH-by-_elementId contract
    // to hold after a cluster-wide replay this is the correct behaviour.
    //
    // Filter `_CDCDeleteEvent` out BEFORE the CREATE so the trigger does not
    // fire recursively on its own sentinel writes.
    //
    // BUG-065/BUG-064 historical misconception (now corrected by BUG-066):
    // both earlier fixes assumed `labels(dn)` was safe on deleted-node
    // proxies inside `phase:'before'`. The symmetric assumption for
    // `properties(dn)` is correct (Neo4j reads the snapshot property
    // table), but `labels(dn)` goes through
    // `TransactionBoundReadQueryContext.getLabelTokenSetForNode`, which
    // does an explicit per-tx existence check and throws
    // `EntityNotFoundException: Node with id N has been deleted in this
    // transaction`. Empirically confirmed from the Neo4j primary's own
    // log on 2026-04-21 — BUG-065 did not fix the timeout.
    //
    // BUG-066 (2026-04-21) — correct fix: never call `labels(dn)` here.
    // Use the `_labels` JSON string that `cdc-timestamp-created` now
    // stamps on every newly-created node. `properties(dn)` remains safe
    // and gives us the full current property map (including `_labels`,
    // `_elementId`, etc.); `elementId(dn)` is also safe because it only
    // encodes `long id -> elementId string` without touching the node's
    // token or property store (see ElementIdMapper.nodeElementId). The
    // filter uses a JSON-string `CONTAINS` check instead of label
    // membership. Marker property `_is_cdc_event` is added so we can
    // filter _CDCDeleteEvent-chain recursion without needing label
    // access on the newly-created sentinel either.
    //
    // Escaping note (BUG-066 follow-up 2026-04-21): the CONTAINS target
    // is the plain substring `_TriggerReadinessProbe` WITHOUT wrapping
    // double quotes. Wrapping it with `\"..\"` would require a four-deep
    // escape chain (Java text block → Java String → outer Cypher body
    // string → inner trigger body), which in our first attempt collapsed
    // to `""_TriggerReadinessProbe""` at the inner layer and crashed
    // every single write tx on the DB with a SyntaxException. Since
    // `_TriggerReadinessProbe` is a reserved internal label (leading
    // underscore), the looser substring match is semantically safe —
    // any JSON encoding of a labels list that contains the literal
    // label name will match.
    //
    // BUG-066 follow-up #5 (2026-04-21, sixth iteration): both the
    // `_is_cdc_event` marker (#4) and the `eventType IS NULL` marker
    // (#4 second attempt) failed to exclude newly-created `_CDCDeleteEvent`
    // sentinels in the cleanup recursion. Empirical evidence: ha-agent
    // logged a steady-state "Cleaned up 4 _CDCDeleteEvent transit nodes"
    // at ~9.5/sec for 50+ seconds straight after both fixes were
    // deployed.
    //
    // Root cause (final): `properties(dn)` in Neo4j 5.26 behaves
    // ASYMMETRICALLY between two delete patterns:
    //   - "CREATE-then-DELETE in same tx" (e.g., `CREATE (n:_HealthCheck) DELETE n`):
    //     `properties(dn)` returns the full property snapshot, including
    //     fields stamped by `cdc-timestamp-created` in the same tx.
    //   - "delete-of-previously-committed-node" (e.g., cleanup's
    //     `MATCH (e:_CDCDeleteEvent) DETACH DELETE e`): `properties(dn)`
    //     returns either an empty map or a map missing the
    //     `eventType` / `_is_cdc_event` keys — empirically, NEITHER
    //     filter expression of those types excluded the cleanup's
    //     own deleted `_CDCDeleteEvent` payloads.
    //
    // Workaround: stop relying on `properties(dn)` for the recursion
    // filter. Use APOC's `$removedLabels` trigger-context map, which
    // is `Map<String, List<Node>>` of labels that were removed in the
    // tx (deleted nodes count as "all their labels removed"). This is
    // a Java-level map populated by APOC at trigger-context build time;
    // its `.relationship`-style fields are kernel proxies but reading
    // the LIST itself + computing `elementId(node)` on each contained
    // proxy is safe (elementId is pure ElementIdMapper encoding, no
    // cursor seek). We cross-reference each deleted node's elementId
    // against the lists keyed by `_CDCDeleteEvent` and
    // `_TriggerReadinessProbe` — if found, skip the CREATE.
    //
    // `properties(dn)` is still used for the CREATE clause to populate
    // `labels` / `properties` JSON fields. Even when `properties(dn)`
    // returns a partial map, that partial data is what we publish to
    // CDC consumers — the published _CDCDeleteEvent's `properties`
    // field will be `{}` for cleanup-recursion victims (empty payload
    // since properties(dn) returned empty), but the `elementId` /
    // `eventType` are still present, which is enough for sync-applier
    // to perform the DETACH DELETE on standby (it matches by
    // elementId only). And anyway, with the new $removedLabels filter
    // in place, `_CDCDeleteEvent` deletions never enter the CREATE
    // branch, so we don't actually emit empty-payload events.
    //
    // BUG-066 follow-up #6 (2026-04-21, seventh iteration): integrity test
    // showed delete_leak=90/126/124 across 3 nodes — i.e. ~10% of all
    // deletes silently fail to replicate to standby because the applier's
    // `MATCH (n:Label {_elementId: $eid}) DETACH DELETE n` cannot find the
    // node by what we published.
    //
    // Root cause: as documented in follow-up #5, `properties(dn)` returns
    // an INCOMPLETE map for tx-deleted "previously-committed" nodes. The
    // recursion-guard (using `$removedLabels`) was unaffected, but the
    // CREATE clause STILL relied on `dnProps["_elementId"]` and
    // `dnProps["_labels"]` for the DELETE event's payload. When those
    // returned NULL, `coalesce(..., dnEid)` fell back to the LOCAL
    // elementId of the deleting primary — which is NOT what standbys'
    // `_elementId` property holds. (Standbys' `_elementId` was copied
    // verbatim from the ORIGINAL primary that created the node, possibly
    // many switchovers ago.) Result: applier's MATCH-by-_elementId on
    // standby never finds the node → DETACH DELETE no-ops → leak.
    //
    // follow-up #5's earlier remark "elementId(dn) is enough for the
    // applier" was wrong. The applier matches by `_elementId` PROPERTY,
    // not by `elementId()` function. Two different things, identical only
    // for nodes created ON the current primary.
    //
    // Fix: same pattern as REL_DELETE_TRIGGER (follow-up #3) — use APOC's
    // `$removedNodeProperties` trigger context map. For each deleted
    // node, all its properties enter `$removedNodeProperties[propName]`
    // as `{node, key, old}` entries (proven safe for `DETACH DELETE` by
    // BUG-064's ping-trigger evidence). We extract `_elementId` and
    // `_labels` via `head([... WHERE elementId(e.node) = dnEid | e.old])`
    // — a JOIN keyed by elementId-encoded strings, never touching node
    // proxy properties or labels. Filter via `$removedLabels` is still
    // safe (proven in follow-up #5).
    //
    // The `properties` field of `_CDCDeleteEvent` is set to `"{}"` (same
    // as REL_DELETE) — applier doesn't consume it on delete events;
    // matching by `_elementId` property + label scope is sufficient.
    //
    // Edge case: nodes that pre-date BUG-063's `cdc-timestamp-created`
    // (legacy nodes that never had `_elementId` stamped) will have
    // `dnEidProp = null` and fall back to `dnEid` (local elementId).
    // For those, applier's MATCH may still fail on standbys whose copy
    // was created via fullsync rather than incremental replication.
    // The current cluster (`--clean-before-run`) creates everything via
    // the trigger so this isn't an issue in test; for upgrade scenarios
    // a one-time `MATCH (n) WHERE n._elementId IS NULL SET n._elementId
    // = elementId(n)` migration would close the gap.
    private static final String NODE_DELETE_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-capture-node-deletes',
          'UNWIND $deletedNodes AS dn
           WITH dn, elementId(dn) AS dnEid
           WITH dn, dnEid,
                size([x IN coalesce($removedLabels["_CDCDeleteEvent"], [])
                      WHERE elementId(x) = dnEid]) AS isCdcEvt,
                size([x IN coalesce($removedLabels["_TriggerReadinessProbe"], [])
                      WHERE elementId(x) = dnEid]) AS isProbe
           WHERE isCdcEvt = 0 AND isProbe = 0
           WITH dnEid,
                head([e IN coalesce($removedNodeProperties["_elementId"], [])
                      WHERE elementId(e.node) = dnEid | e.old]) AS dnEidProp,
                head([e IN coalesce($removedNodeProperties["_labels"], [])
                      WHERE elementId(e.node) = dnEid | e.old]) AS dnLabelsProp
           CREATE (:_CDCDeleteEvent {
             eventType: "NODE_DELETED",
             elementId: coalesce(dnEidProp, dnEid),
             labels:    coalesce(dnLabelsProp, "[]"),
             properties: "{}",
             timestamp: timestamp()
           })',
          {phase: 'before'})
        """;

    // BUG-066 follow-up #3 (2026-04-21, fourth iteration):
    // ALL proxy property access on deleted rels throws — not just
    // `properties(dr)` (full asMap), but also `dr._type` (single-property
    // point read). Neo4j 5.26 stack for the single-property path:
    //
    //   at CursorUtils$VirtualRelationshipReader.next(CursorUtils.java:828)
    //   at CursorUtils$VirtualRelationshipReader.property(CursorUtils.java:839)
    //   at CursorUtils.relationshipGetProperty(CursorUtils.java:521)
    //   at TransactionBoundReadQueryContext$RelationshipReadOperations.getProperty(...:1443)
    //   at Property.apply(Property.scala:68)
    //   -> EntityNotFoundException: Relationship with id N has been deleted
    //
    // Every rel-property access (asMap / getProperty / getType) funnels
    // through `VirtualRelationshipReader.next()`, which does the same
    // up-front existence check that throws on tx-deleted rels. There is
    // NO safe way to read a property value or type directly off a
    // deleted rel proxy in Neo4j 5.26 Community.
    //
    // Safe operations on a deleted rel proxy (verified from the same
    // source audit):
    //   - `elementId(dr)`  — ElementIdMapper.relationshipElementId(long id)
    //     is pure encoding; no Read API call, no cursor, no existence
    //     check. SAFE.
    //   - Map-value access into `$removedRelationshipProperties` —
    //     this is a trigger-context `Map<String, List<Map>>`; reading
    //     `.old` / `.key` fields of inner entries does not touch the
    //     rel proxy at all. Reading `.relationship` yields the proxy
    //     but merely referencing it (for `elementId(...)`) is safe
    //     since `elementId` itself is safe.
    //
    // BUG-064 doc once claimed `$removedRelationshipProperties` is
    // empty for pure `DETACH DELETE` scenarios (only tracks explicit
    // SET / REMOVE in the tx). That claim is CONTRADICTED by its own
    // ping-trigger hard evidence (`removedRelPropKeys=5` for both
    // `DELETE r` and `DETACH DELETE n` cascades). The ping-trigger
    // evidence is authoritative — APOC 5.x DOES populate this map on
    // rel deletion with one entry per property per deleted rel.
    //
    // Fix strategy:
    //   1. UNWIND `$removedRelationshipProperties["_type"]` to iterate
    //      deleted rels along with their `_type` old-value, avoiding
    //      ever calling `dr._type` or `type(dr)` on the proxy.
    //   2. Use `elementId(entry.relationship)` as the JOIN KEY to cross-
    //      reference the matching entry in `["_elementId"]` for the
    //      cluster-stable `_elementId` property — this comparison
    //      ("elementId-of-proxy = elementId-of-proxy") reduces to pure
    //      string equality after encoding and avoids the BUG-064 trap
    //      of comparing proxies directly (`e.relationship = dr`).
    //   3. Probe filter uses `drType <> "_PROBE_REL"` against the map
    //      `.old` value; the probe rel was stamped with `_type` by
    //      `cdc-rel-timestamp`'s afterAsync stamp before the rel-probe
    //      succeeded (probe waits up to REL_PROBE_TIMEOUT_MS for
    //      `_updated_at`, and `_type` is stamped in the same SET block).
    //   4. Drop the `properties` field (fixed to `"{}"`) — applier
    //      doesn't consume it for delete events.
    //
    // Edge case: rels created AND deleted inside the ~1s window between
    // commit and `cdc-rel-timestamp` afterAsync stamping have no `_type`
    // entry in `$removedRelationshipProperties` (because APOC's removed-
    // props map only records properties that were ACTUALLY on the
    // entity at delete time). Those rels produce 0 REL_DELETED events —
    // a slightly different failure shape than BUG-061's naked-create
    // gap, but on the same axis. NakedRelationshipHealer (BUG-062) will
    // stamp `_type` retroactively for in-flight rels, closing this gap
    // in a steady-state measurement.
    //
    // Cross-key JOIN performance note: the list comprehension runs
    // O(n_type * n_eid) where n_* is number of deleted rels in the tx.
    // `_type` and `_elementId` lists have identical length (one entry
    // per deleted rel). For typical transactions n=1..100, this is
    // negligible.
    // (BUG-066 follow-up #4: `_is_cdc_event: true` marker dropped — see
    // NODE_DELETE_TRIGGER note. Rel trigger's CREATE still sets `eventType:
    // "REL_DELETED"`, which serves as the recursion-guard sentinel from
    // NODE_DELETE_TRIGGER's side should any `_CDCDeleteEvent` ever be
    // deleted.)
    // BUG-082 (2026-04-22): additionally stamp the deleted rel's start/end
    // node `_elementId`s onto the `_CDCDeleteEvent` transit node. The
    // sync-applier REL_DELETE cypher uses them to scope the delete to the
    // specific (start, end, _elementId) triple instead of matching any rel
    // with that `_elementId` — which, under Neo4j 5.x `_elementId` recycling,
    // can silently delete a completely unrelated later rel that happens to
    // inherit the recycled id. See CypherTemplates.REL_DELETE_SCOPED.
    //
    // 'before' phase: `dr` is still a live reference inside the commit-
    // pending tx, and startNode/endNode of `dr` are still resolvable (even
    // in DETACH DELETE where the endpoint nodes themselves are being
    // deleted — they're not yet committed). We prefer the endpoint's
    // `_elementId` property (stable app-level id) and only fall back to
    // `elementId()` (internal id) when the property is absent.
    private static final String REL_DELETE_TRIGGER = """
        CALL apoc.trigger.install($db, 'cdc-capture-rel-deletes',
          'UNWIND coalesce($removedRelationshipProperties["_type"], []) AS typeEntry
           WITH typeEntry.relationship AS dr,
                typeEntry.old          AS drType
           WHERE coalesce(drType, "") <> "_PROBE_REL"
           WITH dr, drType, elementId(dr) AS drLocalEid,
                startNode(dr) AS sn, endNode(dr) AS en
           WITH drType, drLocalEid,
                coalesce(sn._elementId, elementId(sn)) AS drStartEid,
                coalesce(en._elementId, elementId(en)) AS drEndEid,
                head([e IN coalesce($removedRelationshipProperties["_elementId"], [])
                      WHERE elementId(e.relationship) = drLocalEid | e.old]) AS drEidProp
           CREATE (:_CDCDeleteEvent {
             eventType: "REL_DELETED",
             elementId: coalesce(drEidProp, drLocalEid),
             relType:   drType,
             startElementId: drStartEid,
             endElementId:   drEndEid,
             properties: "{}",
             timestamp: timestamp()
           })',
          {phase: 'before'})
        """;

    /**
     * Install triggers with retry. In Neo4j 5.x+ the system database uses Raft
     * consensus even in standalone mode; apoc.trigger.install writes to the system
     * database internally and can fail with "role is FOLLOWER" during the brief
     * leader-election window right after startup.
     *
     * <p>BUG-050: after all three trigger install calls return, probe the target
     * database to confirm the trigger is actually armed. APOC 5.x's install is
     * acknowledged as soon as the system-database entry is written, but the
     * trigger does not fire on the target database until the internal cache has
     * been refreshed. Without the probe, the first ~100-400 ms of client writes
     * after switchover land in the target DB without the trigger running, so
     * {@code _updated_at} / {@code _elementId} are never set and those nodes
     * become invisible to CDC's keyset polling.</p>
     */
    public void ensureInstalled(Driver driver, String database) {
        // BUG-063: three independent node-stamping triggers replace the
        // previous single 'cdc-timestamp' trigger. Also drop the legacy
        // name if still present from a pre-fix install (best-effort; the
        // drop inside installWithRetry only cleans the new names).
        dropLegacyTrigger(driver, database, "cdc-timestamp");
        installWithRetry(driver, database, "cdc-timestamp-created",  TIMESTAMP_CREATED_TRIGGER);
        installWithRetry(driver, database, "cdc-timestamp-assigned", TIMESTAMP_ASSIGNED_TRIGGER);
        installWithRetry(driver, database, "cdc-timestamp-removed",  TIMESTAMP_REMOVED_TRIGGER);
        installWithRetry(driver, database, "cdc-rel-timestamp", REL_TIMESTAMP_TRIGGER);
        installWithRetry(driver, database, "cdc-capture-node-deletes", NODE_DELETE_TRIGGER);
        installWithRetry(driver, database, "cdc-capture-rel-deletes", REL_DELETE_TRIGGER);
        waitForTriggerArmed(driver, database);
        // BUG-058: cdc-rel-timestamp runs in phase:'afterAsync'. APOC installs
        // async triggers through a periodic refresh; `apoc.trigger.install`
        // returning does not mean the afterAsync hook is wired into commit
        // pipeline yet. Without this additional probe, the first rel MERGE'd
        // after unblockWrites may commit BEFORE the hook is armed — that rel
        // ends up with NULL _elementId / _updated_at / _created_at, becomes
        // invisible to CDC keyset polling, and turns into an orphan on the
        // local primary (never publishes to Stream, never reaches other
        // standbys).
        waitForRelTriggerArmed(driver, database);
        log.info("All 6 APOC triggers ensured AND armed on database '{}' " +
            "(node created/assigned/removed + rel timestamp + node/rel delete)", database);
    }

    /**
     * Best-effort drop of a legacy trigger name no longer installed by this code.
     * Used during BUG-063 upgrade: the old single 'cdc-timestamp' trigger is
     * replaced by three separate triggers, but its definition may still live
     * in the system DB from a pre-upgrade install. Leaving it around would
     * mean its buggy body runs alongside the new triggers.
     */
    private void dropLegacyTrigger(Driver driver, String database, String name) {
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run(
                "CALL apoc.trigger.drop($db, $name) YIELD name RETURN name",
                Map.of("db", database, "name", name)
            ).consume();
            log.info("Legacy APOC trigger '{}' dropped (BUG-063 upgrade cleanup)", name);
        } catch (Exception e) {
            log.debug("Legacy APOC trigger '{}' not present or drop failed: {}", name, e.getMessage());
        }
    }

    /**
     * Busy-wait until the cdc-timestamp trigger is demonstrably armed on the target
     * database: create a sentinel node, read back its {@code _updated_at}, delete it.
     * If the property was set, triggers are live. Retries every 50 ms up to 10 s.
     *
     * <p>The sentinel lives outside the user label space (uses a dedicated internal
     * label {@code _TriggerReadinessProbe}) so it never pollutes CDC keyset results
     * — the CDC query filters to user labels from {@code db.labels()}, which would
     * skip any label starting with {@code _}. The sentinel is deleted in the same
     * session so it never appears in the normal data path.</p>
     */
    private static final String PROBE_LABEL = "_TriggerReadinessProbe";
    private static final long PROBE_TIMEOUT_MS = 10_000;
    // BUG-060: rel trigger runs in `phase:'afterAsync'`. It can only fire
    // AFTER APOC's refresh thread (default apoc.trigger.refresh=1000ms)
    // has reloaded the trigger definition from system DB into the user DB,
    // PLUS APOC's async executor has warmed up post-promote. That easily
    // takes several seconds on a freshly promoted primary. 10s is too
    // tight; give rel probe 30s.
    private static final long REL_PROBE_TIMEOUT_MS = 30_000;
    private static final long PROBE_INTERVAL_MS = 50;

    /**
     * Poll the target DB until the cdc-timestamp trigger is demonstrably armed.
     *
     * <p>The trigger's {@code phase:'before'} hook runs <b>inside the transaction</b>
     * but its mutations (SET _updated_at) are only visible <b>after</b> the statement
     * that created the node has returned — i.e. not within the same {@code RETURN}.
     * So the probe uses two transactions:</p>
     * <ol>
     *   <li>Tx 1: {@code CREATE} the sentinel, return its elementId.</li>
     *   <li>Tx 2: {@code MATCH} by elementId, return {@code _updated_at}.</li>
     *   <li>Tx 3 (always, even on probe failure): {@code MATCH DETACH DELETE} to sweep.</li>
     * </ol>
     *
     * <p>If Tx 2 sees {@code _updated_at != null} the trigger is armed. Otherwise
     * sleep {@link #PROBE_INTERVAL_MS} and retry, up to {@link #PROBE_TIMEOUT_MS}.</p>
     */
    private void waitForTriggerArmed(Driver driver, String database) {
        long deadline = System.currentTimeMillis() + PROBE_TIMEOUT_MS;
        int attempts = 0;
        String lastEid = null;
        // BUG-065: retain the most recent probe-iteration exception so the
        // eventual timeout error carries the root cause. Without this, a
        // trigger-body bug that crashes Tx1/Tx3 on every iteration would
        // hide behind a generic "did not become armed in 10s" and require
        // reproduction with debug logging to diagnose.
        Exception lastProbeException = null;
        try {
            while (System.currentTimeMillis() < deadline) {
                attempts++;
                try {
                    // Tx 1: create sentinel
                    try (Session s1 = driver.session(SessionConfig.forDatabase(database))) {
                        var r = s1.run(
                            "CREATE (n:" + PROBE_LABEL + " {probeAt: $ts}) RETURN elementId(n) AS eid",
                            Map.of("ts", System.currentTimeMillis())
                        ).single();
                        lastEid = r.get("eid").asString();
                    }
                    // Tx 2: read back — this sees trigger mutations from Tx 1's commit
                    Object ts;
                    try (Session s2 = driver.session(SessionConfig.forDatabase(database))) {
                        var r = s2.run(
                            "MATCH (n) WHERE elementId(n) = $eid RETURN n._updated_at AS ts",
                            Map.of("eid", lastEid)
                        ).single();
                        ts = r.get("ts").asObject();
                    }
                    // Tx 3: sweep this attempt's sentinel
                    try (Session s3 = driver.session(SessionConfig.forDatabase(database))) {
                        s3.run("MATCH (n) WHERE elementId(n) = $eid DETACH DELETE n",
                            Map.of("eid", lastEid)).consume();
                    }
                    lastEid = null;

                    if (ts != null) {
                        log.info("APOC trigger readiness probe succeeded on database '{}' (attempt {})",
                            database, attempts);
                        return;
                    }
                } catch (Exception e) {
                    lastProbeException = e;
                    log.debug("Trigger readiness probe transient failure (attempt {}): {}",
                        attempts, e.toString());
                }
                try { Thread.sleep(PROBE_INTERVAL_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for trigger readiness", ie);
                }
            }
            String rootCause = lastProbeException != null
                ? " Last probe exception: " + lastProbeException
                : " All probes returned null _updated_at (trigger likely not firing on target DB).";
            throw new RuntimeException("APOC trigger on database '" + database
                + "' did not become armed within " + PROBE_TIMEOUT_MS + "ms (tried "
                + attempts + " probes). Switchover would create orphan writes; aborting."
                + rootCause, lastProbeException);
        } finally {
            // Sweep everything: any straggler from a failed attempt + any
            // `_CDCDeleteEvent` transit nodes the delete trigger spawned.
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                session.run("MATCH (n:" + PROBE_LABEL + ") DETACH DELETE n").consume();
                session.run("MATCH (n:_CDCDeleteEvent) WHERE n.labels CONTAINS '"
                    + PROBE_LABEL + "' DETACH DELETE n").consume();
            } catch (Exception sweep) {
                log.warn("Failed best-effort sweep of trigger-probe sentinels: {}",
                    sweep.toString());
            }
        }
    }

    /**
     * BUG-058: Busy-wait until the cdc-rel-timestamp trigger is demonstrably
     * armed. Unlike {@link #waitForTriggerArmed}, which probes a
     * {@code phase:'before'} trigger that is guaranteed armed the moment
     * {@code apoc.trigger.install} returns, this probe targets an
     * {@code afterAsync} trigger.
     *
     * <p>APOC wires afterAsync hooks into the commit pipeline through its
     * periodic trigger-refresh job; installation ack only means the system-DB
     * entry is written, not that each target DB's kernel has constructed the
     * async executor for it. In the window before it hooks up, newly-created
     * relationships commit WITHOUT being stamped, which makes them permanently
     * invisible to CDC keyset polling.</p>
     *
     * <p>Three-tx probe:</p>
     * <ol>
     *   <li>Tx 1: {@code CREATE (a)-[r:_PROBE_REL]->(b)} return {@code elementId(r)}.
     *       afterAsync is not observable in Tx 1 because it runs in a separate
     *       post-commit transaction.</li>
     *   <li>Tx 2 (retry): {@code MATCH ()-[r]->() WHERE elementId(r)=$eid RETURN r._updated_at}.
     *       First few reads may return null because afterAsync hasn't run yet;
     *       keep polling.</li>
     *   <li>Tx 3 (always): {@code DETACH DELETE} the two sentinel nodes + any
     *       {@code _CDCDeleteEvent} nodes spawned by the rel-delete trigger.</li>
     * </ol>
     *
     * <p>Sentinel label is {@link #PROBE_LABEL} (same as node probe) and rel
     * type is {@link #PROBE_REL_TYPE}, both starting with {@code _} so
     * CDC's {@code db.labels()} / {@code db.relationshipTypes()} whitelists
     * filter them out.</p>
     */
    private static final String PROBE_REL_TYPE = "_PROBE_REL";

    private void waitForRelTriggerArmed(Driver driver, String database) {
        // BUG-060: use extended timeout for rel probe. See REL_PROBE_TIMEOUT_MS.
        long deadline = System.currentTimeMillis() + REL_PROBE_TIMEOUT_MS;
        int attempts = 0;
        String lastEid = null;
        try {
            // BUG-060: force APOC trigger-refresh on the user DB BEFORE we
            // create the sentinel rel. Calling apoc.trigger.list() reads
            // the per-DB trigger registry; in APOC 5.x that path also
            // pulls any pending updates from system DB. Without this, the
            // sentinel rel CREATE may commit during the <=1s window where
            // `cdc-rel-timestamp` is registered in system DB but not yet
            // hooked into this DB's kernel, and the afterAsync task for
            // that rel is silently dropped — which is exactly what
            // happened in the BUG-059 switchover run (all 186 probes
            // failed because the rel was created before the trigger was
            // actually armed).
            try (Session s0 = driver.session(SessionConfig.forDatabase(database))) {
                s0.run("CALL apoc.trigger.list() YIELD name RETURN count(name)").consume();
            } catch (Exception e) {
                log.debug("apoc.trigger.list() warmup call failed (non-fatal): {}", e.toString());
            }
            // Small grace period for APOC's trigger-refresh thread
            // (apoc.trigger.refresh, default 1000ms) to pick up the
            // newly-installed trigger. This is the "right amount of
            // patience", not a workaround: polling faster than the refresh
            // interval can't help — the trigger literally isn't hooked
            // yet.
            try { Thread.sleep(1200); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for APOC trigger refresh", ie);
            }

            // Tx 1: create the sentinel rel once; we'll keep polling its
            // _updated_at across attempts. (Re-creating per attempt is safe
            // but wasteful; a single rel that never gets stamped is enough
            // evidence the trigger isn't armed.)
            try (Session s1 = driver.session(SessionConfig.forDatabase(database))) {
                var r = s1.run(
                    "CREATE (a:" + PROBE_LABEL + " {role:'src', probeAt:$ts})"
                    + "-[r:" + PROBE_REL_TYPE + " {probeAt:$ts}]->"
                    + "(b:" + PROBE_LABEL + " {role:'dst', probeAt:$ts}) "
                    + "RETURN elementId(r) AS eid",
                    Map.of("ts", System.currentTimeMillis())
                ).single();
                lastEid = r.get("eid").asString();
            }

            // BUG-065: carry rel-probe's last exception into the timeout
            // message (same rationale as waitForTriggerArmed).
            Exception lastProbeException = null;
            while (System.currentTimeMillis() < deadline) {
                attempts++;
                try {
                    Object ts;
                    try (Session s2 = driver.session(SessionConfig.forDatabase(database))) {
                        var r = s2.run(
                            "MATCH ()-[r]->() WHERE elementId(r) = $eid RETURN r._updated_at AS ts",
                            Map.of("eid", lastEid)
                        ).single();
                        ts = r.get("ts").asObject();
                    }
                    if (ts != null) {
                        log.info("APOC rel-trigger readiness probe succeeded on database '{}' (attempt {})",
                            database, attempts);
                        return;
                    }
                } catch (Exception e) {
                    lastProbeException = e;
                    log.debug("Rel-trigger readiness probe transient failure (attempt {}): {}",
                        attempts, e.toString());
                }
                try { Thread.sleep(PROBE_INTERVAL_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for rel-trigger readiness", ie);
                }
            }
            String rootCause = lastProbeException != null
                ? " Last probe exception: " + lastProbeException
                : " All probes returned null _updated_at (afterAsync queue not firing on target DB).";
            throw new RuntimeException("APOC rel-trigger (cdc-rel-timestamp) on database '" + database
                + "' did not become armed within " + REL_PROBE_TIMEOUT_MS + "ms (tried "
                + attempts + " probes). Switchover would create orphan unstamped relationships; aborting."
                + rootCause, lastProbeException);
        } finally {
            // Sweep: the sentinel rel, its two endpoints, and any
            // `_CDCDeleteEvent` nodes the rel-delete trigger spawns when we
            // DETACH DELETE the sentinels.
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                session.run("MATCH (n:" + PROBE_LABEL + ") DETACH DELETE n").consume();
                session.run("MATCH (n:_CDCDeleteEvent) WHERE n.relType = '"
                    + PROBE_REL_TYPE + "' OR (n.labels IS NOT NULL AND n.labels CONTAINS '"
                    + PROBE_LABEL + "') DETACH DELETE n").consume();
            } catch (Exception sweep) {
                log.warn("Failed best-effort sweep of rel-trigger-probe sentinels: {}",
                    sweep.toString());
            }
        }
    }

    /**
     * BUG-061: Drain the APOC {@code afterAsync} queue for the rel-timestamp
     * trigger before uninstalling it (or before stopping CDC during
     * switchover).
     *
     * <p><b>Why this is needed.</b> `cdc-rel-timestamp` runs in
     * {@code phase:'afterAsync'}: APOC enqueues a stamp task at tx commit
     * and flushes it later in a dedicated async executor. If we
     * {@code apoc.trigger.drop} before that queue is flushed, every pending
     * task for the dropped trigger is silently discarded — the
     * corresponding relationships end up with
     * {@code _elementId IS NULL AND _updated_at IS NULL} on the node that
     * was primary at commit time. Those "naked" rels are then invisible to
     * keyset-paginated CDC polling
     * ({@code WHERE r._updated_at > $lastTs}), so the new primary never
     * replicates them; worse, standbys may have independently produced
     * their own naked rels in prior switchover cycles, causing divergence.
     *
     * <p><b>The probe.</b> Insert a sentinel rel of type
     * {@link #PROBE_REL_TYPE} (matches the readiness probe so CDC's
     * user-label/type whitelist filters it out). Keep polling
     * {@code r._updated_at} until it becomes non-null — that proves the
     * afterAsync executor has processed every task enqueued at-or-before
     * the sentinel's commit, including every business rel committed before
     * the caller's "block writes" cut. Once the probe succeeds we can
     * safely drop the trigger without losing stamps.
     *
     * <p><b>Ordering vs CdcCollector.stop().</b> This drain MUST run AFTER
     * {@code InflightTxDrainWaiter} (business txs are already committed
     * and no new rel writes can commit) and BEFORE {@code CdcCollector.stop}
     * (so the final CDC poll sees every stamped rel in the keyset scan).
     * Calling it again inside {@link com.neo4j.ha.agent.recovery.ApocTriggerUninstaller}
     * is cheap — by then the queue is already empty and the probe succeeds
     * on the first attempt.
     *
     * @return true if the queue drained within the timeout; false on
     *         timeout (the caller should log, mark for deferred cleanup,
     *         and continue — draining is best-effort, not a hard failure).
     */
    public static boolean drainRelTriggerAfterAsync(Driver driver, String database) {
        long deadline = System.currentTimeMillis() + REL_PROBE_TIMEOUT_MS;
        int attempts = 0;
        String lastEid = null;
        try {
            try (Session s1 = driver.session(SessionConfig.forDatabase(database))) {
                var r = s1.run(
                    "CREATE (a:" + PROBE_LABEL + " {role:'src', drainAt:$ts})"
                    + "-[r:" + PROBE_REL_TYPE + " {drainAt:$ts}]->"
                    + "(b:" + PROBE_LABEL + " {role:'dst', drainAt:$ts}) "
                    + "RETURN elementId(r) AS eid",
                    Map.of("ts", System.currentTimeMillis())
                ).single();
                lastEid = r.get("eid").asString();
            } catch (Exception e) {
                // Trigger may already be gone (uninstalled) or db is
                // unreachable. Either way, drain is impossible/unnecessary.
                log.warn("drainRelTriggerAfterAsync: failed to create sentinel rel on '{}' ({}). Skipping drain.",
                    database, e.toString());
                return false;
            }
            while (System.currentTimeMillis() < deadline) {
                attempts++;
                try {
                    Object ts;
                    try (Session s2 = driver.session(SessionConfig.forDatabase(database))) {
                        var r = s2.run(
                            "MATCH ()-[r]->() WHERE elementId(r) = $eid RETURN r._updated_at AS ts",
                            Map.of("eid", lastEid)
                        ).single();
                        ts = r.get("ts").asObject();
                    }
                    if (ts != null) {
                        log.info("drainRelTriggerAfterAsync: afterAsync queue drained on '{}' (attempt {})",
                            database, attempts);
                        return true;
                    }
                } catch (Exception e) {
                    log.debug("drainRelTriggerAfterAsync transient probe failure (attempt {}): {}",
                        attempts, e.toString());
                }
                try { Thread.sleep(PROBE_INTERVAL_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("drainRelTriggerAfterAsync interrupted on '{}' after {} attempts", database, attempts);
                    return false;
                }
            }
            log.warn("drainRelTriggerAfterAsync: afterAsync queue on '{}' did NOT drain within {}ms ({} probes). "
                + "Pending rel stamps may be dropped by subsequent trigger uninstall.",
                database, REL_PROBE_TIMEOUT_MS, attempts);
            return false;
        } finally {
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                session.run("MATCH (n:" + PROBE_LABEL + ") DETACH DELETE n").consume();
                session.run("MATCH (n:_CDCDeleteEvent) WHERE n.relType = '"
                    + PROBE_REL_TYPE + "' OR (n.labels IS NOT NULL AND n.labels CONTAINS '"
                    + PROBE_LABEL + "') DETACH DELETE n").consume();
            } catch (Exception sweep) {
                log.warn("drainRelTriggerAfterAsync: failed best-effort sweep of sentinels: {}",
                    sweep.toString());
            }
        }
    }

    /**
     * Install an APOC trigger only when not already present with the desired
     * body. Idempotent: on agent restart with an unchanged trigger, this is
     * a pure read from system DB (one {@code CALL apoc.trigger.list} per
     * trigger) with no writes.
     *
     * <p><b>BUG-063 lesson — why we must NOT unconditionally drop+install on
     * every startup:</b> Neo4j 2026.x Community Edition's {@code system}
     * database uses an internal Raft-like coordinator. In standalone
     * deployments (no real cluster), the single node bootstraps itself as
     * {@code LEADER} shortly after startup. Each {@code apoc.trigger.install}
     * / {@code drop} writes to {@code system} DB; doing that 6+ times in
     * quick succession on every agent restart (6 triggers × drop+install)
     * reliably pushes the coordinator back to {@code FOLLOWER} state in
     * Community Edition, at which point <b>every subsequent write to
     * system DB fails</b> with "No write operations are allowed directly
     * on this database ... role is: FOLLOWER", and the only recovery is a
     * full Neo4j container restart. This is a Neo4j-level limitation we
     * cannot fix; we can only reduce the write frequency.</p>
     *
     * <p><b>Algorithm:</b>
     * <ol>
     *   <li>{@code apoc.trigger.show($db) YIELD name, query WHERE name = $name} —
     *       if the trigger exists and its {@code query} body is byte-for-byte
     *       identical to what we'd install, return immediately (zero writes
     *       to system DB).</li>
     *   <li>If the body differs (or the trigger is absent), drop-if-present
     *       then install with the current body.</li>
     * </ol>
     *
     * <p>Net effect on a steady-state agent restart: <b>zero system DB
     * writes</b> for trigger maintenance. A code-level change to a trigger
     * body still takes effect on the next restart via the compare-and-write
     * path.</p>
     *
     * <p>The {@code installCypher} string contains the full
     * {@code CALL apoc.trigger.install($db, $name, $body, ...)} invocation.
     * We re-parse it here to extract the expected body for comparison.</p>
     */
    private void installWithRetry(Driver driver, String database, String name, String installCypher) {
        // Extract the expected body from the install cypher's second
        // positional argument (between the first and second single-quote).
        // If extraction fails we fall through to unconditional install
        // (safe, just not optimised).
        String expectedBody = extractTriggerBody(installCypher);

        // Check if the currently-installed trigger already matches expectedBody.
        // apoc.trigger.show is a READ on system DB (no leader-write required),
        // so this always works even if the coordinator is in FOLLOWER state —
        // which means an agent restart against a wedged system DB can still
        // short-circuit when nothing has changed.
        if (expectedBody != null && isTriggerAlreadyCurrent(driver, database, name, expectedBody)) {
            log.info("APOC trigger '{}' already current on database '{}' (skipping reinstall)",
                name, database);
            return;
        }

        // Need to write system DB. Drop any existing trigger with this name
        // first so install doesn't error out with "already installed".
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            session.run(
                "CALL apoc.trigger.drop($db, $name) YIELD name RETURN name",
                Map.of("db", database, "name", name)
            ).consume();
            log.info("APOC trigger '{}' dropped (body change detected) before reinstall", name);
        } catch (Exception e) {
            log.debug("APOC trigger '{}' drop returned: {}", name, e.getMessage());
        }

        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
                session.run(installCypher, Map.of("db", database)).consume();
                log.info("APOC trigger '{}' installed on attempt {}", name, attempt);
                return;
            } catch (Exception e) {
                lastException = e;
                if (e.getMessage() != null && e.getMessage().contains("already installed")) {
                    log.warn("APOC trigger '{}' still reports already-installed after drop; " +
                        "retrying drop+install (attempt {}/{})", name, attempt, MAX_RETRIES);
                    try (Session system = driver.session(SessionConfig.forDatabase("system"))) {
                        system.run(
                            "CALL apoc.trigger.drop($db, $name) YIELD name RETURN name",
                            Map.of("db", database, "name", name)
                        ).consume();
                    } catch (Exception drop) {
                        log.debug("Re-drop of '{}' failed: {}", name, drop.getMessage());
                    }
                    continue;
                }
                boolean isFollowerError = e.getMessage() != null
                    && e.getMessage().contains("FOLLOWER");
                if (isFollowerError && attempt < MAX_RETRIES) {
                    log.warn("APOC trigger '{}' install attempt {}/{} failed (system DB not leader; " +
                        "retrying in {}ms — BUG-063 Community Edition leader-flap)",
                        name, attempt, MAX_RETRIES, RETRY_DELAY_MS);
                    try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for trigger install retry", ie);
                    }
                } else {
                    log.error("Failed to install APOC trigger '{}': {}", name, e.getMessage());
                    throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
                }
            }
        }
        log.error("Failed to install APOC trigger '{}' after {} retries", name, MAX_RETRIES);
        throw new RuntimeException("Failed to install APOC trigger '" + name + "' after " + MAX_RETRIES + " retries", lastException);
    }

    /**
     * Return true if a trigger with {@code name} is currently installed on
     * {@code database} AND its stored body exactly equals {@code expectedBody}
     * (whitespace-normalised). On any read failure this returns false so the
     * caller falls through to a write-path install (fail-safe).
     */
    private boolean isTriggerAlreadyCurrent(Driver driver, String database, String name, String expectedBody) {
        try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
            var result = session.run(
                "CALL apoc.trigger.show($db) YIELD name AS tn, query AS q " +
                "WHERE tn = $name RETURN q AS body",
                Map.of("db", database, "name", name)
            );
            if (!result.hasNext()) return false;
            String installedBody = result.single().get("body").asString();
            String a = normaliseCypher(installedBody);
            String b = normaliseCypher(expectedBody);
            boolean match = a.equals(b);
            if (!match) {
                log.info("APOC trigger '{}' body differs from desired — will reinstall. " +
                    "Installed length={}, desired length={}", name, a.length(), b.length());
            }
            return match;
        } catch (Exception e) {
            log.debug("Could not read existing trigger '{}': {} (will fall through to install)",
                name, e.getMessage());
            return false;
        }
    }

    /**
     * Extract the trigger body (second positional argument of
     * {@code apoc.trigger.install}) from the install cypher string. The body
     * is always wrapped in single quotes in our templates:
     * <pre>
     *   CALL apoc.trigger.install($db, 'trigger-name', '....body....', {phase: ...})
     * </pre>
     * Returns the raw body string (without the wrapping quotes), or
     * {@code null} if parsing fails.
     */
    private static String extractTriggerBody(String installCypher) {
        // Find the first single quote after "install(" and track balanced
        // quotes. The body starts at the THIRD single quote (after $db
        // placeholder = first quote pair around trigger name, actually we
        // start after "install(" — first single quote is opening of name,
        // then closing, then opening of body, then closing of body).
        int start = installCypher.indexOf("install(");
        if (start < 0) return null;
        // Quote 1: open of name, Quote 2: close of name, Quote 3: open of body,
        // Quote 4: close of body. We want the substring between 3 and 4.
        int q = 0; int bodyStart = -1; int bodyEnd = -1;
        for (int i = start; i < installCypher.length(); i++) {
            char c = installCypher.charAt(i);
            if (c == '\'') {
                q++;
                if (q == 3) bodyStart = i + 1;
                else if (q == 4) { bodyEnd = i; break; }
            }
        }
        if (bodyStart < 0 || bodyEnd < 0 || bodyEnd <= bodyStart) return null;
        return installCypher.substring(bodyStart, bodyEnd);
    }

    /**
     * Normalise a cypher string for comparison: collapse runs of whitespace
     * to single spaces and trim. APOC's stored body and our Java-side
     * template often differ only in indentation / line endings, which
     * should not count as "body changed".
     */
    private static String normaliseCypher(String s) {
        if (s == null) return "";
        return s.replaceAll("\\s+", " ").trim();
    }
}
