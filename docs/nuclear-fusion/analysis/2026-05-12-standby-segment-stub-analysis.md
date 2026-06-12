# Project Analysis — Standby Segment Stub Drift

**Date**: 2026-05-12
**Analyst**: GPT-5.5
**Repo**: `/Users/fukai/Downloads/llm_project/git_target/neo4j-ha`
**Commit**: `b98927e` with working tree changes
**Coverage**: Focused analysis of HA CDC / SyncApplier paths, operations docs, and known BUG records. Did not perform a full project-wide review or run tests.

---

## 1. Executive Summary

The observed standby state is not a simple `_updated_at` timestamp drift. The seven affected `aiagent_Segment` nodes on standby have only `["_elementId"]` as keys, while primary has the same `_elementId` values with `_updated_at = 1777023842047`. That shape matches the BUG-079 endpoint-stub mechanism: `RelationshipApplier` can create a minimal endpoint node on standby so an out-of-order relationship event can be applied before the endpoint node event arrives.

The most likely root cause is that relationship events for those `aiagent_Segment` endpoints were applied first, creating label + `_elementId` stubs, but the later node event that should have executed `SET n = $properties` never arrived or was skipped. This points to an incomplete residual around the BUG-079 fix: it prevents relationship loss, but it does not detect or heal endpoint stubs when the promised later `NODE_CREATED` / `NODE_UPDATED` event is absent.

## 2. Relevant Architecture

| Component | Evidence | Role in this incident |
|---|---|---|
| APOC node stamping triggers | `src/ha-agent/src/main/java/com/neo4j/ha/agent/bootstrap/ApocTriggerInstaller.java:132-160` | Primary-side node CREATE / property SET should stamp `_elementId` and `_updated_at`. |
| Node CDC polling | `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/capture/NodeChangeCapture.java:46-53` | Nodes are captured only if `_updated_at` is greater than the node cursor. |
| Node CDC cursor update | `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java:368-385` | Node cursor advances from the last published node event after successful publish. |
| Node apply path | `src/sync-applier/src/main/java/com/neo4j/ha/sync/applier/CypherTemplates.java:9-13` | Later node events should hit the stub by `(label, _elementId)` and replace properties. |
| Relationship apply path | `src/sync-applier/src/main/java/com/neo4j/ha/sync/applier/CypherTemplates.java:101-110` | Relationship events MERGE endpoint nodes, creating stubs if endpoints are not present. |
| Relationship applier | `src/sync-applier/src/main/java/com/neo4j/ha/sync/applier/RelationshipApplier.java:75-116` | Uses endpoint labels when available, so stubs have the same label as real nodes. |

## 3. Incident Fit

Observed standby rows:

- label: `aiagent_Segment`
- keys: `["_elementId"]`
- `_updated_at`: `NULL`
- `_elementId`: same source-primary IDs as primary

This is exactly what `MERGE (a:%1$s {_elementId: $startNodeId})` / `MERGE (b:%2$s {_elementId: $endNodeId})` creates when a relationship event arrives first. Because the endpoint MERGE pattern only carries label and `_elementId`, the created node has no `_created_at`, no `_updated_at`, no `_labels`, and no business properties until the node event later applies.

Primary having `_updated_at` for the same `_elementId`s rules out a primary-side missing-trigger incident as the direct current state. The source of truth is intact; the standby copy is an incomplete stub.

## 4. Closest Historical Bugs

### BUG-079 — Closest Match, Likely Incomplete Edge

BUG-079 documents the exact reason endpoint stubs were introduced: relationship events can be sorted before endpoint `NODE_UPDATED` events, so the old `MATCH` endpoint logic dropped relationships. The fix intentionally changed endpoint lookup to MERGE and says the later `NODE_UPDATED` should fill the stub (`docs/nuclear-fusion/design/modules/ha-agent-design.md:6232-6324`).

The current incident is the negative case of that fix: the relationship was preserved, but the endpoint stub was never filled. Therefore BUG-079 fixed relationship loss, but did not add a guardrail for "stub remains after node event is lost/skipped/trimmed."

### BUG-050 — Similar Symptom, Different Shape

BUG-050 was about trigger installation being asynchronously armed after switchover; first writes on the new primary could have `_elementId = NULL` and `_updated_at = NULL`, making them invisible to CDC (`docs/nuclear-fusion/design/modules/ha-agent-design.md:2689-2734`). That does not match this incident directly, because primary has `_updated_at` and `_elementId`; only standby is incomplete.

### BUG-057 — Similar Loss Mechanism, Current Code Has a Fix

BUG-057 split node / relationship / delete cursors because a relationship cursor could otherwise cause same-millisecond node events to be skipped (`src/cdc-collector/src/main/java/com/neo4j/ha/cdc/polling/PollingState.java:6-18`, `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java:368-407`). If the running container is older than this code or not redeployed, this could explain lost node events after relationship events.

### BUG-038 / BUG-040 — Plausible Operational Cause

The design notes say trimmed Redis Stream / PEL payloads are acknowledged and require manual fullsync, not automatic repair (`docs/nuclear-fusion/design/modules/ha-agent-design.md:1864-1911`, `docs/nuclear-fusion/design/modules/ha-agent-design.md:1996-2017`). If the seven node events were trimmed or lost while relationship events had already been applied, the result would be exactly these stubs.

### BUG-063 — Same "Standby Missing Properties" Family, Not Same Root

BUG-063 covers client-side GDS `.write` bypassing APOC triggers, causing primary node changes not to refresh `_updated_at`, so standby never receives algorithm properties (`docs/nuclear-fusion/design/modules/ha-agent-design.md:3790-3908`). In this incident, primary has `_updated_at`, and standby has only `_elementId`; the signature is stub-not-filled, not GDS property drift.

## 5. Risk Assessment

### High

| ID | Issue | Evidence | Impact | Remediation Direction |
|---|---|---|---|---|
| H1 | Endpoint stubs can remain permanently if node events are skipped after BUG-079 creates them. | `src/sync-applier/src/main/java/com/neo4j/ha/sync/applier/CypherTemplates.java:101-110`, `docs/nuclear-fusion/design/modules/ha-agent-design.md:6324-6334` | Standby can pass count/label/relationship checks while silently missing node properties and CDC metadata. | Add a "stub node healer" or post-apply validation that finds non-internal nodes with `_elementId` present and `_updated_at IS NULL`, then repairs from primary or forces targeted fullsync. |
| H2 | Current operations checks detect missing `_updated_at`, but total count and label distribution can falsely look healthy. | `docs/nuclear-fusion/operations/ha-agent-cluster-operations.md:330-393` | Operators may treat standby as failover-ready after only count/distribution checks. | Promote the reserved-field invariant check to a hard failover gate; add a diagnostic subsection for `keys(n)=["_elementId"]` stub signatures. |

## 6. Suggested Immediate Verification

Run these from the Docker host against the standby:

```bash
docker exec "$STANDBY" cypher-shell \
  -a bolt://localhost:7687 \
  -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain \
  "MATCH (n)
   WHERE NOT n:_CDCDeleteEvent
     AND n._updated_at IS NULL
   RETURN labels(n) AS labels, keys(n) AS keys, count(*) AS cnt
   ORDER BY labels"
```

If all rows show only `["_elementId"]`, treat them as endpoint stubs. Then check their relationship degree:

```bash
docker exec "$STANDBY" cypher-shell \
  -a bolt://localhost:7687 \
  -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --format plain \
  "MATCH (n)
   WHERE n._elementId IN [$IDS]
   OPTIONAL MATCH (n)-[r]-()
   RETURN n._elementId AS eid, labels(n) AS labels, count(r) AS degree
   ORDER BY eid"
```

## 7. Suggested Next Steps

- [ ] Manual repair now: copy the full property map for the seven `_elementId`s from primary to standby, not only `_updated_at`, because standby currently has only `_elementId`.
- [ ] Handoff to `quick-coding`: add a focused diagnostic script/query for endpoint stubs (`_elementId IS NOT NULL AND _updated_at IS NULL`) and include degree / labels / keys output.
- [ ] Handoff to `building-production-feature`: design a safe stub-node healer or targeted reconciliation path, because blindly setting `_updated_at` masks missing business properties.
- [ ] Before any failover: require `missingUpdatedAt = 0` and no `keys(n)=["_elementId"]` business nodes on every standby.

---

**Scope honesty**: This report did not inspect live Redis streams or agent container logs. The conclusion is based on repository evidence and the provided live Neo4j query output.
