# Code Review Report — BUG-080 Two-Phase Reconcile (record-at-failover / execute-at-recovery)

**Date**: 2026-04-22
**Reviewer**: nuclear-fusion / reviewing-code
**Target**: commit `17592e2` "增加failover后的修复动作"
**Base SHA**: `db646a5`
**Head SHA**: `17592e2`

---

## 1. Scope

- **Files changed**: 7 (5 substantive — 1 config file `.claude/settings.local.json` ignored; 1 docs file reviewed separately)
- **Lines changed**: +337 / -126
- **Change type**: feature (re-architecture of BUG-080 reconciliation)
- **Stated intent**: Move post-failover reverse reconciliation off the graceful switchover critical path and onto the old-primary recovery path. Graceful switchover relies solely on the drain chain (Phase 2.5 / 2.6 / CDC stop) for zero-loss; crash failover records a `PendingReconcile` intent in Redis, consumed by `OldPrimaryRecovery.execute` Step 4.5 when the failed node comes back as a standby.

Files reviewed:
- `src/common/src/main/java/com/neo4j/ha/common/redis/CheckpointManager.java` (+80)
- `src/ha-agent/src/main/java/com/neo4j/ha/agent/failover/FailoverOrchestrator.java` (+35 / -32)
- `src/ha-agent/src/main/java/com/neo4j/ha/agent/recovery/OldPrimaryRecovery.java` (+62)
- `src/ha-agent/src/main/java/com/neo4j/ha/agent/recovery/PostSwitchoverReconciler.java` (±60 doc + signature)
- `docker/init-3node.sh` (+5)
- `docs/nuclear-fusion/design/modules/ha-agent-design.md` (not re-reviewed — already refreshed in `db646a5`)

## 2. Summary

The re-architecture is sound and correctly implements the user's guidance: graceful switchover no longer patches over drain bugs, and reverse-reconcile is deferred to the recovery path. `PendingReconcile` + per-node Redis key is the right transport, and the snapshot-at-failover-time rationale is valid. Ordering inside `OldPrimaryRecovery.execute` (after trigger-uninstall / role-flip, before `addTarget`) is well reasoned in comments and correct.

Two concerns worth fixing before we call this done: (M1) the `PendingReconcile` key can be orphaned in Redis when Phase 8 cleanup succeeds (i.e., the old primary is reachable throughout failover), because `markPendingCleanup(..., true)` is only set on the unreachable path — orphans accumulate and the stranded writes in that window never get reconciled. (M2) `PostSwitchoverReconciler.checkpointManager` is now a dead field after removing `loadCdcCheckpoint`, which is confusing and will trip self-review 6 months from now.

Everything else is Minor / Info.

## 3. Dimension Scan

| Dimension | Status | Notes |
|---|---|---|
| Correctness | ⚠️ | M1 orphan-key window; other paths OK. |
| Security | ✅ | No new externally-reachable inputs; all keys scoped per-node. |
| Performance | ✅ | Step 4.5 adds a bolt-scan bounded by `RECONCILE_BUDGET_MS=10s`; only runs when `pending-reconcile:<nodeId>` exists. |
| Maintainability | ⚠️ | M2 dead field; m2 doc inaccuracy re: `copyCdcCheckpoint` mutation. |
| Consistency | ✅ | Follows existing `CheckpointManager` record + save/load/delete idiom; hardcoded `"neo4j"` DB matches existing `OldPrimaryRecovery` pattern. |
| Completeness | ⚠️ | No unit test for roundtrip; no metrics for pending-reconcile counters. |

## 4. Findings

### Critical (blocks merge)

*None.*

### Major (should fix before merge)

- **M1** — `src/ha-agent/src/main/java/com/neo4j/ha/agent/failover/FailoverOrchestrator.java:254-272` (Phase 3.5) together with `src/ha-agent/src/main/java/com/neo4j/ha/agent/failover/FailoverOrchestrator.java:538-565` (`tryCleanupOldPrimary`) and `src/ha-agent/src/main/java/com/neo4j/ha/agent/HaAgent.java:203-208` (`onNodeRecovered` gate).

  **Problem**: `savePendingReconcile(failedNodeId, ...)` at Phase 3.5 writes the Redis key unconditionally when the old primary has a non-cold CDC cursor. Whether that key is ever *consumed* depends entirely on `nodeRegistry.markPendingCleanup(failedNodeId, true)` being called — which only happens in `tryCleanupOldPrimary`'s **failure branches** (partial trigger uninstall `!ok` at `L543`, or the catch-all exception at `L562`). If the old primary is reachable throughout Phase 8 (short flap that healed, health-check false positive, kill with auto-restart faster than our cleanup window), `tryCleanupOldPrimary` hits the success path at `L560` and never marks pending-cleanup. Then `onNodeRecovered` gates on `isPendingCleanup(nodeId)` at `HaAgent.java:204`, returns false, and `recovery.execute(nodeId)` never runs — so the `pending-reconcile:<failedNodeId>` hash lingers in Redis indefinitely and the afterAsync stragglers that landed between `cdcCollector.stop()` (Phase 3) and `ApocTriggerUninstaller.uninstall` (Phase 8) are never reconciled. Same orphan-key outcome via a different path inside `OldPrimaryRecovery.execute` itself: if the "no distinct primary" branch at `OldPrimaryRecovery.java:168-172` is taken, we log-and-proceed (no early return), Step 8 `markPendingCleanup(..., false)` at `L255` clears the flag, and the next `onNodeRecovered` will ignore this node forever.

  **Fix** (pick one or both):
  1. **Safety net**: add a TTL when writing the hash. In `CheckpointManager.savePendingReconcile`, after `jedis.hset(...)`, call `jedis.pexpire(PENDING_RECONCILE_PREFIX + oldPrimaryId, 7L * 24 * 3600 * 1000L)`. A week is long enough that any realistic recovery will consume the key first, short enough that orphans self-clean.
  2. **Exactly-once consumption**: if Phase 3.5 decides to write a pending-reconcile, also ensure `recovery.execute` will run on next health recovery. Either call `nodeRegistry.markPendingCleanup(failedNodeId, true)` inside the same try-block as `savePendingReconcile`, or change the `onNodeRecovered` gate at `HaAgent.java:204` to also trigger when `checkpointManager.loadPendingReconcile(nodeId).isPresent()`. The first is the smaller blast radius.

  I'd do **both**: (1) for defense against the "no distinct primary" branch inside recovery, (2) for the happy-path orphan case.

- **M2** — `src/ha-agent/src/main/java/com/neo4j/ha/agent/recovery/PostSwitchoverReconciler.java:93,97,100`. `checkpointManager` is kept as a constructor parameter and private final field, but after the signature change at `L125` the class no longer calls any `checkpointManager.*` method. Grep confirms 0 uses past the assignment. This is dead weight: every future reader will assume the reconciler still reads Redis and spend time figuring out why the field is there. It also means `OldPrimaryRecovery.java:178` is passing a dependency that's actually unused.

  **Fix**: drop the field + constructor param. Update the one caller at `OldPrimaryRecovery.java:178` to `new PostSwitchoverReconciler(clusterState, "neo4j")`. If you want to hold the field for symmetry with a future use (e.g., persisting reconcile-result counters), add a `// INTENTIONAL: reserved for …` comment — silent dead fields age badly.

### Minor

- **m1** — `src/common/src/main/java/com/neo4j/ha/common/redis/CheckpointManager.java:256-258`. The Javadoc says "the CDC key may have been touched between failover and recovery — `copyCdcCheckpoint` moves the value onto the new primary". Reading `copyCdcCheckpoint` at `L179-194`, it **copies** (loadCdcCheckpoint + saveCdcCheckpoint on the *destination* key) — it does not mutate the source key. So the source key `cdc-checkpoint:<failedNodeId>` survives `copyCdcCheckpoint` unchanged. The real reason to snapshot is the second-failover case ("a second failover/recovery cycle could overwrite it" — true), and the general hygiene of not trusting a Redis key through multiple cluster-state transitions. **Fix**: drop "`copyCdcCheckpoint` moves the value" and keep only the "second failover cycle could overwrite it" rationale; that one is correct and sufficient.

- **m2** — `src/common/src/main/java/com/neo4j/ha/common/redis/CheckpointManager.java:91` writes `schemaVersion=1` into the hash, but `loadPendingReconcile` at `L96-110` never reads or validates it. The field is write-only. If this is intentional forward-compat, fine — but then add a comment explaining the rollout plan (how a v2 loader coexists with a v1 writer, or vice versa). As is, a reader will wonder whether it's a stub or a forgotten check. **Fix**: either (a) add a 2-line comment next to the `hash.put("schemaVersion", "1")` line explaining "reserved for future loader compat; bump to 2 when field shape changes", or (b) read it and `log.warn` on unexpected value.

- **m3** — `src/common/src/main/java/com/neo4j/ha/common/redis/CheckpointManager.java:82-94`. `savePendingReconcile` uses `jedis.hset(key, map)` which **merges** fields into an existing hash rather than replacing it. Today all fields are always written, so this is a no-op hazard — but if the `PendingReconcile` record ever grows an optional field that's sometimes null, a stale value from a previous failover at the same node would survive. **Fix**: either `jedis.del(key)` before `hset`, or use a Pipeline/Transaction that does DEL+HSET atomically. Low priority since today's schema has no optional fields, but cheap to harden now.

- **m4** — `src/ha-agent/src/main/java/com/neo4j/ha/agent/recovery/OldPrimaryRecovery.java:183-184`. The log "replayed N stranded event(s)" fires even when `N==0`. Operators grepping logs during incident response will get a flood of zero-replay lines after every clean reboot of a node that had the pending key. **Fix**: branch the log — `INFO` for `N>0`, `DEBUG` (or nothing) for `N==0`. Keep the outer `log.info("Step 4.5 (BUG-080): reconciling … cursors …")` as the "we ran it" signal.

- **m5** — `src/ha-agent/src/main/java/com/neo4j/ha/agent/recovery/OldPrimaryRecovery.java:178-189`. The reconciler is instantiated inside `execute()` on every recovery. Not wrong (it's cheap), but it breaks the "declare collaborators in the constructor" convention the rest of this class follows (see `ClusterStateManager`, `SyncApplier`, `CdcCollector`, `CheckpointManager` at `L38-44`). **Fix**: if M2 is applied (drop `checkpointManager` dep), the reconciler has just `(clusterState, database)` — trivial to hoist to a constructor-held field. If you keep the factory-style construction, a single-line comment ("created per-recovery; stateless & cheap") avoids the reader assuming the file forgot something.

### Info / Observations

- **i1** — `PendingReconcile.lastNodeTs` and `newPrimaryAtFailover` are captured at Phase 3.5 but never used by `reconcile()`. The class Javadoc at `CheckpointManager.java:64-66,69-71` correctly marks them as diagnostic-only. This is fine and worth preserving — `lastNodeTs` will matter the day we decide to extend the scan to node-create events, and `newPrimaryAtFailover` is exactly what forensic log-reading needs. No action.

- **i2** — There are no unit tests asserting the save / load / delete roundtrip of `PendingReconcile`, nor Mockito tests for `FailoverOrchestrator` Phase 3.5 writing the intent, nor for `OldPrimaryRecovery` Step 4.5 consuming + deleting it. The change silently compiles-and-ships. Given how unreproducible the afterAsync gap is in practice, a regression here (e.g., someone refactoring `hset` to `hsetnx`) would only surface under chaos test. **Suggestion**: one `CheckpointManagerTest` method for the roundtrip + empty-hash case; one Mockito test asserting `savePendingReconcile` is called with the right cursors from Phase 3.5. Not blocking merge — but should be in the follow-up backlog.

- **i3** — No `HaMetrics` counters for `pendingReconcileSavedTotal`, `pendingReconcileConsumedTotal`, `pendingReconcileReplayedEventsTotal`, `pendingReconcileSkippedNoPrimaryTotal`. Everything is log-only. For a mechanism that's supposed to silently run in production on rare events, log-only observability is inverse-ergonomic: you don't know it's broken until you run `rel-gap-diag.sh`. Adding four counters mirrors `oldPrimaryRecoveryTotal` / `autoFullsyncSuppressedTotal` patterns already in `HaMetrics.java`. Follow-up backlog.

- **i4** — `OldPrimaryRecovery.java:179` passes the hard-coded string `"neo4j"` as the database name to `PostSwitchoverReconciler`. This matches `indexInstaller.ensureIndexes(oldDriver, "neo4j")` at `L132`, so consistency-within-file is preserved — but the whole class has no `database` field while `FailoverOrchestrator` does. If `database` ever becomes configurable, two independent files will need editing. Not a change-introduced problem; pre-existing.

- **i5** — Phase 3.5 runs before `fencingTokenManager.increment()` (Phase 4). The save is a plain Redis write not gated by the epoch, so a slow-failover whose epoch was already bumped could in principle write a stale pending-reconcile. In practice `doFailoverPhases2to10` is serialized by `failoverExecutor` (`HaAgent.java:177-182`), so no concurrency. Flagging for awareness if anyone removes that serialization in the future.

- **i6** — `init-3node.sh` adds `redis_del_pattern "neo4j:ha:pending-reconcile:*"` at `docker/init-3node.sh:148`. The surrounding comment is good. The KEYS/SCAN-based delete patterns are fine for the init script's single-shot use. No concern.

- **i7** — Commit message `增加failover后的修复动作` is thin for a change that re-architects the reconciler contract. Future `git log --grep` will miss this. Follow-up backlog: `git commit --amend` with a longer body before pushing, or add to the next commit's body. (Non-blocking; historical hygiene.)

## 5. Strengths (值得表扬)

- **`CheckpointManager.java:31-40`** — the `PENDING_RECONCILE_PREFIX` Javadoc is a model of what a Redis-key schema comment should contain: who writes, who reads, when deleted, and the operational fallback (`rel-gap-diag.sh`) when things go wrong. Keep doing this.

- **`FailoverOrchestrator.java:458-468`** — removing the reconciler from the switchover path and replacing it with a clear "why not" comment is exactly the right move. The comment explicitly calls out that reverse-reconcile on the graceful path would "mask the bug and add RTO" — that's the correct mental model and will stop the next engineer from "fixing" this by re-adding the call.

- **`OldPrimaryRecovery.java:145-158`** — the ordering-rationale block listing three reasons Step 4.5 must run before `addTarget` is exactly the kind of comment that earns its line count. Triggers off → direct write is safe; role is STANDBY → HAProxy won't route; not-yet-subscribed → no race between our replay and SyncApplier. Each bullet corresponds to a concrete invariant, cited correctly. Reviewer does not need to cross-check the state machine — the author already did.

- **`PostSwitchoverReconciler.java:104-115`** — the Javadoc for `reconcile()` makes the "use caller-supplied cursor, not Redis cursor" decision *and its reason* unmistakable. A future maintainer going "why don't we just re-read the CDC key here" will be stopped cold by this paragraph. That's how you prevent entropy.

- **`CheckpointManager.java:140` cold-cursor guard (pre-existing, preserved)** and **Phase 3.5 cold-cursor short-circuit** at `FailoverOrchestrator.java:256` — defense in depth (writer refuses to persist cold, reader refuses to act on cold). Belt-and-braces. Good.

## 6. Verdict

**REQUEST CHANGES**

**Rationale**: Core design is right, most of the code is careful, comments are excellent. Two Major findings: (M1) the orphan-key window where a reachable-but-failed-over old primary produces a `pending-reconcile:<id>` that is never consumed — small blast radius but it defeats the whole point of the mechanism in that window; a one-line TTL on `savePendingReconcile` closes it. (M2) a dead `checkpointManager` field that will confuse every future reader. Both are ≤10-minute fixes. Minor findings are accumulate-later. Once M1 + M2 land, this is a clean APPROVE.

## 7. Follow-ups

- [ ] **M1** — add TTL to `savePendingReconcile` AND/OR wire `markPendingCleanup(..., true)` into Phase 3.5 (recommendation: both, defense in depth).
- [ ] **M2** — drop `checkpointManager` param + field from `PostSwitchoverReconciler`; update `OldPrimaryRecovery.java:178-179` caller.
- [ ] **m1** — correct the `copyCdcCheckpoint` Javadoc in `CheckpointManager.PendingReconcile`.
- [ ] **m2** — either read-and-validate `schemaVersion` or add a 2-line explanatory comment.
- [ ] **m3** — DEL-before-HSET (or pipelined MULTI) in `savePendingReconcile` for schema-evolution safety.
- [ ] **m4** — suppress the zero-replay info log in `OldPrimaryRecovery` Step 4.5.
- [ ] **m5** — (optional) hoist reconciler construction to a field (after M2 lands it's a trivial ctor).
- [ ] **i2** — add `CheckpointManagerTest` roundtrip + Phase 3.5 / Step 4.5 Mockito tests.
- [ ] **i3** — add `pendingReconcile*` counters to `HaMetrics`.
- [ ] **i7** — expand the commit message body before next push, or fold a longer rationale into the next commit.
