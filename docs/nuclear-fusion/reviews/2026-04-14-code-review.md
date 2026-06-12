# Neo4j HA Code Review Report

**Date:** 2026-04-14
**Phase:** Nuclear Fusion Phase 5 — Code Review
**Scope:** All 4 modules (common, cdc-collector, sync-applier, ha-agent), ~85 Java files

---

## Round 1: Design Compliance Review

### Module: common

| Item | Status | Notes |
|------|--------|-------|
| ChangeEvent record | PASS | All 8 fields match design |
| ChangeEventType enum | PASS | All variants present |
| EntityData record | PASS | 8 fields, node + relationship support |
| NodeInfo with builder methods | PASS | withRole/withHealth/withServiceState/withPendingCleanup |
| NodeServiceState transitions | PASS | OFFLINE → SYNCING → ONLINE documented |
| StreamPublisher fencing Lua | PASS | Atomic check+publish via EVAL |
| StreamConsumer XREADGROUP | PASS | consume/ack/readPending/claim |
| DistributedLock Lua scripts | PASS | Acquire(NX+PX)/Renew/Release all correct |
| CheckpointManager | PASS | CDC + Sync checkpoints, composite cursor |
| HaConfig nested records | PASS | All YAML sections mapped |
| ConfigLoader env resolution | PASS | `${ENV_VAR:-default}` pattern |
| EventSerializer/Deserializer | PASS | JSON + Map bidirectional |
| HaMetrics Prometheus | PASS | All metric categories present |
| RetryUtil | PASS | Configurable retry with exception type |
| UUID v7 (IdGenerator) | PASS | Time-ordered UUID generation |

### Module: cdc-collector

| Item | Status | Notes |
|------|--------|-------|
| Keyset pagination (Node) | PASS | `WHERE n._updated_at > $lastTs OR (= AND _elementId >)` |
| Keyset pagination (Rel) | PASS | Same pattern for relationships |
| DeleteEventCapture transit nodes | PASS | Scans `_CDCDeleteEvent`, cleanup after publish |
| DiffCalculator LRU | PASS | LinkedHashMap with removeEldestEntry |
| ChangeEventBuilder UUID v7 | PASS | Uses IdGenerator |
| PublishBuffer file overflow | PASS | JSONL file fallback |
| FullSyncCoordinator flow | PASS | START → export nodes → export rels → END |
| PollingState composite cursor | PASS | lastTs + lastElementId + delete cursors |

### Module: sync-applier

| Item | Status | Notes |
|------|--------|-------|
| IncrementalConsumer XREADGROUP | PASS | With FencingTokenFilter + DuplicateDetector |
| FullSyncConsumer | PASS | Consumes from fullsync stream |
| PendingRecovery PEL | PASS | Reads with ID "0-0" on startup |
| MERGE-based idempotent Cypher | PASS | NODE_MERGE, REL_MERGE templates |
| IndexManager dynamic indexes | PASS | Per-label _elementId index creation |
| FencingTokenFilter | PASS | Filters stale tokens |
| DuplicateDetector LRU | PASS | LinkedHashSet dedup |
| FullSyncReceiver state machine | PASS | IDLE→PREPARING→RECEIVING→CATCHING_UP |
| DatabaseCleaner batch delete | PASS | DETACH DELETE with LIMIT |
| BulkImporter UNWIND+MERGE | PASS | Bulk import for nodes and relationships |

### Module: ha-agent

| Item | Status | Notes |
|------|--------|-------|
| Failover 8-phase orchestration | PASS | All 8 phases implemented per design |
| FencingTokenManager Redis INCR | PASS | Monotonic token management |
| StandbySelector | PASS | ONLINE+HEALTHY, newest checkpoint |
| Multi-layer health check (L1-L3) | PASS | TCP→Bolt→Cypher; L4 Write not in routine check |
| HAProxy Unix domain socket | PASS | Java 16+ UnixDomainSocketAddress |
| ApocTriggerInstaller 3 triggers | PASS | cdc-timestamp, capture-node-deletes, capture-rel-deletes |
| OldPrimaryRecovery 8-step | PASS | Uninstall→cleanup→role→index→eval→sync→wait→clear |
| BackupCoordinator pause/resume | PASS | With timeout protection |
| AdminHttpServer REST API | PASS | /health, /cluster/status, /failover, /backup/*, /metrics |
| NodeRegistry Redis Hash | PASS | With periodic updates |
| ClusterInitializer | PASS | Discover→restore→verify |

**Design Compliance Summary:** All major design specifications are implemented. No missing features detected.

---

## Round 2: Code Quality Review

### CRITICAL — Security Issues

#### S1. Cypher Injection via Label/RelType String Formatting

**Severity:** CRITICAL
**Files affected:**
- `sync-applier/.../NodeApplier.java:34,55` — `CypherTemplates.NODE_DELETE.formatted(labelStr)`
- `sync-applier/.../RelationshipApplier.java:32,62` — `CypherTemplates.REL_DELETE.formatted(relType)`
- `sync-applier/.../BulkImporter.java:81,104` — `SET n:%s` and `MERGE (a)-[r:%s]`
- `sync-applier/.../IndexManager.java:24,41` — `CREATE RANGE INDEX ... FOR (n:%s)`
- `ha-agent/.../IndexInstaller.java:27,31,43,45` — Same pattern

**Description:** Labels and relationship types from incoming CDC events are interpolated into Cypher via `String.formatted()`. While `IndexManager.sanitizeLabel()` backtick-escapes special characters (and properly handles embedded backticks), **the data originates from the primary Neo4j database, not from untrusted user input**. The sanitization with backtick-escaping is the standard Neo4j approach for dynamic labels.

**Verdict:** LOW actual risk (data comes from trusted Neo4j source, sanitization is correct), but the pattern should be documented. No fix needed.

#### S2. HAProxy Command Injection

**Severity:** MEDIUM
**File:** `ha-agent/.../HaProxyUpdater.java:34,37,40,55`

**Description:** Server names from config are formatted into HAProxy admin commands: `"set server %s/%s state drain".formatted(primaryBackend, oldPrimaryServer)`. These values come from user-provided config or `ClusterStateManager` (which reads from config).

**Fix:** Validate server names against `[a-zA-Z0-9_.-]+` pattern at config load time (in `ConfigValidator`).

---

### HIGH — Concurrency Issues

#### C1. DiffCalculator is not thread-safe

**Severity:** HIGH
**File:** `cdc-collector/.../DiffCalculator.java:11-14`

**Description:** Uses plain `LinkedHashMap` (not synchronized) as LRU cache. Although `CdcCollector.pollLoop` runs on a single-threaded scheduler, the cache could be accessed during `switchTarget()` restart, creating a brief race window.

**Fix:** Wrap with `Collections.synchronizedMap()` or use a concurrent LRU implementation.

#### C2. DuplicateDetector likely not thread-safe

**Severity:** MEDIUM
**File:** `sync-applier/.../DuplicateDetector.java`

**Description:** Similar LRU pattern. `SyncApplier` uses a single thread for consumption, but `drainPending()` could be called from the failover thread.

**Fix:** Ensure all access happens on the consumer thread, or synchronize.

#### C3. FailoverOrchestrator rate-limiting is non-atomic

**Severity:** LOW
**File:** `ha-agent/.../FailoverOrchestrator.java:143-153`

**Description:** `checkSafeToFailover()` reads `lastFailoverTime` and `failoverCountInHour` without atomicity. In practice, failover is triggered from a single health-checker callback, so this is not a real risk. The `failoverCountInHour` counter also never resets (minor bug — should reset hourly).

**Fix:** Add hourly reset logic for `failoverCountInHour`, or use a sliding window approach.

---

### HIGH — Resource & Error Handling

#### R1. RelationshipApplier dead code

**Severity:** MEDIUM
**File:** `sync-applier/.../RelationshipApplier.java:50-53`

**Description:** Lines 50-53 construct a `cypher` string using `CypherTemplates.REL_MERGE`, but lines 56-62 immediately overwrite it with a fallback query. The first assignment is dead code.

**Fix:** Remove lines 49-53 (the dead `REL_MERGE` usage).

#### R2. BulkImporter creates one transaction per label-set operation

**Severity:** MEDIUM
**File:** `sync-applier/.../BulkImporter.java:74-86`

**Description:** For each node in a full-sync batch, a separate `session.executeWrite()` is called just to set labels. This is N transactions for N nodes in a batch — potentially very slow.

**Fix:** Batch label-setting into a single transaction, or group nodes by label combination.

#### R3. PublishBuffer memory/disk inconsistency

**Severity:** MEDIUM
**File:** `cdc-collector/.../PublishBuffer.java:39-47`

**Description:** `add()` increments `bufferSize` by events count, but `flushToFile()` decrements per-event. If `add()` is called concurrently while flushing, the counter may drift. Also, file-flushed events are never read back — the `drain()` method only reads from the in-memory queue.

**Fix:** Implement file-based drain or document that file flush is emergency-only persistence.

#### R4. HaProxySocketClient read buffer too small

**Severity:** LOW
**File:** `ha-agent/.../HaProxySocketClient.java:25`

**Description:** 4KB buffer may be insufficient for `show servers state` response on large configurations. The read loop handles this correctly (reads until EOF), but could be more efficient with a larger buffer.

**Fix:** Increase to 16KB or make configurable.

#### R5. FullSyncReceiver blocking call in onFullSyncStart

**Severity:** HIGH
**File:** `sync-applier/.../FullSyncReceiver.java:43-47`

**Description:** `onFullSyncStart()` calls `databaseCleaner.clean()` and `fullSyncConsumer.consumeFullSyncBatches()` — both blocking operations. This is called from within the `IncrementalConsumer.consumeOnce()` loop, which means the incremental consumer thread is blocked for the entire full sync duration.

**Verdict:** This is actually correct behavior per design — during full sync, incremental consumption must pause. The state machine handles this. No fix needed, but should be documented.

---

### MEDIUM — Performance Issues

#### P1. IndexManager ensureIndex runs session.run() outside transaction

**Severity:** MEDIUM
**File:** `sync-applier/.../IndexManager.java:23-24`

**Description:** `session.run()` is called directly (auto-commit) inside `ChangeApplier.applyEvent()` which is already inside a `session.executeWrite()` transaction. Neo4j driver allows this but it creates a separate auto-commit transaction for index creation while the main write transaction is open.

**Fix:** Move index checks before the `executeWrite()` block, or cache more aggressively to avoid repeated checks.

#### P2. OldPrimaryRecovery cleanup limited to 10000

**Severity:** LOW
**File:** `ha-agent/.../OldPrimaryRecovery.java:98`

**Description:** `cleanupResidualDeleteEvents` only cleans up to 10000 `_CDCDeleteEvent` nodes. If there are more, they remain.

**Fix:** Add a loop until count = 0, same as `DatabaseCleaner` pattern.

---

### LOW — Code Standards

#### CS1. Magic numbers

**Severity:** LOW
**Files:**
- `PublishBuffer.java:45` — `10000` (flush threshold)
- `PublishBuffer.java:75` — `10000` (flush batch size)
- `OldPrimaryRecovery.java:98` — `10000` (cleanup limit)
- `HaProxySocketClient.java:25` — `4096` (buffer size)

**Fix:** Extract to named constants.

#### CS2. StreamPublisher.publishBatch fencing check is non-atomic

**Severity:** MEDIUM
**File:** `common/.../StreamPublisher.java:62-67`

**Description:** `publishBatch()` checks the fencing token with a plain GET, then publishes via Pipeline. Between the GET and Pipeline execution, another writer could increment the token. The single-event `publish()` uses atomic Lua correctly, but the batch version does not.

**Fix:** Either use Lua for the entire batch, or accept the race window and document it (fencing is best-effort for batches).

---

## Issues Summary

| Severity | Count | Action Required |
|----------|-------|----------------|
| CRITICAL | 0 | — |
| HIGH | 2 | R1 (dead code), C1 (DiffCalculator thread safety) |
| MEDIUM | 6 | S2, C2, R2, R3, P1, CS2 |
| LOW | 4 | C3, R4, P2, CS1 |

## Recommended Fixes (Phase 6)

**Must fix:**
1. **R1** — Remove dead code in `RelationshipApplier.java:49-53`
2. **C1** — Synchronize `DiffCalculator` LRU cache
3. **CS2** — Document or fix `StreamPublisher.publishBatch` fencing gap
4. **C3** — Add hourly reset for `failoverCountInHour`

**Should fix:**
5. **R2** — Batch label-setting in `BulkImporter`
6. **R3** — Document PublishBuffer file flush as emergency-only
7. **S2** — Add server name validation in `ConfigValidator`
8. **P2** — Loop `cleanupResidualDeleteEvents` until complete

**Nice to have:**
9. **CS1** — Extract magic numbers to constants
10. **R4** — Increase HAProxy socket buffer
