# Integration Test Strategy

**Project:** Neo4j HA (Community Edition High Availability)
**Date:** 2026-04-14

---

## 1. Test Environment

### Required Infrastructure

| Component | Version | Count | Purpose |
|-----------|---------|-------|---------|
| Neo4j Community | 2026.2.3 | 3 instances | 1 primary + 2 standby |
| Redis | 7.x | 1 instance | Stream, checkpoint, lock, registry |
| HAProxy | 2.8+ | 1 instance | Routing with Unix domain socket admin |
| HA Agent | (this project) | 1 instance | Centralized cluster management |

### Docker Compose Setup (Recommended)

```yaml
# docker-compose.test.yml
services:
  neo4j-primary:
    image: neo4j:2026.2.3-community
    ports: ["7474:7474", "7687:7687"]
    environment:
      NEO4J_PLUGINS: '["apoc-core", "apoc-extended"]'
      NEO4J_dbms_security_auth__enabled: "false"

  neo4j-standby-1:
    image: neo4j:2026.2.3-community
    ports: ["7475:7474", "7688:7687"]
    environment:
      NEO4J_PLUGINS: '["apoc-core", "apoc-extended"]'
      NEO4J_dbms_security_auth__enabled: "false"

  neo4j-standby-2:
    image: neo4j:2026.2.3-community
    ports: ["7476:7474", "7689:7687"]
    environment:
      NEO4J_PLUGINS: '["apoc-core", "apoc-extended"]'
      NEO4J_dbms_security_auth__enabled: "false"

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  haproxy:
    image: haproxy:2.8-alpine
    ports: ["7690:7690"]
    volumes:
      - ./config/haproxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg
      - /tmp/haproxy:/var/run/haproxy
```

---

## 2. Integration Test Scenarios

### 2.1 CDC Pipeline (cdc-collector + common)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-01 | Node create → CDC event | Create node on primary, wait poll interval, check Redis stream | Stream contains NODE_CREATED event with correct properties |
| IT-02 | Node update → diff capture | Create node, update property, wait | Event has beforeState with old props |
| IT-03 | Node delete → transit capture | Create node, delete it, wait | NODE_DELETED event via _CDCDeleteEvent transit node |
| IT-04 | Relationship CRUD | Create nodes + relationship, update, delete | 3 events in stream with correct types |
| IT-05 | Keyset pagination ordering | Bulk create 100 nodes in rapid succession | All 100 captured, timestamps monotonically increasing |
| IT-06 | Fencing token rejection | Publish with token=5, set Redis key to 10, publish with token=5 | FencingTokenRejectedException |
| IT-07 | Checkpoint persistence | Run CDC, stop, check Redis checkpoint, restart | Resumes from checkpoint, no duplicate events |
| IT-08 | PublishBuffer fallback | Stop Redis, create node on primary, restart Redis | Events buffered, then drained to stream |

### 2.2 Sync Pipeline (sync-applier + common)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-10 | Node sync end-to-end | Create node on primary → CDC → stream → sync-applier → standby | Node exists on standby with same properties |
| IT-11 | Relationship sync | Create 2 nodes + relationship on primary, wait full cycle | Relationship exists on standby |
| IT-12 | Idempotent MERGE | Send same NODE_CREATED event twice | Only 1 node on standby (DuplicateDetector) |
| IT-13 | Fencing filter | Send events with mixed fencing tokens | Only events with valid token applied |
| IT-14 | Full sync: clean + bulk import | Trigger full sync, verify standby cleared then repopulated | Standby matches primary (node count, relationship count, properties) |
| IT-15 | Full sync: state machine | Monitor FullSyncReceiver states during sync | IDLE → PREPARING → RECEIVING → CATCHING_UP → IDLE |
| IT-16 | Dynamic index creation | Create node with new label, sync to standby | _elementId index auto-created for new label on standby |
| IT-17 | Sync checkpoint recovery | Sync 50 events, kill applier, restart | Resumes from checkpoint, no data loss |

### 2.3 Health Check & Failover (ha-agent)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-20 | Health check L1-L3 | Start all nodes, verify healthy | All nodes HEALTHY |
| IT-21 | Primary failure detection | Stop primary Neo4j container | Primary → SUSPECT → DOWN within threshold |
| IT-22 | Automatic failover | Stop primary, wait | 8-phase failover completes: new primary selected, HAProxy updated, CDC switched |
| IT-23 | Failover rate limiting | Trigger 2 failovers in quick succession | Second blocked by minInterval |
| IT-24 | Failover max-per-hour | Trigger maxAutoPerHour+1 failovers | Last one blocked |
| IT-25 | Confirmation wait cancellation | Stop primary, restart before confirmation wait expires | Failover cancelled, "recovered" in audit |
| IT-26 | HAProxy route verification | After failover, check HAProxy stats | New primary in `ready` state, old primary in `maint` |
| IT-27 | Split-brain prevention | Simulate network partition (old primary still running) | Fencing token prevents old primary's events from being applied |

### 2.4 Old Primary Recovery (ha-agent)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-30 | Immediate cleanup | Failover, bring old primary back immediately | APOC triggers removed, _CDCDeleteEvent cleaned, role=STANDBY, sync started |
| IT-31 | Deferred cleanup | Failover while old primary unreachable, bring it back later | pendingCleanup=true, then recovery executed on reconnect |
| IT-32 | Sync strategy evaluation | Old primary with valid checkpoint | Incremental sync (not full sync) |
| IT-33 | Sync strategy: stale checkpoint | Old primary with expired checkpoint | Full sync triggered |

### 2.5 APOC Trigger Installation (ha-agent bootstrap)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-40 | Trigger installation | Start HA Agent against fresh Neo4j | 3 triggers installed: cdc-timestamp, capture-node-deletes, capture-rel-deletes |
| IT-41 | _updated_at auto-set | Create/update node after triggers installed | `_updated_at` property set automatically |
| IT-42 | _CDCDeleteEvent creation | Delete a node | _CDCDeleteEvent transit node created before deletion |
| IT-43 | Trigger uninstallation | Call ApocTriggerUninstaller | All 3 triggers removed, no impact on normal operations |

### 2.6 Admin HTTP API (ha-agent)

| # | Scenario | Steps | Expected |
|---|----------|-------|----------|
| IT-50 | GET /health | Healthy cluster | 200 OK, status=UP |
| IT-51 | GET /cluster/status | Running cluster | JSON with all nodes, roles, health states |
| IT-52 | POST /cluster/failover | Manual failover | Failover executes, 200 OK with new primary |
| IT-53 | POST /cluster/backup/start | Start backup | SyncApplier paused, 200 OK |
| IT-54 | POST /cluster/backup/stop | Stop backup | SyncApplier resumed, 200 OK |
| IT-55 | GET /metrics | Prometheus scrape | Valid Prometheus text format |
| IT-56 | Auth required | Request without Bearer token | 401 Unauthorized |

---

## 3. End-to-End Test Scenarios

### E2E-01: Full Cluster Lifecycle

1. Start 3 Neo4j + Redis + HAProxy + HA Agent
2. Verify initial state: 1 PRIMARY + 2 STANDBY, all HEALTHY
3. Write 1000 nodes + 500 relationships to primary via HAProxy
4. Wait for sync completion (check sync lag metric < 1s)
5. Verify standby-1 and standby-2 have identical data
6. Stop primary Neo4j container
7. Wait for failover (check /cluster/status)
8. Verify new primary accepts writes via HAProxy
9. Write 100 more nodes to new primary
10. Verify remaining standby receives new data
11. Bring old primary back
12. Verify old primary recovers as STANDBY and syncs

### E2E-02: Data Consistency Under Load

1. Start cluster
2. Run concurrent write load (10 threads, 100 ops each) to primary
3. Wait for sync stabilization
4. Compare node/relationship counts across all instances
5. Sample 100 random nodes, verify property equality

### E2E-03: Rolling Restart

1. Start cluster with data
2. Restart standby-1 (stop + start)
3. Verify standby-1 re-syncs without full sync (checkpoint valid)
4. Restart standby-2
5. Verify cluster health maintained throughout

---

## 4. Test Framework & Tooling

| Tool | Purpose |
|------|---------|
| JUnit 5 | Test runner |
| Testcontainers | Docker-based Neo4j/Redis/HAProxy lifecycle |
| Neo4j Java Driver | Direct database assertions |
| Jedis | Redis stream/state assertions |
| Awaitility | Async condition polling (wait for sync, failover) |
| REST-assured | Admin HTTP API testing |

### Maven Dependencies (test scope)

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.19.7</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>neo4j</artifactId>
    <version>1.19.7</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.awaitility</groupId>
    <artifactId>awaitility</artifactId>
    <version>4.2.1</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>io.rest-assured</groupId>
    <artifactId>rest-assured</artifactId>
    <version>5.4.0</version>
    <scope>test</scope>
</dependency>
```

---

## 5. CI/CD Integration

### Pipeline Stages

```
compile → unit-test → integration-test → e2e-test → package
```

### Configuration

| Stage | Trigger | Docker Required | Timeout |
|-------|---------|----------------|---------|
| unit-test | Every commit | No | 2 min |
| integration-test | Every PR | Yes (Testcontainers) | 10 min |
| e2e-test | Merge to main | Yes (full cluster) | 30 min |

### GitHub Actions Example

```yaml
jobs:
  unit-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { java-version: '17', distribution: 'temurin' }
      - run: mvn test

  integration-test:
    runs-on: ubuntu-latest
    services:
      docker:
        image: docker:dind
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { java-version: '17', distribution: 'temurin' }
      - run: mvn verify -Pit
```

---

## 6. Test Data Management

- **Fixture data**: Use Cypher scripts in `src/test/resources/fixtures/` for repeatable data setup
- **Cleanup**: Each test should use `MATCH (n) DETACH DELETE n` in `@BeforeEach` or fresh Testcontainers
- **Data assertions**: Compare by `_elementId` (the sync key), not by internal Neo4j node IDs

---

## 7. Key Metrics to Assert

| Metric | Healthy Value |
|--------|-------------|
| `ha_sync_lag_ms` | < 1000 ms under normal load |
| `ha_sync_apply_errors_total` | 0 |
| `ha_cdc_poll_errors_total` | 0 |
| `ha_failover_total{success=true}` | Matches expected failover count |
| `ha_health_check_failures_total` | 0 (when all nodes up) |
