# BUG-084 Full Sync Consumer 提前退出，导致 standby 大量丢失关系

- 日期：2026-05-15
- 编号：**BUG-084**（沿用项目编号习惯，与 BUG-067 / 074 / 078 / 079 / 080 / 081 / 083 同属"主备同步一致性"类）
- 类别：Critical（**Phase 1 已修复**）
- 影响模块：`src/sync-applier/.../consumer/FullSyncConsumer.java`
- 关联文档：`docs/design/2026-05-14-ha-agent-ui-solution.md` §13（数据一致性 / Full Sync 是 v1.2 的兜底修复手段）

---

## 1. 现象

执行 Full Sync 后通过 cypher 对比主备节点 / 关系数：

```
== neo4j-primary: relationship types ==
"aiagent_BELONGS_TO",   6401
"aiagent_MENTIONED_IN", 11199
"aiagent_RELATES_TO",   4697     合计 22 297

== neo4j-standby-1: relationship types ==
"aiagent_BELONGS_TO",   1134
"aiagent_MENTIONED_IN", 1155
"aiagent_RELATES_TO",    865     合计  3 154   (-85.8%)
```

**节点 100% 一致**（Community 680/680, Entity 4451/4451, Segment 1005/1005）；**关系丢失 80–90%**。
报错日志没有任何 ERROR / Exception，fullsync "正常完成"。

## 2. 复现条件

- 任意触发 Full Sync 的场景（手动 / OldPrimaryRecovery 自动）
- 关系数量 > batchSize（默认 1000）且 关系批次数 > 1
- 节点批次数 ≥ 1
- 一次 Redis `XREADGROUP` 返回中**同时包含** Node 最后一批 + 至少 1 个 Rel 批次（实测 batchSize=10 的 consumer 读普遍如此）

## 3. 根因

`FullSyncConsumer.consumeFullSyncBatches` 的退出条件是：

```java
// 修复前
if (batch.batchIndex() + 1 >= batch.totalBatches()) {
    completed = true;
}
```

但 `FullSyncCoordinator.startFullSync` 分两阶段向同一个 Redis Stream
（`neo4j:cdc:neo4j:fullsync`）publish 批次：

```
NodeExporter      → 6138 nodes / 1000 batchSize = 7 批 (totalBatches=7)
RelationshipExporter → 22204 rels / 1000 batchSize = 23 批 (totalBatches=23)
```

**两类批次各自携带自己的 `totalBatches`**。当 Node 第 7/7 批被处理后立刻 `completed=true`：

| 时刻 | 事件 |
|---|---|
| 14:53:54.030 | Consumer batch 1/7 (NODE) imported |
| 14:53:54.831 | Consumer batch 6/7 (NODE) imported |
| 14:53:54.897 | Consumer batch 7/7 (NODE) imported → **`completed=true` 触发** |
| 14:54:09.234 | Consumer batch 1/23 (REL) imported  ← 同一次 read 顺带消费 |
| 14:54:22.353 | Consumer batch 2/23 (REL) imported |
| 14:54:35.453 | Consumer batch 3/23 (REL) imported → **`while(!completed)` 退出** |
| 14:54:35.455 | `Full sync end received in unexpected state: IDLE` ← 控制事件晚到 |

剩下 20 批 REL（约 19 050 条关系）**永远没有被 consumer 消费**。

### 为什么 3 批 REL 还能进去

`StreamConsumer.consume(..., count=10, blockMs=2000)` 一次最多拉 10 条。当 batch 7/7 (NODE) 触发 `completed=true` 后，`for` 循环**不会立即 break**，会继续处理当前 batch 已读取到的剩余 entries。这些 entries 里恰好混入了 REL 1-3。之后再次进入 `while(!completed)` 判定，立即退出。

### 算术验证

- standby 总 rel = 1134 + 1155 + 865 = 3154
- 3 批 × 1000 entities ≈ 3000  ✓

## 4. FullSyncReceiver 看到的"完成"假象

```
14:54:35.453 sync-applier FullSyncReceiver - Full sync receiving complete (snapshotTs=...)
14:54:35.454 sync-applier FullSyncReceiver - Catch-up complete, returning to IDLE
14:54:35.455 sync-applier FullSyncReceiver - Full sync end received in unexpected state: IDLE
```

`receiver` 看到 `consumeFullSyncBatches` 返回 `true`，转 `CATCHING_UP`，
立即 `checkCatchUp` 满足条件（snapshotTs 已经比 lastAppliedTs 小），转回 `IDLE`。
真正的 `FULL_SYNC_END` 控制事件随后到达，却找不到 RECEIVING / CATCHING_UP 状态，
打了一行 WARN 然后丢弃。HAProxy 还会把 standby 升回 ONLINE，把残缺的 standby 推到前台。

## 5. 修复（已实施）

**文件**：`src/sync-applier/src/main/java/com/neo4j/ha/sync/consumer/FullSyncConsumer.java`

**修改**：退出条件从「任意 batch 完成」改为「NODE 最后一批 AND REL 最后一批都到达」，并增加图无关系的 5 秒静默 fallback。

```java
// 关键片段
boolean nodePhaseDone = false;
boolean relPhaseDone  = false;
long nodePhaseDoneAtMs = 0L;

while (!completed && consecutiveEmpty < MAX_CONSECUTIVE_EMPTY_READS) {
    var results = streamConsumer.consume(..., 10, 2000);

    if (results.isEmpty()) {
        consecutiveEmpty++;
        // Empty-graph edge: 5s of silence after node phase = no rels exist.
        if (nodePhaseDone && !relPhaseDone
                && (System.currentTimeMillis() - nodePhaseDoneAtMs) > REL_PHASE_GRACE_MS) {
            log.info("Full sync: node phase complete + 5s silence; assuming no rels");
            completed = true;
        }
        continue;
    }
    consecutiveEmpty = 0;

    for (var entry : results) {
        for (StreamEntry se : entry.getValue()) {
            FullSyncBatch batch = deserializer.fullSyncBatchFromMap(se.getFields());
            bulkImporter.importBatch(session, batch);
            streamConsumer.ack(...);

            if (batch.batchIndex() + 1 >= batch.totalBatches()) {
                if (batch.entityType() == EntityType.NODE) {
                    nodePhaseDone = true;
                    nodePhaseDoneAtMs = System.currentTimeMillis();
                } else if (batch.entityType() == EntityType.RELATIONSHIP) {
                    relPhaseDone = true;
                }
            }
            if (nodePhaseDone && relPhaseDone) completed = true;
        }
    }
}
```

**关键不变式**（保证修复不再出错）：

1. RELATIONSHIP 批次总是在 NODE 批次之后 publish（`FullSyncCoordinator.startFullSync` line 53-59）；所以 `relPhaseDone` 必然蕴含 `nodePhaseDone`。
2. 5 秒静默 fallback 处理"图无关系"的合法边界；不影响有关系的常规情况。
3. `consecutiveEmpty` × `blockTimeout`（默认 30 × 2s = 60s）仍是终极兜底。

## 6. 验证

修复后用户场景重新触发 Full Sync，日志应出现两行新关键标记：

```
Full sync: NODE phase complete (7 batches)
Full sync batch 1/23 imported (1000 entities, type=RELATIONSHIP)
...
Full sync batch 23/23 imported (..., type=RELATIONSHIP)
Full sync: RELATIONSHIP phase complete (23 batches)
Full sync receiving complete for node node-02, entering CATCHING_UP
```

**不能再出现**：`Full sync end received in unexpected state: IDLE`

数量级一致性验证：

```bash
for c in neo4j-primary neo4j-standby-1; do
  docker exec "$c" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
    "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY t"
done
# Primary 和 Standby 三种 rel type 的 count 应该完全一致
```

## 7. 关联 BUG / 影响面

| BUG | 关系 |
|---|---|
| BUG-074 | PEL drain 漏取一致性 — 修复增量同步的边界 |
| BUG-079 | rel afterAsync 与 node `_updated_at` 顺序错乱 — 修复增量同步丢 rel |
| BUG-080 | 优雅 switchover 时 afterAsync 残留 — 修复 switchover 数据 |
| BUG-081 | rel `_elementId` 复用 — 修复批内同 elementId |
| **BUG-084（本）** | **Full Sync 协议级 bug，导致 fullsync 也丢 80–90% 关系** |

BUG-084 是上述 BUG 的"最后一道防线"——当增量同步出现 BUG-074/079/081 类问题时，运维通过 Full Sync 兜底修复；如果 Full Sync 自己也有缺陷，问题就**无法收敛**。这是本 bug 必须 Critical 优先修复的根本原因。

## 8. 单元测试覆盖（待补）

`FullSyncConsumer` 当前**没有单元测试**。Review 报告（`docs/nuclear-fusion/reviews/2026-05-15-ha-agent-ui-review.md` m-7）已指出，但当时只覆盖了新增的 `consistency/**` 三件套，遗漏了 fullsync 链路。

**待办**：补 `FullSyncConsumerTest` 至少 3 个用例：

```java
@Test void exitsOnlyAfterBothPhasesComplete() {
    // mock stream returns: node-1..7, rel-1..23 mixed
    // assert: imports all 30 batches before exiting
}

@Test void emptyRelGraphCompletesAfterGrace() {
    // mock stream: node-1..7 only, then 5s silence
    // assert: completes within ~5s (not 60s timeout)
}

@Test void respectsHardTimeout() {
    // mock stream: half batches + permanent silence
    // assert: returns false after MAX_CONSECUTIVE_EMPTY_READS
}
```

工作量约 1 小时。建议在下一迭代补上。

## 9. 经验教训

1. **共享协议字段必须 phase-aware**：`totalBatches` 在多阶段 publisher 中复用同一字段名却各自计数，是典型的"看似简单实则歧义"的字段设计。未来 fullsync 协议演进应该考虑加 `phaseTotalBatches` 或显式 phase 字段。
2. **退出条件必须对齐 publisher 的"完成"语义**：consumer 退出条件应该匹配 publisher 端的 "all phases done"，而不是"任一 phase 的 last batch"。
3. **单测覆盖率盲区**：UI / 鉴权 / consistency 工具类都补了单测，但**核心 sync 链路**（消费者、receiver 状态机）没有。这是高风险盲区。
4. **生产数据 diff 是终极验证**：单元测试不会发现这种"协议解释错位"，只有真实主备 count 对比能暴露。要把"fullsync 后必须 cypher count 对比"列进 smoke test。

## 10. 防止重现的护栏

| 措施 | 文件 / 位置 | 状态 |
|---|---|---|
| FullSyncConsumer 单测 | `src/sync-applier/.../test/.../consumer/FullSyncConsumerTest.java` | ⏳ 待补 |
| Smoke test 加 fullsync 后 cypher count diff | `scripts/deploy/ha-smoke-test.sh` | ⏳ 待补 |
| Fullsync 阶段事件落 ui-audit 流（透明化进度） | `FullSyncCoordinator` + `BulkImporter` | ⏳ 待补（review m-3 已提） |
| Receiver 看到 "FULL_SYNC_END in unexpected state" 直接 ERROR + 上报指标 | `FullSyncReceiver.onFullSyncEnd` | ⏳ 待补（强信号告警） |

护栏 1 + 2 是最低优先级；护栏 3 / 4 优先级中等。

## 11. 上线指引（已通过）

```bash
cd /home/ubuntu/neo4j-ha
docker build -f docker/ha-agent/Dockerfile -t neo4j-ha-agent:1.1.0-SNAPSHOT .
docker compose -f docker/neo4j/deploy-test.yml --env-file docker/.env up -d --force-recreate ha-agent
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:18888/api/cluster/fullsync?nodeId=node-02"
# 等 30 秒后再 cypher 对比，三种 rel type count 应全部一致。
```

---

## 12. BUG-085 后续修复（2026-05-15 当晚）

### 12.1 BUG-084 修复后仍然不一致

用户按 §11 重新部署 + 重新 fullsync，结果**仍然丢失 80%+ 关系**（standby 只有 1111/1085/797 vs primary 6426/11117/4659）。

### 12.2 日志真相

```
15:21:09.214 Consumer group 'sync-applier-node-02-fullsync' created
15:21:12.403 batch 4/23 imported (REL)   ← 首条竟然是 REL 4/23！
...
15:21:35.309 batch 23/23 (REL) → relPhaseDone=true
15:21:35.309 RELATIONSHIP phase complete (23 batches)
15:21:36.957 batch 7/7 (NODE) → nodePhaseDone=true
15:21:36.958 NODE phase complete (7 batches)
15:21:51.280 batch 1/23 (REL)            ← 14 秒后又来！
15:22:04.838 batch 2/23 (REL)
15:22:17.291 batch 3/23 (REL)
15:22:17.291 Full sync receiving complete  ← 才退出
```

### 12.3 BUG-084 修复为什么不够

修复后逻辑：「NODE 最后一批 AND REL 最后一批都到达」时退出。但 stream 上存在**两次 fullsync 的批次叠加**（上次 BUG-084 失败时的残留 + 这次新发的），相同 `totalBatches=7/23` 值出现两次：

| 阶段 | 来源 | totalBatches | 触发的判断 |
|---|---|---|---|
| Read 1-2 (15:21:12-35) | **上次 fullsync 残留**的 REL 4-23 | 23 | 上次 REL 23/23 → relPhaseDone=true（**误判**） |
| Read 3 (15:21:36) | 本次 NODE 1-7 | 7 | 本次 NODE 7/7 → nodePhaseDone=true → **completed=true → 提前退出**（**误退**） |
| Read 4-6 (15:21:51-22:17) | 本次 REL 1-3（实际只 3 批被读到，后 20 批从未消费） | 23 | 后续未触发 |

`totalBatches` 计数器**无法区分本次 fullsync 与上次的批次**——这是协议层面的缺陷。

### 12.4 BUG-085 修复（已实施）

**两道防线并用**：

#### 防线 1：SENTINEL 显式终结符（最重要）

`FullSyncCoordinator.startFullSync` 在两阶段 export 完成后，向 fullsync stream 发**一个 sentinel batch**（`batchIndex=-1, totalBatches=0, entities=[]`）。这是"publisher 的物理终结信号"，与上次/这次的批次数完全无关。

```java
// FullSyncCoordinator.java
public static final int SENTINEL_BATCH_INDEX = -1;

FullSyncBatch sentinel = new FullSyncBatch(
    IdGenerator.uuidV7(), SENTINEL_BATCH_INDEX, 0,
    EntityType.NODE, Collections.emptyList(), System.currentTimeMillis()
);
streamPublisher.publishFullSyncBatch(fullsyncStreamKey, sentinel);
```

Consumer 看到 `batchIndex == -1` 立即退出，**完全不再依赖** `totalBatches` 计数器：

```java
// FullSyncConsumer.java
if (batch.batchIndex() == SENTINEL_BATCH_INDEX) {
    streamConsumer.ack(...);
    log.info("Full sync: SENTINEL received — publisher done.");
    completed = true;
    break;
}
```

#### 防线 2：snapshotTs 过滤旧残留批次

每个 `FullSyncBatch` 自带 `timestamp` 字段（publish 时刻）。`FULL_SYNC_START` 控制事件携带本次 fullsync 的 `snapshotTs`。Consumer 收到 batch 时若 `batch.timestamp < snapshotTs`，判定为**上次 fullsync 的残留**，**ACK 但不导入**：

```java
if (snapshotTs > 0 && batch.timestamp() < snapshotTs) {
    streamConsumer.ack(...);
    log.info("Full sync: skipping stale batch {}/{} (ts={} < snapshotTs={}) — "
           + "leftover from previous run", ...);
    continue;
}
```

这样**避免浪费时间**导入根本不会成功的旧批次（DatabaseCleaner 已清库，旧批次的 elementId 在 standby 找不到，MERGE 全部失败但仍要做 cypher 解析）。

### 12.5 涉及文件

- `src/cdc-collector/.../FullSyncCoordinator.java` — 发 sentinel
- `src/sync-applier/.../FullSyncReceiver.java` — 传 snapshotTs 给 consumer
- `src/sync-applier/.../FullSyncConsumer.java` — 识别 sentinel + 过滤 stale

### 12.6 修复后验证日志（预期）

```
Starting full sync for target node: node-02
...
Full sync sentinel published; consumer will exit on receipt
Full sync completed for target node: node-02   (publisher 端)

Full sync start received for node node-02     (receiver 端)
Database clean complete: deleted N nodes total
Full sync: skipping stale batch X/23 (type=RELATIONSHIP, ts=... < snapshotTs=...) — leftover from previous run
... (跳过所有旧残留)
Full sync batch 1/7 imported (1000 entities, type=NODE)
...
Full sync batch 23/23 imported (..., type=RELATIONSHIP)
Full sync: RELATIONSHIP phase complete (23 batches)
Full sync: SENTINEL received — publisher done. Exiting consumer loop
Full sync receiving complete for node node-02, entering CATCHING_UP
```

**关键判定**：必须看到 `SENTINEL received — publisher done` 才是正常退出。

### 12.7 上线指引（再来一次）

```bash
cd /home/ubuntu/neo4j-ha
docker build -f docker/ha-agent/Dockerfile -t neo4j-ha-agent:1.1.0-SNAPSHOT .
docker compose -f docker/neo4j/deploy-test.yml --env-file docker/.env up -d --force-recreate ha-agent

# 触发新 fullsync
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:18888/api/cluster/fullsync?nodeId=node-02"

# 验证
for c in neo4j-primary neo4j-standby-1; do
  echo "== $c =="
  docker exec "$c" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain \
    "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY t"
done
# 三种 rel type count 应全部一致
```

### 12.8 经验补充

1. **协议字段不能在跨实例间复用语义**：`totalBatches` 在一次 fullsync 内是确定的（7 或 23），但在多次 fullsync 间会重复出现，consumer 无法区分。
2. **状态机退出条件应该看"显式信号"而非"计算推断"**：sentinel 是显式信号，计数器到顶是推断。后者容易在状态污染时误判。
3. **stream-based 异步协议需要明确的 terminator**：类似 HTTP `Transfer-Encoding: chunked` 用 `0\r\n\r\n` 收尾；本协议借鉴该模式。
4. **回归测试不光要看单测，更要看真实状态污染场景**：BUG-084 单纯改逻辑没考虑 stream 残留，是在真实生产环境下才暴露出 BUG-085。

---

## 13. BUG-086 性能修复（2026-05-15 当晚续）

### 13.1 现象

BUG-085 修复后，fullsync 正确性恢复（sentinel + stale 过滤都生效，stale 批次被显式跳过），但**速度极慢**：

```
16:49:14.614 batch 1/23 imported (REL)
16:49:28.728 batch 2/23 imported     (14.1 s)
16:49:44.294 batch 3/23 imported     (15.6 s)
16:49:57.923 batch 4/23 imported     (13.6 s)
```

每个 1000 行 REL batch 耗时 13-15 秒。23 个 batch 需要 ≈ 5 分钟。用户 fullsync 进行中提前看 cypher count，发现还有大量 rel 没导完，**误以为又一次失败**。

### 13.2 根因（两个性能 bug 叠加）

`BulkImporter.importBatch` REL 路径：

```java
// Bug A: NO label → 不走 _elementId per-label 索引
String cypher = """
    MATCH (a {_elementId: $startNodeId})    ← 无 label，全图扫描
    MATCH (b {_elementId: $endNodeId})       ← 无 label，全图扫描
    MERGE (a)-[r:%s ...]->(b) ...""";

// Bug B: 每条 rel 一个独立事务 → 1000 个 commit / batch
for (Map<String, Object> rel : batch.entities()) {
    session.executeWrite(tx -> { tx.run(cypher, ...).consume(); return null; });
}
```

| Bug | 影响 | 量化 |
|---|---|---|
| **A — 全图扫描** | NODE 上的 `_elementId` UNIQUE CONSTRAINT 是 **per-label** 创建的（`IndexInstaller.java:90` `CREATE CONSTRAINT FOR (n:Person) REQUIRE n._elementId IS UNIQUE`），label-less MATCH 用不到 | 1000 条 rel × 2 次 match = 2000 次全图扫描 |
| **B — 事务风暴** | 每条 rel 一个 `executeWrite` | 1000 次 commit + fsync per batch ≈ 5-10 秒 overhead |

两者叠加：13-15 秒 / batch（用户日志）。

### 13.3 修复（已实施）

**两道一起改**：

#### A. 让 RelationshipExporter 把 labels 也传过去

`RelationshipExporter` 的 cypher **已经查询了** `startLabels` / `endLabels`：

```cypher
RETURN ..., labels(a) AS startLabels, labels(b) AS endLabels
```

但 Java 端**忘了放进 entity Map**。补上：

```java
entity.put("startLabels",
    record.get("startLabels").asList(org.neo4j.driver.Value::asString));
entity.put("endLabels",
    record.get("endLabels").asList(org.neo4j.driver.Value::asString));
```

#### B. BulkImporter REL 改 UNWIND 批量 + label-aware MATCH

按 `(startLabel0, relType, endLabel0)` 分组（业务图通常 ≤ 几十种组合），每组用一条带 label 的 UNWIND cypher：

```java
String startMatch = sLabel.isEmpty()
    ? "MATCH (a {_elementId: rel.startNodeId})"             // fallback
    : "MATCH (a:" + sLabel + " {_elementId: rel.startNodeId})";  // index hit
String endMatch = eLabel.isEmpty()
    ? "MATCH (b {_elementId: rel.endNodeId})"
    : "MATCH (b:" + eLabel + " {_elementId: rel.endNodeId})";

String cypher = """
    UNWIND $rels AS rel
    %s
    %s
    MERGE (a)-[r:%s {_elementId: rel.elementId}]->(b)
    SET r = rel.properties
    SET r._elementId = rel.elementId
    """.formatted(startMatch, endMatch, relType);

session.executeWrite(tx -> {
    tx.run(cypher, Map.of("rels", bucket)).consume();
    return null;
});
```

### 13.4 预期效果

| 维度 | 修复前 | 修复后 | 改善 |
|---|---|---|---|
| MATCH 扫描复杂度 | O(N) per match | O(log N) per match | ~1000× |
| 事务数 / batch | 1000 个 | ≤ 几十个（按 label 组合） | 50–100× |
| 单 batch 耗时 | 13-15 秒 | < 1 秒 | 15× |
| 全 23 batch（22k rels）总耗时 | ~5 分钟 | < 30 秒 | 10× |

### 13.5 涉及文件

- `src/cdc-collector/.../RelationshipExporter.java` — 补 startLabels / endLabels 进 entity Map
- `src/sync-applier/.../BulkImporter.java` — REL 路径整段重写

### 13.6 修复后验证

```
docker logs ha-agent | grep "batch.*RELATIONSHIP" | awk '{print $1}' | uniq
# 23 batches 全部完成的总时长应 < 30 秒
```

如果仍然每 batch 5 秒以上，看一下 standby 的 Neo4j 是否在 db pagecache 压力下（`docker stats neo4j-standby-1`），或者 IndexManager 是否在反复 ensure 已经存在的 index（应该有缓存）。

### 13.7 BUG-086 与 BUG-084/085 的关系

- **BUG-084**：fullsync **协议设计** 错（计数器误判退出）— 导致丢数据
- **BUG-085**：BUG-084 修复后**状态污染**仍能误退 — 用 sentinel 解决
- **BUG-086**：fullsync 正确后**性能太慢** — 用户体验仍受影响，且产生"是不是又失败了"的认知误判

三者都和 fullsync 链路相关，**但属性截然不同**：前两个是正确性，BUG-086 是性能。如果不解决 BUG-086，对小图（6k 节点）也要 5 分钟才看到结果，对大图（100k+ 节点）完全无法用。
