# Sync Applier 模块详细设计

> 模块: sync-applier
> 运行方式: 作为 HA Agent 内部模块运行，通过远程 Bolt 连接备节点（v2.0 集中式架构）

---

## 1. 职责

从 Redis Stream 消费 ChangeEvent，通过远程 Bolt 连接在备节点 Neo4j 上幂等回放，保持数据与主节点一致。由 HA Agent 启停管理。

## 2. 包结构

```
com.neo4j.ha.sync/
├── SyncApplier.java                    # 主入口，生命周期管理
├── SyncApplierConfig.java              # 同步专属配置
├── consumer/
│   ├── IncrementalConsumer.java         # 增量消费循环
│   ├── FullSyncConsumer.java            # 全量同步消费
│   └── PendingRecovery.java             # PEL恢复（重启后处理未ACK消息）
├── applier/
│   ├── ChangeApplier.java               # 变更回放总调度
│   ├── NodeApplier.java                 # 节点变更回放
│   ├── RelationshipApplier.java         # 关系变更回放
│   └── CypherTemplates.java             # 回放用Cypher模板
├── validation/
│   ├── FencingTokenFilter.java          # Fencing Token校验过滤
│   ├── DuplicateDetector.java           # 重复事件检测（幂等保障）
│   └── OrderValidator.java             # 事件顺序校验
└── fullsync/
    ├── FullSyncReceiver.java            # 全量同步接收状态机
    ├── DatabaseCleaner.java             # 全量同步前清空数据
    └── BulkImporter.java               # 批量导入
```

## 3. 核心流程

### 3.1 增量消费主循环

```
SyncApplier.start(standbyDrivers)    // 由 HA Agent 传入远程 Neo4j Driver(s)
  │
  ├── 1. 使用 HA Agent 提供的 Neo4j Driver（远程 Bolt 连接备节点）
  ├── 2. 创建/确保 Consumer Group 存在
  ├── 3. PendingRecovery — 处理上次未ACK的消息
  │     └── XREADGROUP ... 0 (读PEL中的消息)
  │         → 回放 → XACK
  │
  └── consumeLoop:
        ├── 4. XREADGROUP GROUP sync-applier applier
        │     COUNT 100 BLOCK 1000
        │     STREAMS neo4j:cdc:neo4j:changes >
        │
        ├── 5. FencingTokenFilter.filter(events)
        │     → 丢弃 fencingToken < 当前已知最大Token的事件
        │
        ├── 6. DuplicateDetector.filter(events)
        │     → 基于 eventId 去重（Bloom Filter 或 LRU Set）
        │
        ├── 7. ChangeApplier.applyBatch(events)
        │     ├── 开启 Neo4j 事务
        │     ├── 逐条回放:
        │     │   ├── IndexManager.ensureIndex(event.labels)  ← 遇到新标签动态建索引
        │     │   ├── NODE_CREATED → NodeApplier.create(labels, elementId, props)
        │     │   ├── NODE_UPDATED → NodeApplier.update(labels, elementId, props)
        │     │   ├── NODE_DELETED → NodeApplier.delete(labels, elementId)
        │     │   ├── REL_CREATED  → RelationshipApplier.create(relType, startLabels, endLabels, ...)
        │     │   ├── REL_UPDATED  → RelationshipApplier.update(relType, ...)
        │     │   └── REL_DELETED  → RelationshipApplier.delete(relType, relElementId)
        │     └── 提交事务
        │
        ├── 8. XACK 批量确认
        │
        ├── 9. CheckpointManager.save(lastStreamId, lastEventTs)
        │
        └── 10. 更新 metrics → 回到 4
```

### 3.2 Cypher 回放模板

```java
public class CypherTemplates {

    // 节点创建/更新（幂等 MERGE）
    // %s = 标签列表（如 Person:Employee），必须在 MERGE 中携带以命中标签索引
    static final String NODE_MERGE = """
        MERGE (n:%s {_elementId: $elementId})
        SET n = $properties
        SET n._elementId = $elementId
        """;

    // 节点删除（带标签加速匹配）
    // %s = 标签列表，无标签时回退为不带标签的 MATCH
    static final String NODE_DELETE = """
        MATCH (n:%s {_elementId: $elementId})
        DETACH DELETE n
        """;

    static final String NODE_DELETE_NO_LABEL = """
        MATCH (n {_elementId: $elementId})
        DETACH DELETE n
        """;

    // 关系创建/更新（幂等 MERGE）
    // %1$s = 起点标签, %2$s = 终点标签, %3$s = 关系类型
    static final String REL_MERGE = """
        MATCH (a:%1$s {_elementId: $startNodeId})
        MATCH (b:%2$s {_elementId: $endNodeId})
        MERGE (a)-[r:%3$s {_elementId: $relElementId}]->(b)
        SET r = $properties
        SET r._elementId = $relElementId
        """;

    // 关系删除
    static final String REL_DELETE = """
        MATCH ()-[r:%s {_elementId: $relElementId}]->()
        DELETE r
        """;
}
```

**关键设计决策：**
- 使用 `_elementId` 作为跨主备的实体唯一标识（而非内部 ID，因为内部 ID 主备不一致）
- **MERGE 中必须携带标签**（`n:%s`），否则 Neo4j 无法命中标签属性索引，导致全表扫描
- 使用 `MERGE` 保证幂等性（重复回放不会创建重复节点）
- `SET n = $properties` 全量覆盖属性以确保最终一致。主节点的 APOC Trigger 已在创建时将 `_elementId` 写入属性，因此 `$properties` 中自然包含 `_elementId`
- **防御性补设** `SET n._elementId = $elementId`：即使 `$properties` 因序列化等原因未包含 `_elementId`，也不会丢失。多余的 SET 是幂等的，无副作用

### 3.3 全量同步接收

```
FullSyncReceiver 状态机:

  IDLE ──[收到FULL_SYNC_START]──► PREPARING
  PREPARING ──[清空本地数据库]──► RECEIVING
  RECEIVING ──[收到FULL_SYNC_BATCH]──► RECEIVING (循环)
  RECEIVING ──[收到FULL_SYNC_END]──► CATCHING_UP
  CATCHING_UP ──[增量追赶完成]──► IDLE

PREPARING阶段:
  1. 停止增量消费
  2. 通知 ClusterStateManager: serviceState → SYNCING
     → HA Agent 将备节点从 HAProxy 读 backend 摘除
  3. DatabaseCleaner.clean()
     → MATCH (n) DETACH DELETE n (分批，每批10000)
  4. 切换到全量接收模式

RECEIVING阶段:
  1. 从 fullsync stream 消费 FullSyncBatch
  2. BulkImporter.import(batch)
     → 使用 UNWIND + MERGE 批量导入
     → UNWIND $nodes AS node
       MERGE (n {_elementId: node.elementId})
       SET n = node.properties
       SET n:Label1:Label2
  3. 进度汇报：batchIndex / totalBatches
  4. serviceState 保持 SYNCING

CATCHING_UP阶段:
  1. 切换到增量消费
  2. 从 snapshotTxId 对应的 Stream 位点开始消费
  3. serviceState 保持 SYNCING
  4. 每次消费后检查 syncLagMs:
     → syncLagMs < syncLagThreshold 且持续 stableDuration
     → 通知 ClusterStateManager: serviceState → ONLINE
     → HA Agent 将备节点加入 HAProxy 读 backend
  5. 切换为正常 IDLE 状态
```

### 3.4 节点服务状态与同步模式的关系

| 同步阶段 | FullSyncReceiver 状态 | serviceState | 接收读流量 | Failover 候选 |
|---------|---------------------|-------------|-----------|-------------|
| 首次启动/备份导入后增量追赶 | — | SYNCING | 否 | 否 |
| 全量同步-准备 | PREPARING | SYNCING | 否 | 否 |
| 全量同步-接收 | RECEIVING | SYNCING | 否 | 否 |
| 全量同步-追赶 | CATCHING_UP | SYNCING | 否 | 否 |
| 增量同步-追赶中(lag > 阈值) | — | SYNCING | 否 | 否 |
| 增量同步-稳态(lag < 阈值) | IDLE | ONLINE | **是** | **是** |

## 4. 幂等性保障

| 事件类型 | 幂等策略 | 说明 |
|---------|---------|------|
| NODE_CREATED | MERGE by _elementId | 已存在则更新 |
| NODE_UPDATED | MERGE + SET = | 全量覆盖属性 |
| NODE_DELETED | MATCH + DELETE | 不存在则忽略 |
| REL_CREATED | MERGE by _elementId | 已存在则更新 |
| REL_UPDATED | MERGE + SET = | 全量覆盖属性 |
| REL_DELETED | MATCH + DELETE | 不存在则忽略 |

## 5. DuplicateDetector

使用 LRU Set（容量 10 万）缓存最近处理过的 eventId：
- 消费到新事件 → 检查 eventId 是否在 Set 中
- 如果存在 → 跳过（已处理过，可能是 PEL 恢复的重复消息）
- 如果不存在 → 处理 + 加入 Set

## 6. 错误处理

| 场景 | 处理 |
|------|------|
| Neo4j 写入失败（单条） | 记录失败事件，跳过，继续处理批次中的其他事件。不ACK该消息 |
| Neo4j 写入失败（事务级） | 整个批次回滚，不ACK，下次消费时重新处理 |
| Redis 消费断连 | 自动重连，从 checkpoint 恢复 |
| Fencing Token 异常 | 丢弃旧 Token 的事件，更新本地已知最大 Token |
| 全量同步中断 | 回到 PREPARING 重新开始 |

## 7. 性能优化

- **批量提交**: 每 100 条事件一个 Neo4j 事务，减少事务开销
- **UNWIND 批量操作**: 同类型事件合并为 UNWIND 语句
- **异步 ACK**: 事务提交成功后异步执行 XACK
- **按标签建 `_elementId` 索引**: MERGE 必须携带标签才能命中索引（见 §3.2 模板设计）

### 7.1 备节点索引策略

> **设计原则：** Neo4j 属性索引必须绑定到标签。MERGE 语句中必须带标签才能利用索引。
> 因此采用 **"按业务标签动态建索引"** 策略，而非使用统一的 `_HasElementId` 伪标签。

**索引创建时机：** Sync Applier 启动时 + 遇到新标签时动态创建。

```java
public class IndexManager {

    private final Set<String> indexedLabels = ConcurrentHashMap.newKeySet();

    /**
     * 启动时扫描备节点已有标签，批量创建索引。
     * 后续遇到新标签时也调用此方法增量创建。
     */
    void ensureIndex(Session session, String label) {
        if (indexedLabels.contains(label)) return;
        session.run("""
            CREATE RANGE INDEX IF NOT EXISTS FOR (n:%s) ON (n._elementId)
            """.formatted(sanitizeLabel(label)));
        indexedLabels.add(label);
    }

    void ensureIndexesForAllLabels(Session session) {
        Result result = session.run("CALL db.labels() YIELD label RETURN label");
        for (Record record : result.list()) {
            ensureIndex(session, record.get("label").asString());
        }
    }
}
```

**索引示例（假设业务标签为 Person、Company、KNOWS）：**
```cypher
-- 节点标签索引（备节点）
CREATE RANGE INDEX idx_eid_Person IF NOT EXISTS FOR (n:Person) ON (n._elementId)
CREATE RANGE INDEX idx_eid_Company IF NOT EXISTS FOR (n:Company) ON (n._elementId)

-- 关系类型索引（备节点，Neo4j 5.x 支持关系属性索引）
CREATE RANGE INDEX idx_eid_KNOWS IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r._elementId)
```

**为什么不用统一伪标签 `_HasElementId`：**
- MERGE 中必须带标签才能命中索引 → 如果用 `_HasElementId`，则 MERGE 写为 `MERGE (n:_HasElementId {_elementId: ...})`
- 这会导致所有节点都额外拥有 `_HasElementId` 标签，增加存储和查询噪音
- 使用业务标签（ChangeEvent 中已携带）更自然，且查询时可以利用标签过滤缩小搜索范围
