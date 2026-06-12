# CDC Collector 模块详细设计

> 模块: cdc-collector
> 运行方式: 作为 HA Agent 内部模块运行，通过远程 Bolt 连接主节点（v2.0 集中式架构）

---

## 1. 职责

通过远程 Bolt 连接从 Neo4j 主节点捕获数据变更，封装为 ChangeEvent，通过 Redis Stream 发布。由 HA Agent 启停管理，Failover 时由 HA Agent 调用 `switchTarget()` 切换到新主节点。

## 2. 包结构

```
com.neo4j.ha.cdc/
├── CdcCollector.java                  # 主入口，生命周期管理
├── CdcCollectorConfig.java            # CDC专属配置
├── polling/
│   ├── CypherPollingStrategy.java     # Cypher轮询实现（主方案，keyset pagination）
│   ├── PollingState.java              # 轮询状态（lastTs + lastElementId 复合游标）
│   └── ChangeDetector.java            # 变更检测接口
├── capture/
│   ├── NodeChangeCapture.java         # 节点变更捕获
│   ├── RelationshipChangeCapture.java # 关系变更捕获
│   └── DeleteEventCapture.java        # 删除事件捕获（扫描APOC Trigger写入的中转节点）
├── transform/
│   ├── ChangeEventBuilder.java        # 原始数据 → ChangeEvent 转换
│   └── DiffCalculator.java            # 计算属性差异（before/after）
├── publish/
│   ├── StreamPublishService.java      # 发布服务（含Fencing Token校验）
│   └── PublishBuffer.java             # 本地缓冲（Redis不可用时暂存）
└── fullsync/
    ├── FullSyncCoordinator.java       # 全量同步协调器
    ├── NodeExporter.java              # 分批导出节点
    └── RelationshipExporter.java      # 分批导出关系
```

## 3. 核心流程

### 3.1 增量轮询主循环

> **Keyset Pagination：** 使用 `(_updated_at, _elementId)` 复合游标替代 `WHERE _updated_at > $lastTs`。
> 解决 `timestamp()` 毫秒精度下同一毫秒内多个变更被跳过的问题。
> `_elementId` 是全局唯一的字符串，作为第二排序键保证无间隙分页。

```
CdcCollector.start(primaryDriver)    // 由 HA Agent 传入远程 Neo4j Driver
  │
  ├── 1. 使用 HA Agent 提供的 Neo4j Driver（远程 Bolt 连接）
  ├── 2. 获取 Fencing Token，校验当前连接的节点是否为合法主节点
  ├── 3. 从 CheckpointManager 恢复上次轮询位点
  │     → PollingState { lastTs, lastElementId, lastDeleteTs, lastDeleteEid }
  │
  └── pollLoop:
        ├── 4. CypherPollingStrategy.poll(pollingState, batchSize)
        │     ├── NodeChangeCapture.detectChanges()
        │     │     MATCH (n) WHERE n._updated_at > $lastTs
        │     │       OR (n._updated_at = $lastTs AND n._elementId > $lastEid)
        │     │     RETURN n, labels(n), properties(n)
        │     │     ORDER BY n._updated_at ASC, n._elementId ASC
        │     │     LIMIT $batchSize
        │     │
        │     ├── RelationshipChangeCapture.detectChanges()
        │     │     MATCH ()-[r]->() WHERE r._updated_at > $lastTs
        │     │       OR (r._updated_at = $lastTs AND r._elementId > $lastRelEid)
        │     │     RETURN r, type(r), startNode(r)._elementId, endNode(r)._elementId
        │     │     ORDER BY r._updated_at ASC, r._elementId ASC
        │     │     LIMIT $batchSize
        │     │
        │     └── DeleteEventCapture.captureDeleteEvents()
        │           MATCH (e:_CDCDeleteEvent)
        │           WHERE e.timestamp > $lastDeleteTs
        │             OR (e.timestamp = $lastDeleteTs
        │                 AND elementId(e) > $lastDeleteEid)
        │           RETURN e ORDER BY e.timestamp ASC, elementId(e) ASC
        │           LIMIT $batchSize
        │
        ├── 5. ChangeEventBuilder.build(rawChanges) → List<ChangeEvent>
        │
        ├── 6. StreamPublishService.publishBatch(events)
        │     ├── FencingToken校验（Lua脚本原子操作）
        │     ├── Pipeline批量XADD
        │     └── 失败时写入 PublishBuffer 本地缓冲
        │
        ├── 7. 清理已发布的 _CDCDeleteEvent 中转节点
        │     MATCH (e:_CDCDeleteEvent) WHERE e.timestamp <= $publishedTs
        │     DETACH DELETE e
        │
        ├── 8. CheckpointManager.save(pollingState)
        │     → 记录本批最后一条的 (lastTs, lastElementId)
        │
        └── 9. sleep(pollInterval) → 回到 4
```

### 3.2 DiffCalculator 差异计算

对于 NODE_UPDATED 和 RELATIONSHIP_UPDATED 事件，需要计算属性变更差异。

**策略：** 在本地维护一个 LRU Cache 缓存最近访问过的实体属性快照。

```
detectChanges() → 查询到实体当前状态
  → LRU Cache 中查找该 elementId 的上次快照
  → 如果找到 → 计算 diff → 生成 UPDATE 事件（含 beforeState）
  → 如果未找到 → 生成 UPDATE 事件（beforeState 为空）
  → 更新 LRU Cache
```

LRU Cache 参数：
- 容量：`cdc.cache.maxSize` (默认 50000)
- 淘汰策略：LRU
- 注意：重启后 cache 丢失，第一轮可能无法提供 beforeState（可接受）

### 3.3 全量同步流程

```
FullSyncCoordinator.startFullSync(targetNodeId)
  │
  ├── 1. 暂停增量轮询循环
  ├── 2. 发布 FULL_SYNC_START 控制事件到 control stream
  ├── 3. 记录当前快照点（snapshotTimestamp）
  │
  ├── 4. NodeExporter.export()
  │     ├── MATCH (n) RETURN count(n) → totalNodes
  │     └── 分批查询:
  │           MATCH (n) RETURN n ORDER BY elementId(n)
  │           SKIP $offset LIMIT $batchSize
  │         → 每批封装为 FullSyncBatch → XADD to fullsync stream
  │
  ├── 5. RelationshipExporter.export()
  │     └── 同上，MATCH ()-[r]->() ...
  │
  ├── 6. 发布 FULL_SYNC_END 控制事件
  ├── 7. 恢复增量轮询（从 snapshotTimestamp 开始）
  └── 8. 记录全量同步完成的审计日志
```

全量同步参数：
- `fullsync.batchSize`: 1000（每批导出实体数）
- `fullsync.throttle`: 10ms（批次间间隔，避免压垮主节点）

## 4. 发布缓冲（PublishBuffer）

当 Redis 不可用时，事件暂存到本地文件：

```
/var/lib/neo4j-ha/buffer/
├── buffer-2026-04-10T12-00-00.jsonl
├── buffer-2026-04-10T12-00-05.jsonl
└── ...
```

- 每个文件最大 10MB，滚动创建
- Redis 恢复后自动回放缓冲文件
- 最多保留 100 个文件（约 1GB），超出告警
- 回放时保持原始顺序

## 5. 删除事件捕获（DeleteEventCapture）

**机制：** 不使用软删除。由 APOC Trigger（`phase: 'before'`）在节点/关系被真正删除前，将快照写入中转节点 `_CDCDeleteEvent`。CDC Collector 轮询该中转节点获取删除事件。

**APOC Trigger 安装（由 HA Agent 的 ApocTriggerInstaller 通过远程 Bolt 连接在主节点上执行）：**

- 安装命令在 `system` 数据库会话中执行（`SessionConfig.forDatabase("system")`）
- 触发器生效数据库由 `apoc.trigger.install($db, ...)` 的 `$db` 参数指定（当前为 `neo4j`）
- 因此：安装入口在 `system`，但增删改捕获动作实际运行在 `neo4j` 业务库
- Neo4j 启动初期如出现 `role=FOLLOWER`，HA Agent 会重试安装（10 次、每次间隔 3s）

需要安装 APOC Extended（提供 `apoc.trigger.toNode`）：
```bash
# Docker 方式
NEO4J_PLUGINS='["apoc", "apoc-extended"]'

# 手动方式
cp apoc-extended-2026.x.x.jar $NEO4J_HOME/plugins/
```

Trigger 注册（节点删除）：
```cypher
CALL apoc.trigger.install('neo4j', 'cdc-capture-node-deletes',
  'UNWIND $deletedNodes AS dn
   WITH apoc.trigger.toNode(dn, $removedLabels, $removedNodeProperties) AS node, dn
   WHERE NOT "_CDCDeleteEvent" IN apoc.node.labels(node)
   CREATE (:_CDCDeleteEvent {
     eventType: "NODE_DELETED",
     elementId: elementId(dn),
     labels: apoc.convert.toJson(apoc.node.labels(node)),
     properties: apoc.convert.toJson(apoc.any.properties(node)),
     timestamp: timestamp()
   })',
  {phase: 'before'})
```

> **关键设计约束：`_CDCDeleteEvent` 自排除过滤**
>
> 触发器 **必须** 包含 `WHERE NOT "_CDCDeleteEvent" IN apoc.node.labels(node)` 过滤条件。
> 否则当 `cleanupDeleteEvents()` 删除 `_CDCDeleteEvent` 中转节点时，删除触发器会为这些删除
> 再次创建新的 `_CDCDeleteEvent` 节点，形成**无限递归循环**，最终耗尽 Neo4j 事务内存
> （`dbms.memory.transaction.total.max`）导致 OOM。
>
> 同理，`cdc-timestamp` 触发器也需排除 `_CDCDeleteEvent` 节点，避免为中转节点设置
> `_created_at` / `_updated_at` 等系统属性（这些属性会让中转节点被增量轮询误捕获）。
>
> 此约束在 2026-04-15 修复 BUG-001 时引入，详见评审报告 §3.1。

Trigger 注册（关系删除）：
```cypher
CALL apoc.trigger.install('neo4j', 'cdc-capture-rel-deletes',
  'UNWIND $deletedRelationships AS dr
   WITH apoc.trigger.toRelationship(dr, $removedRelationshipProperties) AS rel, dr
   CREATE (:_CDCDeleteEvent {
     eventType: "REL_DELETED",
     elementId: elementId(dr),
     relType: apoc.rel.type(rel),
     properties: apoc.convert.toJson(apoc.any.properties(rel)),
     timestamp: timestamp()
   })',
  {phase: 'before'})
```

**降级方案（仅 APOC Core，不安装 Extended）：**

在 `phase: 'before'` 阶段节点尚未删除，原生 `labels()` 和 `properties()` 可能仍然可用：
```cypher
CALL apoc.trigger.install('neo4j', 'cdc-capture-deletes-fallback',
  'UNWIND $deletedNodes AS dn
   CREATE (:_CDCDeleteEvent {
     eventType: "NODE_DELETED",
     elementId: elementId(dn),
     labels: apoc.convert.toJson(labels(dn)),
     properties: apoc.convert.toJson(properties(dn)),
     timestamp: timestamp()
   })',
  {phase: 'before'})
```
> ⚠️ 降级方案需在 Neo4j 2026.2.3 上实际验证。

**CDC Collector 消费中转节点流程：**

```java
// DeleteEventCapture.captureDeleteEvents()
List<ChangeEvent> captureDeleteEvents(long lastTs, int batchSize) {
    // 1. 查询中转节点
    Result result = session.run("""
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp > $lastTs
        RETURN e ORDER BY e.timestamp ASC LIMIT $batchSize
        """, Map.of("lastTs", lastTs, "batchSize", batchSize));

    List<ChangeEvent> events = new ArrayList<>();
    List<String> processedIds = new ArrayList<>();

    for (Record record : result.list()) {
        Node e = record.get("e").asNode();
        events.add(ChangeEventBuilder.fromDeleteEvent(e));
        processedIds.add(elementId(e));
    }

    // 2. 发布到 Stream 成功后清理中转节点
    //    (清理在 publishBatch 成功之后执行，见主循环步骤 7)
    return events;
}

// 清理已消费的中转节点
void cleanupDeleteEvents(long publishedTs) {
    session.run("""
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp <= $publishedTs
        DETACH DELETE e
        """, Map.of("publishedTs", publishedTs));
}
```

**中转节点特性：**

| 属性 | 说明 |
|------|------|
| 标签 | `_CDCDeleteEvent`（前缀 `_` 表示系统内部使用） |
| 生命周期 | 极短（100ms ~ 几秒，下次 CDC 轮询即消费并清理） |
| 存储开销 | 可忽略（瞬态存在，不积累） |
| 查询影响 | 零（业务查询不涉及此标签） |
| 启动时清理 | CDC Collector 启动时扫描并处理所有残留中转节点 |
| 触发器自排除 | 所有 APOC 触发器必须排除 `_CDCDeleteEvent` 标签，防止递归死循环（BUG-001） |

## 6. 错误处理

| 场景 | 处理 |
|------|------|
| Neo4j 查询超时 | 重试1次，仍失败则跳过本轮，下轮继续 |
| Redis XADD 失败 | 写入 PublishBuffer，后台重试 |
| Fencing Token 不匹配 | 立即停止 CDC，通知 HA Agent 的 FailoverOrchestrator |
| Cypher 语法/运行时错误 | 记录错误日志，告警，不重试 |
| OOM | JVM 参数预防 + LRU Cache 容量限制 |
| 中转节点清理失败 | 记录日志，下次轮询时重试清理（幂等） |
| APOC Trigger 未安装 | 启动时检测，缺失则告警并拒绝启动 |

## 7. 索引依赖

CDC Collector 的轮询性能强依赖主节点上的索引。HA Agent 启动时，`IndexInstaller.ensureIndexes(primaryDriver)` 必须在 `CdcCollector.start(primaryDriver)` 之前执行。

**必需索引：**
- 每个业务标签：`CREATE RANGE INDEX ... FOR (n:Label) ON (n._updated_at)` — 增量轮询
- 每个关系类型：`CREATE RANGE INDEX ... FOR ()-[r:TYPE]-() ON (r._updated_at)` — 关系变更轮询
- `_CDCDeleteEvent`：`CREATE RANGE INDEX ... FOR (n:_CDCDeleteEvent) ON (n.timestamp)` — 删除事件轮询
- 每个业务标签：`CREATE RANGE INDEX ... FOR (n:Label) ON (n._elementId)` — keyset pagination 排序

详见架构文档 §4.5。

## 8. 性能预估

> 以下预估基于主节点已正确创建 `_updated_at` 和 `_elementId` 索引的前提。无索引时性能将退化 1-2 个数量级。

| 场景 | 变更速率 | 轮询延迟 | 说明 |
|------|---------|---------|------|
| 低负载 | < 100 events/s | ~100ms | 单次轮询即可覆盖 |
| 中等负载 | 100-1000 events/s | ~200ms | 增大 batchSize |
| 高负载 | 1000-5000 events/s | ~500ms | 减小 pollInterval + 增大 batchSize |
| 峰值 | > 5000 events/s | ~1s | 接近上限，考虑 Tx Log 方案 |
