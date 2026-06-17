# 2026-06-17 BUG-087：批量删除超出 batchSize 时 DELETE 事件被静默丢失

- 日期：2026-06-17
- 状态：已修复
- 影响模块：`cdc-collector`
- 关联：[[BUG-067]] orphan sweep、[[BUG-057]] 三独立游标、[[BUG-052/082]] elementId 匹配

## 1. 业务症状

实际运行中（**无 failover、无备节点延迟**）出现主备数据对不上：**备节点 > 主节点**，
表现得像「DELETE 操作没有同步」—— 备节点保留了主节点已经删除的数据。

## 2. 根因（单句定论）

`_CDCDeleteEvent` 中转节点的**捕获**按 keyset 分页（`LIMIT batchSize`），而**发布后清理**
按时间戳阈值整段删除（`WHERE timestamp <= maxDeleteTs`）。两端语义不对称：当**同一时间戳**
上的删除标记数量超过一个批次能捕获的数量时，清理会把「尚未捕获、尚未发布」的同时间戳溢出
标记一并删除，这些 DELETE 永远不会进入 Redis Stream，备节点收不到 → 备 > 主。

### 为什么会有大量「同一时间戳」的标记

删除触发器（`ApocTriggerInstaller` 的 `NODE_DELETE_TRIGGER` / `REL_DELETE_TRIGGER`）用
`timestamp: timestamp()` 给每个标记打时间戳。Cypher 的 `timestamp()` **在单个事务内是常量**。
因此一次 `MATCH (n:Foo) DETACH DELETE n`（或单个高连接度节点的 `DETACH DELETE`）会产生
**N 个时间戳完全相同**的 `_CDCDeleteEvent`。只要 N > `cdc.poll.batchSize`（默认 500），
边界必被切断。

### 触发时序（batchSize=500，单事务删 600 个实体）

| 步骤 | 行为 | 结果 |
|---|---|---|
| Poll1 捕获 | `ORDER BY (ts, eid) LIMIT 500` | 取到 600 中前 500，全部 ts=T |
| Poll1 清理 | `DELETE WHERE timestamp <= T` | **删掉全部 600**，含从未捕获的 100 个 |
| Poll1 游标 | 推进到 (T, eid_500) | |
| Poll2 捕获 | `WHERE ... eid > eid_500` | 第 501..600 已被物理删除 → 查不到 |

净效果：第 501..600 个删除从未发布 → 备节点保留这 100 个实体。

### 附带发现：删除游标 keyset 本身不一致

捕获查询原先按 `elementId(e)`（中转节点自身 Neo4j id）分页/排序，但游标
（`PollingState.lastDeleteEid`）存的、以及全局排序用的 `RawChange.elementId` 是
**被删实体的 `_elementId` 属性**（`e.elementId`）。两者是不同的 id，keyset 比较
`elementId(e) > $lastDeleteEid` 实际是「中转 id 比实体 id」，语义错误。原先被激进的
`timestamp <=` 清理掩盖（每轮把同时间戳的都删光，tie-break 分支几乎用不到）。

## 3. 修复方案

把删除路径的 **捕获分页、游标推进、清理边界** 三者统一到同一个 keyset
`(timestamp, e.elementId)`（被删实体的 `_elementId` 属性，正是 `RawChange.elementId`）：

1. **捕获**（`DeleteEventCapture.CAPTURE_QUERY`）：keyset 与 `ORDER BY` 从 `elementId(e)`
   改为 `e.elementId`，与游标一致。
2. **清理**（新增 `DeleteEventCapture.cleanupCapturedDeleteEvents`）：只删本批捕获边界
   `(maxTs, lastEid)` 以内的标记 —— `timestamp < maxTs OR (timestamp = maxTs AND
   e.elementId <= lastEid)`。同时间戳、`e.elementId > lastEid` 的溢出**保留**，下一轮捕获。
3. **调用点**（`CdcCollector.pollLoop`）：发布成功后，用本批最后一个 DELETED 记录的
   `(timestamp, elementId)` 同时作为清理边界与游标推进值。

> 保留旧的 `cleanupDeleteEvents(timestamp <= cutoff)`，它仍服务于 [[BUG-067]] 的启动 sweep
> （清理「上一轮 tenure 残留的孤儿」语义正确，与发布无关）。

### 幂等性论证

DELETE 在备节点按 `_elementId` 匹配执行（`MATCH (n {_elementId}) DETACH DELETE n`），
重复应用幂等。即使极端情况下两个标记的 `(timestamp, e.elementId)` 完全相同（如 rel-id
复用导致同毫秒重复删除），丢失的也只是「对同一实体的重复删除」，备节点已被首个事件删除，
无数据影响。

## 4. 非目标

- 不改 `RawChange` 契约、不改 schema、不改触发器。
- 不改 sync-applier 应用端（本漏洞在「根本没发出去」之前）。
- 不处理 PEL/Stream 裁剪导致的 DELETE 丢失（备节点严重落后场景，已由 `PendingRecovery`
  打 ERROR + 人工 fullsync 兜底，属另一条已知、需运维介入的路径）。

## 5. 回溯矩阵

| 设计条目 | 代码位置 |
|---|---|
| §3.1 捕获 keyset 统一到 e.elementId | `DeleteEventCapture.CAPTURE_QUERY` |
| §3.2 有界清理 | `DeleteEventCapture.cleanupCapturedDeleteEvents` + `CLEANUP_CAPTURED_QUERY` |
| §3.3 调用点 | `CdcCollector.pollLoop()` 删除游标块 |
| 旧 sweep 保留 | `DeleteEventCapture.cleanupDeleteEvents`（BUG-067） |
