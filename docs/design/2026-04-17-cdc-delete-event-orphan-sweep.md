# 2026-04-17 `_CDCDeleteEvent` orphan sweep

## 1. 目标

修复"跨 primary-tenure 的 `_CDCDeleteEvent` 永久残留"问题（内部编号 **BUG-067**，详情见
`docs/nuclear-fusion/design/modules/ha-agent-design.md` 即将新增章节）。

**业务症状**：上一轮 `docker kill` primary + restart 实验后，`standby-2` 本地图中发现 2 个
`:_CDCDeleteEvent` 节点，另外两个节点 0 个，`:TestNode` 三节点完全一致 (`2177 / 2177 /
2177`)。业务数据无损，但内部中转节点"泄漏"。

**不修会怎样**：

- 每一次 failover 或"短暂 promote + 回切"都可能留下数个残留
- 残留永远不会被任何进程清理（CDC Collector 只 sweep 自身所在 primary 的本地库）
- 长期运行下来会累积（单位可能是几十到几百），虽然每个节点都有业务数据几千倍于此，但属于**架构纪律破口**
- 测试脚本 `--clean-before-run` 只清 `:TestNode`，不清 `:_CDCDeleteEvent`，后续测试会被残留污染

## 2. 根因（单句定论）

`CdcCollector.cleanupDeleteEvents()` 的调用位点是 poll 循环内部
（`src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java:273-282`），
**只在"本轮 batch 有 delete 事件 (`maxDeleteTs > 0`) + publish 成功"后运行**。一旦在
"publish 成功 → cleanup 运行"之间被 `stop()` / crash / role 切换打断，剩余事件就成为
**孤儿**；且 cleanup 永远只扫当前 primary 的本地库，之前 tenure 在本地留下的事件**无人
负责**。

## 3. 修复方案（A + B）

### A. 测试脚本扩展 `--clean-before-run` 覆盖 `_CDCDeleteEvent`

#### 目的

让自动化测试在启动前**清扫三节点本地**的 `:_CDCDeleteEvent`，把历史实验污染挡在测试
之外。单独对 primary 清扫不够 —— 残留就是在 standby 本地，HAProxy 写通道到不了。

#### 文件 & 行号

- `scripts/deploy/ha-load-switchover-test.py`：在 `--clean-before-run` 现有逻辑
  （~1009-1026 行，`:TestNode` 清理块）之后**追加**一段：遍历 `DEFAULT_NODES` 字典
  （L89-L93），每个节点用 bolt 直连（绕过 HAProxy），跑
  `MATCH (e:_CDCDeleteEvent) DETACH DELETE e RETURN count(*)`。

#### 行为

- 直连每个 Neo4j 实例（`DEFAULT_NODES[*]` 的 bolt URI）
- 对每个节点跑一次 `MATCH (e:_CDCDeleteEvent) DETACH DELETE e`
- **任意节点**上发现非零残留时 log WARN（把它当成"上次运行 + BUG-067 漏洞"的证据）
- 单个节点连接失败不阻塞整体测试（log 后跳过）

#### 错误处理

- 某节点不可达 → log warn，跳过该节点
- 整体异常 → `try/except` 沿用现有 pattern，测试继续

### B. HA Agent 启动 sweep：primary 身份获取时清扫本地旧 tenure 残留

#### 目的

在当前节点**成为 primary**（`CdcCollector.start()` 被调用）时，**首次 poll 之前**
对本地库做一次无条件 sweep：`DELETE all _CDCDeleteEvent WHERE timestamp < sweepCutoff`。

这条防线是**自愈性的**：

- 上一次 tenure 因为 crash/stop/role-shift 留下的残留 → 本次成为 primary 时清掉
- 即使有 A 漏掉的实验污染 → 下次任何 primary 转移后彻底消失

#### 文件 & 行号

- `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java`：在 `start()`
  方法 (L67) 内，**在 `pollingStrategy` 构造之后 (L79)、`scheduler` 启动之前 (L104)** 插入一块
  sweep 逻辑，复用现有 `DeleteEventCapture.cleanupDeleteEvents()`
  （`src/cdc-collector/src/main/java/com/neo4j/ha/cdc/capture/DeleteEventCapture.java:73-80`）。

#### 行为

```text
sweepCutoffMs = System.currentTimeMillis() - SWEEP_SAFETY_WINDOW_MS
  # SWEEP_SAFETY_WINDOW_MS = 5_000L (5s)
try (Session s = currentDriver.session(...)) {
    long swept = deleteCapture.cleanupDeleteEvents(s, sweepCutoffMs);
    if (swept > 0) {
        log.warn("Startup sweep removed {} orphan _CDCDeleteEvent(s) " +
                 "left over from a previous primary tenure (BUG-067)", swept);
    }
}
```

- 5 秒安全窗口的理由：trigger 在 `phase:'before'` 跑，`timestamp()` 是 trigger 执行
  时间；从 trigger 执行到 commit 到 query visibility，最长不超过秒级。5s 的缓冲**一定**
  不会误杀当前 tenure 真正在飞的事件。
- sweep 前日志写 INFO，sweep 非 0 结果写 WARN（触发告警路径）。

#### 错误处理

- sweep 查询抛异常 → log ERROR，但**不阻塞 CDC Collector 启动**（继续执行后续 scheduler 启动）。
  理由：sweep 是"最佳努力的垃圾回收"，失败了由 poll 循环内的 cleanup 兜底；启动失败的
  后果严重得多（HA 无法切换），不值得把启动绑死在 sweep 上。

#### 并发/时序安全性论证

- `start()` 的调用者（`FailoverOrchestrator` 或 `HaAgentBootstrap`）保证在此之前：
  - APOC trigger 已安装 (`ApocTriggerInstaller.installAll()` 先于 `cdcCollector.start()`)
  - 新 primary 已经可以接受写（`blockWrites` 已解除）
- 因此 sweep 跑时：
  - 任何新产生的 `_CDCDeleteEvent` 都会有 `timestamp >= now() - 1ms`，远大于 cutoff
  - 旧 tenure 残留的 `_CDCDeleteEvent` 必然 `timestamp < now() - 5s`（因为至少经历了
    P1-P10 的状态迁移，通常已经过去 10-30s）

故 5s 安全窗口**既不误杀，也一定命中残留**。

## 4. 非目标

- **不**触及 `cleanupDeleteEvents` 本身的语义（仍然是 `WHERE timestamp <= publishedTs`）
- **不**添加新的 Cypher 查询或 schema
- **不**改动 `stop()` 的 drain 逻辑 —— drain 已经能在 graceful 停止路径上捕获大多数事件，
  这次只加**自愈兜底**，不改主流程
- **不**改 standby 上的任何行为 —— sweep 只在成为 primary 时跑

## 5. 数据/接口变更

| 维度 | 变更 | 说明 |
|------|------|------|
| Schema | 无 | `_CDCDeleteEvent` 结构不变 |
| Cypher | 复用 `CLEANUP_QUERY` (`DeleteEventCapture.java:32-38`) | 只是多一次调用 |
| 对外 API | 无 | 不影响 `/cluster/*` 任何端点 |
| checkpoint 格式 | 无 | 不碰 `CdcCheckpoint` |
| Metric | （可选）`cdc_startup_sweep_count` | 取决于要不要暴露，当前 scope 不加 |

## 6. 测试计划

### 6.1 回归测试
- 手动清掉 standby-2 上现存的 2 个残留（`MATCH (e:_CDCDeleteEvent) DETACH DELETE e`）
- 重跑上一次的 kill/restart 实验
- 验证：
  - 三节点上 `_CDCDeleteEvent` 数量在 post-quiet 结束后都 = 0
  - 新 primary 的日志里能看到 `Startup sweep removed N orphan...`（如果确实有的话）

### 6.2 新 primary 启动无残留时
- 预期：sweep 运行一次，删除 0 条，不打 WARN
- 验证：日志 level INFO / DEBUG，无噪声

### 6.3 `--clean-before-run` 覆盖验证
- 人为在某个 standby 上手动 `CREATE (:_CDCDeleteEvent {timestamp: timestamp()})`
- 跑 `ha-load-switchover-test.py --clean-before-run --skip-switchover`
- 验证：测试开始前这个节点上的 `_CDCDeleteEvent` 已经 = 0

## 7. 开放问题

无。A + B 互补：A 是测试基础设施的安全网，B 是产品路径的自愈。用户方案确认 → 可直接
落地。

## 8. 实施顺序

1. `CdcCollector.java`：加 sweep 代码块
2. `ha-load-switchover-test.py`：扩展 `--clean-before-run`
3. `ha-agent-design.md`：记录为 BUG-067，含根因、修复点、cross-ref

## 9. 回溯矩阵

| 设计条目 | 代码位置 |
|---------|---------|
| §3.A 直连 + sweep `_CDCDeleteEvent` | `scripts/deploy/ha-load-switchover-test.py` `--clean-before-run` 块 |
| §3.B `CdcCollector.start()` sweep | `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java:start()` |
| §3.B 常量 `SWEEP_SAFETY_WINDOW_MS = 5_000L` | 同上 |
| §6 测试计划 | 手动执行，不写新 test |
| §8 文档 | `docs/nuclear-fusion/design/modules/ha-agent-design.md` 新增 BUG-067 |
