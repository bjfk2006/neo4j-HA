# Neo4j HA Code Review Report — 2026-04-15

> 评审日期: 2026-04-15
> 评审范围: 全部 Java 源文件 + 部署脚本 vs 设计文档一致性 + 代码 Bug
> 评审结论: **REQUEST CHANGES → 已修复**

---

## 1. 评审范围

- **变更来源：** main 分支全量代码（含 BUG-001 ~ BUG-006 修复）
- **变更类型：** 设计合规性 + 代码质量 + 安全性
- **涉及文件：** 12 个核心 Java 源文件 + 3 个脚本/配置文件 + 6 个设计文档

## 2. 评估结论

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| Critical | 2 | 0 |
| Major | 4 | 0 |
| Minor | 4 | 0 |
| Info | 2 | 2（保留为参考） |
| **结论** | REQUEST CHANGES | **APPROVE with suggestions** |

## 3. 问题清单与修复记录

### Critical — 已修复

| # | 文件 | 问题 | BUG-ID | 修复方案 |
|---|------|------|--------|---------|
| C1 | `HaAgent.java` | serviceState 评估使用全局共享的单一 `stableSinceMs` 变量追踪所有备节点稳定时间 | BUG-007 | 改为 `Map<String, Long> stableSinceByNode`（ConcurrentHashMap），每节点独立计时 |
| C2 | `HaAgent.java` | serviceState 评估使用全局单一 `metrics.syncLagMs` 替代各节点独立 checkpoint lag | BUG-007 | 改为 `checkpointManager.loadSyncCheckpoint(nodeId)` 按节点读取独立 lag |

### Major — 已修复

| # | 文件 | 问题 | BUG-ID | 修复方案 |
|---|------|------|--------|---------|
| M1 | `CdcCollector.java` + `StreamPublishService.java` + `StreamPublisher.java` | saveCheckpoint 始终传 null 作为 lastStreamId，且重启时未从 checkpoint 恢复 | BUG-008 | `publishBatch()` 返回最后 Stream ID 并逐层透传；`start()` 恢复 checkpoint 时同步恢复 `lastStreamId` |
| M2 | `SyncApplier.java` | `drainPending()` 硬编码 `sleep(2000)` 不保证排空 | BUG-009 | 新增 `processing` 标志，轮询等待当前批次完成，超时 30s |
| M3 | `FailoverOrchestrator.java` | `tryCleanupOldPrimary` 清理 `_CDCDeleteEvent` 未分批 | BUG-010 | 改用 `LIMIT 10000` 循环删除，与设计文档 §12.2 一致 |
| M4 | `FailoverOrchestrator.java` | `executeSwitchover` 未卸载旧主 APOC Trigger | BUG-011 | 添加 `ApocTriggerUninstaller.uninstall()` + 分批清理 `_CDCDeleteEvent` |

### Minor — 已修复

| # | 文件 | 问题 | 修复方案 |
|---|------|------|---------|
| m1 | `ApocTriggerInstaller.java` | Cypher 关键字 `With`/`WITH` 大小写不一致 | 统一为 `WITH`（BUG-012） |
| m2 | `docker/.env.example` | 示例密码看起来像真实密码 | 改为 `CHANGE_ME_*` 占位符 |
| m3 | `ha-smoke-test.sh` | Switchover 后探测节点清理遗漏 | 合并 switchover 前后所有 host，去重后清理 |
| m4 | `FailoverOrchestrator.java` | Switchover 未卸载旧主 Trigger | 同 M4，已修复 |

### Info — 保留参考

| # | 文件 | 说明 |
|---|------|------|
| I1 | `docker/init.sh:22` | `COMPOSE_HA_AGENT_REDIS_HOST` 硬编码，仅作警告提示，不影响功能 |
| I2 | `IndexManager.java:54` | `ensureIndexesForAllLabels` 跳过 `_` 前缀标签，`_CDCDeleteEvent` 的索引由 `IndexInstaller` 负责，职责划分合理 |

## 4. 设计文档一致性修复后状态

| 维度 | 修复前 | 修复后 |
|------|--------|--------|
| APOC Trigger (BUG-001) | 一致 | 一致 |
| 索引创建 (BUG-002) | 一致 | 一致 |
| Fencing Token (BUG-003) | 一致 | 一致 |
| standbyDrivers (BUG-004) | 一致 | 一致 |
| auto-bump (BUG-005) | 一致 | 一致 |
| **serviceState 评估** | **不一致** | **一致** — per-node lag + per-node timer |
| **CDC checkpoint lastStreamId** | **不一致** | **一致** — 保存实际 Stream ID |
| **Switchover 旧主降级** | **不一致** | **一致** — 卸载 Trigger + 清理 |
| **tryCleanupOldPrimary 分批** | **不一致** | **一致** — LIMIT 10000 循环 |
| drainPending 实现 | 弱实现 | 基于状态标志等待 |

## 5. 修改文件清单

| 文件路径 | 修改类型 |
|---------|---------|
| `src/ha-agent/src/main/java/com/neo4j/ha/agent/HaAgent.java` | C1, C2: per-node 评估 |
| `src/ha-agent/src/main/java/com/neo4j/ha/agent/failover/FailoverOrchestrator.java` | M3: 分批清理; M4/m4: switchover trigger 卸载 |
| `src/ha-agent/src/main/java/com/neo4j/ha/agent/bootstrap/ApocTriggerInstaller.java` | m1: Cypher 关键字统一 |
| `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/CdcCollector.java` | M1: lastStreamId 保存 |
| `src/cdc-collector/src/main/java/com/neo4j/ha/cdc/publish/StreamPublishService.java` | M1: publishBatch 返回 streamId |
| `src/common/src/main/java/com/neo4j/ha/common/redis/StreamPublisher.java` | M1: publishBatch 返回 streamId |
| `src/sync-applier/src/main/java/com/neo4j/ha/sync/SyncApplier.java` | M2: drainPending 正确实现 |
| `docker/.env.example` | m2: 密码占位符 |
| `scripts/deploy/ha-smoke-test.sh` | m3: 完善清理逻辑 |
| `src/sync-applier/src/main/java/com/neo4j/ha/sync/consumer/IncrementalConsumer.java` | BUG-013: 移除全局 syncLagMs 写入 |
| `docs/nuclear-fusion/design/modules/ha-agent-design.md` | 追加 BUG-007 ~ BUG-014 修复记录 |

## 6. 多备节点（>=2 standby）专项分析

> 分析日期: 2026-04-15
> 分析范围: 数据同步 / 主备切换 / 同步暂停全量备份

### 6.1 数据同步 — 已修复

| # | 严重级别 | 问题 | BUG-ID | 状态 |
|---|---------|------|--------|------|
| MS1 | Critical | `SyncApplier` 共享单一 `DuplicateDetector`，第二个 standby 的事件被误判为重复而跳过 | — | 已修复：per-node component maps |
| MS2 | Critical | `SyncApplier` 共享单一 `FullSyncReceiver`，全量同步状态跨节点污染 | — | 已修复：per-node `fullSyncReceivers` map |
| MS3 | Major | `IncrementalConsumer` 写入全局 `metrics.syncLagMs`，最后处理的节点覆盖前面节点的值 | BUG-013 | 已修复：改为 SyncApplier 层取 MAX lag |

**修复方案：** `SyncApplier` 完整重写，所有有状态组件（ChangeApplier, FencingTokenFilter, DuplicateDetector, OrderValidator, IncrementalConsumer, FullSyncConsumer, FullSyncReceiver）均改为 `Map<String, T>` per-node 持有。`metrics.syncLagMs` 移至 consumeLoop 层统一计算 MAX。

### 6.2 Failover/Switchover — 正确

| 维度 | 分析结论 |
|------|---------|
| **StandbySelector.selectBest()** | 正确：从 `getStandbyNodes()` 筛选 HEALTHY+ONLINE 候选，按 checkpoint `lastEventTs` 降序排列取最优。>=2 standby 时正确选择延迟最小的节点 |
| **executeFailover Phase 7b** | 正确：promoted node 角色已更新为 PRIMARY，failed node 为 DOWN。`getStandbyDrivers()` 按 `NodeRole.STANDBY` 过滤，仅返回剩余未参与切换的 standby |
| **executeSwitchover** | 正确：old primary → STANDBY，new primary → PRIMARY。`getStandbyDrivers()` 返回 old primary + 其余 standby（不含 new primary），SyncApplier 正确为所有新 standby 建立同步 |
| **ClusterStateManager.getStandbyDrivers()** | 正确：实时按 `NodeRole.STANDBY` 过滤，不硬编码节点列表 |

**结论：** Failover 和 Switchover 流程在 >=2 standby 场景下无代码缺陷。

### 6.3 同步暂停全量备份 — 已知限制

| # | 严重级别 | 问题 | BUG-ID | 状态 |
|---|---------|------|--------|------|
| MS4 | Minor | `BackupCoordinator.prepare(nodeId)` 接收节点 ID 但调用 `syncApplier.pause()` 暂停全部节点同步 | BUG-014 | Tech Debt |

**分析：** `BackupCoordinator.prepare()` 旨在暂停同步以获取一致性快照。当前 `SyncApplier.pause()` 为全局标志，暂停后所有 standby 的消费循环均停止。在 >=2 standby 场景下，备份 standby-A 会导致 standby-B 的同步也被暂停，增加 standby-B 的 lag。

**影响评估：** 备份窗口通常为分钟级，暂停其他 standby 的同步不会导致数据不一致，仅增加临时 lag。这与 `consumeLoop` 单线程串行设计是同一根因——需要 per-node 消费线程才能实现 per-node pause。

**当前状态：** 标记为 Tech Debt，与下方遗留建议 M4 一致。

### 6.4 多备节点分析总结

| 功能 | >=2 standby 正确性 | 说明 |
|------|-------------------|------|
| 增量数据同步 | **已修复** | SyncApplier per-node 重写 + metrics MAX lag |
| 全量同步接收 | **已修复** | per-node FullSyncReceiver |
| Failover | **正确** | StandbySelector + getStandbyDrivers 均按角色动态过滤 |
| Switchover | **正确** | 角色更新后 driver map 自动正确 |
| 备份暂停同步 | **已知限制** | 全局 pause 影响所有 standby，Tech Debt |

## 7. 增量回归修复（2026-04-16）

| # | 严重级别 | 问题 | 修复状态 |
|---|---------|------|---------|
| BUG-016 | Critical | `HaAgent.evaluateServiceStates()` 采用 `now - standby.lastEventTs` 口径，在低写入时会把已追平 standby 误判为高 lag，导致无法进入 ONLINE，switchover 被阻塞 | 已修复：改为 `primaryCdc.lastTs - standbySync.lastEventTs` |
| BUG-017 | Major | `ha-smoke-test.sh` 未等待目标 standby 进入 ONLINE 即发起 switchover，导致测试误报失败 | 已修复：新增 ONLINE 轮询等待逻辑 |
| BUG-018 | Critical | `SyncApplier.start()` 索引确保和 pending 恢复未容忍不可达 standby，任一节点 DOWN 导致 Agent 启动崩溃 | 已修复：per-node try-catch 隔离，跳过不可达节点 |
| BUG-019 | Critical | `XREADGROUP BLOCK` 与非阻塞操作共用 JedisPool + `testOnReturn=true`，Redis 瞬时抖动时连接销毁导致全线程超时 | 已修复：阻塞读独立池隔离 + 移除 testOnReturn + testWhileIdle 后台驱逐 |
| BUG-020 | Critical | HAProxy 无法解析 standby DNS 时无退避崩溃循环 + Docker `unless-stopped` 无上限重启，CPU/IO 饱和导致 OS 卡死 | 已修复：haproxy.cfg 增加 `init-addr none` + compose 改为 `on-failure:5` + 解除 standby 硬依赖 |

**补充说明：**
- BUG-016 修复后，serviceState 判定与设计文档"主备同步差值"语义一致；
- BUG-017 修复后，脚本对 `stableDuration` 窗口更加鲁棒，能避免切换前短时状态抖动导致的假失败；
- BUG-018 修复后，Agent 可在部分 standby 不可达时正常启动，不可达节点恢复后由消费循环自动接管同步；
- BUG-019 修复后，阻塞读与共享池彻底隔离，Redis 抖动不再级联影响 checkpoint、registry、metrics 等关键操作；
- BUG-020 修复后，standby 不可达时 HAProxy 正常启动（方案 A），即使 init-addr 失效崩溃循环也最多 5 次后停止（方案 B）。

## 8. 遗留建议（M4 — 多备节点并行消费）

当前 `SyncApplier.consumeLoop` 单线程串行处理多个 standby 节点。在当前双节点部署（1 primary + 1 standby）场景下无影响。若未来扩展到多 standby，建议为每个 standby 分配独立消费线程，同时支持 per-node pause/resume。此项标记为 **Tech Debt**，优先级 Low，不阻塞当前发布。
