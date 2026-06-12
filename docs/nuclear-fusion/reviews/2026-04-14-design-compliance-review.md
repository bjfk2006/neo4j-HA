# Neo4j 社区版主备高可用系统 — 代码与设计合规性评审报告

> 评审日期: 2026-04-14
> 评审模式: Nuclear Fusion Code Review — 设计合规性评审
> 评审文档:
>   - `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md` (v2.2)
>   - `docs/nuclear-fusion/design/modules/common-design.md`
>   - `docs/nuclear-fusion/design/modules/cdc-collector-design.md`
>   - `docs/nuclear-fusion/design/modules/sync-applier-design.md`
>   - `docs/nuclear-fusion/design/modules/ha-agent-design.md`
>   - `docs/nuclear-fusion/requirements/2026-04-10-neo4j-ha-requirements.md`
> 评审结论: **REQUEST CHANGES（原始评审结论）**

---

## 1. 评审范围

- **变更来源：** 全部代码实现（85 个 Java 源文件 + 配置文件）
- **变更类型：** 完整系统实现（Full Pipeline Phase 4 产出）
- **涉及模块：** common (31 files) / cdc-collector (15 files) / sync-applier (16 files) / ha-agent (23 files)
- **配置文件：** ha-agent.yml / haproxy.cfg / docker-compose.yml / pom.xml (4 files)
- **测试文件：** 18 个单元测试

## 2. 变更意图理解

本项目在 Neo4j 社区版之上构建旁路主备同步层，通过 CDC Cypher 轮询 → Redis Stream → Sync Applier 回放实现准实时数据复制，配合健康检查、自动 Failover、HAProxy 路由管理提供高可用能力。代码是根据 Full Pipeline Phase 2-3 产出的设计文档实现的。

## 3. 评估结论

- **结果：REQUEST CHANGES**
- Critical: 3 | Major: 12 | Minor: 9 | Info: 5
- **亮点：** 见第 4 节

### 3.1 状态更新（与当前代码对齐）

以下问题已在后续修复中完成：

- Critical: **C1 / C2 / C3**
- Major: **M1 / M2 / M5 / M6 / M7**

同时，源码目录中的历史空目录 `src/failover-manager` 与 `src/client-router` 已删除，当前 `src/` 仅保留 `common` / `cdc-collector` / `sync-applier` / `ha-agent` 四个模块目录。

### 3.2 运行期间发现的新问题（2026-04-15）

以下问题在集成测试（ha-smoke-test.sh）过程中发现并修复：

| # | 严重度 | 模块 | 问题描述 | 修复状态 |
|---|--------|------|---------|---------|
| BUG-001 | **Critical** | cdc-collector + ha-agent | **APOC 删除触发器递归死循环。** `cdc-capture-node-deletes` 触发器未排除 `_CDCDeleteEvent` 标签，导致清理中转节点时递归创建新中转节点，最终 Neo4j OOM（`dbms.memory.transaction.total.max`）。 | ✅ 已修复 — 触发器添加 `WHERE NOT "_CDCDeleteEvent" IN ...` 过滤 |
| BUG-002 | **Major** | sync-applier | **IndexManager 会话冲突。** `ChangeApplier` 在 `executeWrite` 事务内部调用 `indexManager.ensureIndex(session, ...)`，Neo4j 不允许同一 session 同时执行 DDL 和 DML。 | ✅ 已修复 — 索引创建移到事务开始之前执行 |
| BUG-003 | **Critical** | cdc-collector | **Switchover 后 Fencing Token 未同步到 StreamPublishService。** `CdcCollector.start()` 和 `switchTarget()` 未传递新 token，导致发布服务使用旧 token 被 Redis 拒绝。 | ✅ 已修复 — `start()` 和 `switchTarget()` 中显式同步 token |

> 注：本报告第 5-8 节保留的是 2026-04-14 当日的原始评审内容，便于追溯。以上"状态更新"反映当前仓库实际状态。详细修复记录见 `ha-agent-design.md` §15。

## 4. 设计亮点（代码中做得好的地方）

1. **模块划分清晰** — common / cdc-collector / sync-applier / ha-agent 四模块的包结构与设计文档高度吻合，职责边界明确
2. **Keyset Pagination 正确实现** — CDC 轮询使用 `(_updated_at, _elementId)` 复合游标，节点/关系/删除事件三路查询均正确实现了无间隙分页
3. **Fencing Token 机制完整** — 从 `FencingTokenManager` (Redis INCR) → `StreamPublisher` (Lua 原子校验) → `FencingTokenFilter` (消费端过滤) 全链路实现
4. **APOC Trigger 安装/卸载完备** — `ApocTriggerInstaller` 安装 3 个 Trigger，`ApocTriggerUninstaller` 幂等卸载，`OldPrimaryRecovery` 中正确调用
5. **防误切机制齐全** — `confirmationWait`、`minIntervalMs`、`maxAutoPerHour` 三重保护与设计一致
6. **HAProxy 多活架构实现** — `HaProxyUpdater` 遍历所有实例 + `HaProxyStateSyncer` 定期同步，与 v2.1 设计一致

---

## 5. 问题清单

### Critical — 必须修复

| # | 模块 | 文件:位置 | 问题描述 | 修复建议 |
|---|------|----------|---------|---------|
| C1 | ha-agent | `failover/FailoverOrchestrator.java` executeFailover() | **Failover 后 SyncApplier 未重启。** `executeFailover()` 在 Phase 3 停止了 `syncApplier.stop()`，但后续从未调用 `syncApplier.start()`。这意味着自动 Failover 后**数据同步永久中断**，新主产生的变更无法复制到任何备节点。而 `executeSwitchover()` 中正确调用了 `syncApplier.start()`，说明这是遗漏。 | 在 Phase 5（CDC 切换目标并启动后）添加 `syncApplier.start(newStandbyDrivers)` 调用。需要确定 Failover 后的备节点列表（原主可能已宕机，其余 Standby 继续作为备）。参考 `executeSwitchover()` 的实现。 |
| C2 | cdc-collector | `publish/PublishBuffer.java` | **PublishBuffer 无磁盘回放能力 — Redis 恢复后缓冲数据丢失。** 设计要求"Redis 恢复后自动回放缓冲文件"。实现中 `flushToFile()` 将事件写入 `.jsonl` 文件，但 `drain()` 只从内存 `memoryBuffer` 读取，**没有任何代码从磁盘文件回读**。如果 HA Agent 在 Redis 不可用期间重启，内存缓冲丢失，磁盘文件也不会被回放。这违反设计 §4 "Redis 恢复后自动回放缓冲文件"和架构文档 §12.5 "CDC Collector 将事件暂存到本地 PublishBuffer（文件）；Redis 恢复后自动回放缓冲"。 | 实现 `replayFromDisk()` 方法：按文件时间顺序读取 `.jsonl`，反序列化后通过 `StreamPublisher` 重新发布，发布成功后删除文件。在 `StreamPublishService` 检测到 Redis 恢复时调用。 |
| C3 | cdc-collector | `CdcCollector.java` pollLoop + `publish/StreamPublishService.java` | **删除事件中转节点在 Redis 发布失败时仍被清理。** `StreamPublishService.publishBatch()` 捕获异常后写入 `PublishBuffer` 且不抛出异常。`CdcCollector.pollLoop` 随后无条件执行 `DeleteEventCapture.cleanupDeleteEvents()`。设计明确要求"发布到 Stream 成功后清理"。如果 Redis 持续不可用且 PublishBuffer 磁盘回放未实现（C2），清理后的删除事件将**永久丢失**。 | `StreamPublishService.publishBatch()` 应返回发布结果（成功/缓冲）。`pollLoop` 仅在返回"成功"时清理中转节点。缓冲模式下保留中转节点，待回放成功后再清理。 |

### Major — 强烈建议修复

| # | 模块 | 文件:位置 | 问题描述 | 修复建议 |
|---|------|----------|---------|---------|
| M1 | ha-agent | `backup/BackupCoordinator.java` + `HaAgent.java` | **备份超时安全阀未接线。** `BackupCoordinator.checkTimeout()` 存在但**从未被任何调度器或定时任务调用**。设计 §13.2 要求"超过 maxBackupDuration（2h）自动恢复 Sync Applier"。如果运维调用 `/backup/prepare` 后忘记 `/backup/complete`，Sync Applier 将永久暂停。 | 在 `HaAgent` 启动时为 `BackupCoordinator.checkTimeout()` 注册一个 `ScheduledExecutorService.scheduleAtFixedRate(60s)` 定时任务。 |
| M2 | ha-agent | `failover/FailoverOrchestrator.java` executeFailover() | **Failover 未取消进行中的备份。** 设计 §13.3 明确要求"备份期间如果主节点故障需要 Failover，HA Agent 将自动取消备份"。`BackupCoordinator.cancelForFailover()` 方法已实现，但 `executeFailover()` 未调用它。 | 在 `executeFailover()` Phase 3（停止同步）之前添加 `backupCoordinator.cancelForFailover()` 调用。 |
| M3 | ha-agent | `lifecycle/GracefulShutdown.java` | **优雅关闭缺少多个设计要求的步骤。** 设计 §10 要求：停止 HealthChecker → 等待 CDC/Sync 完成当前批次(30s) → 保存 Checkpoint → 停止 AdminHttpServer → 关闭连接 → 更新 node-registry 标记离线。实现仅停止 CDC/Sync + 关闭连接，缺少：HealthChecker 停止、AdminHttpServer 停止、显式 Checkpoint 保存、node-registry 离线标记。 | 扩展 `GracefulShutdown` 构造函数，注入 `HealthChecker`、`AdminHttpServer`、`NodeRegistry`。按设计顺序依次执行关闭操作。 |
| M4 | ha-agent | `health/HealthChecker.java` | **健康检查未实现分层间隔。** 设计 §5 要求 L1(1s) / L2(2s) / L3(5s) / L4(10s) 不同间隔。实现使用单一 `intervalMs` 驱动所有层级。且 SUSPECT 状态下未实现"增加检查频率"。 | 为每个层级维护独立的计时器，按各自间隔触发。SUSPECT 状态时将间隔减半。 |
| M5 | ha-agent | `lifecycle/ClusterStateManager.java` + `routing/HaProxyUpdater.java` | **ServiceState 自动化联动未实现。** 设计 §3 要求 `syncLagMs < syncLagThreshold` 且持续 `stableDuration` 后自动从 SYNCING → ONLINE，并联动 HAProxy 读 backend。代码中 `ClusterStateManager` 只是存取字段，无评估循环。`HaProxyUpdater.enableReadBackend()` / `disableReadBackend()` 已实现但**未被任何代码调用**。 | 在 `HaAgent` 中添加定时任务（每 1s），遍历所有 STANDBY 节点，检查 `syncLagMs` 和 `stableDuration`，满足条件时调用 `setServiceState(ONLINE)` + `haProxyUpdater.enableReadBackend()`。 |
| M6 | sync-applier | `applier/RelationshipApplier.java` | **关系回放未使用标签索引。** 设计 §3.2 的 `REL_MERGE` 模板要求 `MATCH (a:%1$s {_elementId: ...})`（带起点/终点标签），以命中标签属性索引。但 `RelationshipApplier` 使用 `MATCH (a {_elementId: ...})`（无标签），导致每次关系回放都是**全表扫描**。`CypherTemplates.REL_MERGE` 模板本身是正确的但未被使用。 | 修改 `RelationshipApplier` 使用 `CypherTemplates.REL_MERGE`。需要在 `EntityData` 或 `ChangeEvent` 中携带起点/终点的标签信息（当前 `EntityData` 只有 `labels` 字段用于自身标签，缺少 startNodeLabels/endNodeLabels）。 |
| M7 | sync-applier | `fullsync/FullSyncReceiver.java` + `consumer/FullSyncConsumer.java` | **全量同步状态机不完整。** (1) `CATCHING_UP → IDLE` 转换的 `onCatchUpComplete()` 存在但**从未被调用**（仅测试引用）。(2) `FullSyncConsumer` 在 `consume()` 返回空结果时立即 `break`，可能导致**提前退出**（网络延迟时误判"全量同步结束"）。(3) PREPARING 阶段未停止增量消费、未通知 ClusterStateManager 设置 SYNCING。 | (1) 在 `IncrementalConsumer` 检测到 syncLag 稳定后调用 `onCatchUpComplete()`。(2) `FullSyncConsumer` 改为等待 `FULL_SYNC_END` 事件而非空结果。(3) PREPARING 中调用 `syncApplier.stopIncremental()` 和 `clusterState.setServiceState(SYNCING)`。 |
| M8 | cdc-collector | `fullsync/FullSyncCoordinator.java` | **全量同步未暂停增量轮询。** 设计 §3.3 要求 Step 1 "暂停增量轮询循环"，Step 7 "恢复增量轮询（从 snapshotTimestamp 开始）"。实现中 `FullSyncCoordinator` 无任何暂停/恢复 CDC 轮询的逻辑，也无恢复后重置 `PollingState` 到 `snapshotTimestamp`。 | 注入 `CdcCollector` 引用（或通过回调），在 `startFullSync()` 开始时调用 `cdcCollector.pausePolling()`，结束后调用 `cdcCollector.resumePolling(snapshotTs)`。 |
| M9 | ha-agent | `recovery/OldPrimaryRecovery.java` | **旧主恢复流程中 pendingCleanup 清除时机过早。** 设计 §12.2 要求 Step 7（等待 SYNCING → ONLINE）之后才执行 Step 8（清除 pendingCleanup）。代码在启动 Sync 后立即清除 `pendingCleanup`，未等待 ONLINE。如果同步过程中旧主再次宕机，`pendingCleanup` 已被清除，下次恢复时可能跳过 Trigger 卸载和中转节点清理。 | 将 `markPendingCleanup(false)` 移到 ServiceState 变为 ONLINE 的回调中执行。需要与 M5 的 ServiceState 自动化联动配合实现。 |
| M10 | common | `redis/RedisClientFactory.java` | **RedisClientFactory 仅支持 Standalone 模式。** 设计要求"支持 Standalone/Sentinel/Cluster"。实现中 Sentinel 和 Cluster 分支抛出 `IllegalArgumentException`。虽然当前部署场景为 Standalone，但设计明确列出三种模式。 | 至少实现 Sentinel 模式（设计中多处提到 Redis Sentinel）。Cluster 模式可标记为 TODO。 |
| M11 | common | `redis/StreamPublisher.java` publishBatch() | **批量发布的 Fencing Token 校验非原子。** `publish()` 使用 Lua 脚本原子检查 Token + XADD。但 `publishBatch()` 先 `GET` Token 再 Pipeline `XADD`，两步之间存在竞态窗口：Failover 在 GET 和 XADD 之间递增 Token，旧主的批量发布仍会成功。 | 方案 A：每条消息独立使用 Lua 脚本（性能开销大但安全）。方案 B：使用 Lua 脚本在 Pipeline 开始前锁定 Token，Pipeline 结束后释放。方案 C：在 Lua 中实现批量 XADD（单次 eval 多条）。推荐方案 C。 |
| M12 | sync-applier | `validation/DuplicateDetector.java` | **去重策略为 FIFO 而非设计要求的 LRU。** 设计 §5 要求"LRU Set"。实现使用 `LinkedHashSet` + 删除首元素（即 FIFO/oldest-inserted eviction），不是 LRU（least-recently-used）。如果某个 eventId 被频繁重试查询，FIFO 策略可能过早淘汰它。 | 改用 `Collections.newSetFromMap(new LinkedHashMap<>(cap, 0.75f, true) { @Override removeEldestEntry... })`，`accessOrder=true` 实现 LRU。 |

### Minor — 建议修复，不阻塞

| # | 模块 | 文件:位置 | 问题描述 | 修复建议 |
|---|------|----------|---------|---------|
| m1 | common | `model/NodeRole.java` | 设计定义 `PRIMARY / STANDBY`，代码额外添加了 `DOWN`。`DOWN` 在 Failover 场景有实际用途，但应**更新设计文档**。 | 在设计文档中补充 `DOWN` 状态的定义和转换规则。 |
| m2 | common | `redis/StreamConsumer.java` consume() | 返回类型为 `List<Map.Entry<String, List<StreamEntry>>>`（Jedis 原始类型），设计要求 `List<StreamEntry>`。上层调用方需要额外解包。 | 封装为 `List<StreamEntry>` 返回，隐藏 Jedis 实现细节。 |
| m3 | common | `redis/CheckpointManager.java` saveSyncCheckpoint() | 未持久化设计中 `SyncCheckpoint` 的 `lastFullSyncAt` 和 `pendingCount` 字段。`loadSyncCheckpoint()` 读取这两个字段但总是默认值 0。 | `saveSyncCheckpoint()` 增加 `lastFullSyncAt` 和 `pendingCount` 参数并写入 Redis Hash。 |
| m4 | cdc-collector | `publish/PublishBuffer.java` | `maxFiles` 字段已声明但从未使用。设计要求"最多保留 100 个文件，超出告警"。同时缺少 10MB 单文件大小限制。 | 在 `flushToFile()` 中检查文件数量，超过 `maxFiles` 时告警。按字节大小而非事件数滚动文件。 |
| m5 | cdc-collector | `CdcCollector.java` start() | **启动时未清理残留 `_CDCDeleteEvent` 中转节点。** 设计 §5 表格明确要求"启动时清理：CDC Collector 启动时扫描并处理所有残留中转节点"。 | 在 `start()` 方法中，`CheckpointManager` 恢复位点后，调用 `deleteEventCapture.cleanupAll()` 清理残留。 |
| m6 | sync-applier | `consumer/PendingRecovery.java` | PEL 恢复路径未经过 `FencingTokenFilter` 和 `DuplicateDetector`。设计 §3.1 增量消费流程中这两步位于回放之前。 | 在 `PendingRecovery.recover()` 中复用 `FencingTokenFilter.filter()` 和 `DuplicateDetector.isDuplicate()` 进行过滤。 |
| m7 | sync-applier | `applier/IndexManager.java` ensureIndexesForAllLabels() | 跳过以 `_` 开头的标签（如 `_CDCDeleteEvent`），设计中未提及此过滤逻辑。 | 行为合理（系统标签无需 MERGE 索引），但应在设计文档中补充说明。 |
| m8 | ha-agent | `routing/HaProxyStateSyncer.java` isConsistent() | 使用 `state.contains(expectedPrimary)` 做字符串子串匹配判断一致性。这过于粗糙，可能产生误判（如 server 名包含预期名的子串）。 | 解析 `show servers state` 输出格式，按列匹配 server name + operational state。 |
| m9 | config | `docker/docker-compose.yml` | **HA Agent 服务被注释掉**，`build: ./ha-agent` 指向不存在的目录。 | 补充 HA Agent 的 Dockerfile 和 build 路径。 |

### Info — 参考建议

| # | 模块 | 文件:位置 | 说明 |
|---|------|----------|------|
| I1 | common | `util/IdGenerator.java` | 除设计要求的 `uuidV7()` 外，额外实现了 `shortId()`（随机 UUID 子串）。不影响功能，但非 v7 格式的短 ID 不具备时间排序性。 |
| I2 | ha-agent | `http/AdminHttpServer.java` | 设计列出 6 个独立 Endpoint 类（`ClusterStatusEndpoint` 等），实现将所有路由注册在 `AdminHttpServer.start()` 中。功能等价但不利于单独测试。 |
| I3 | ha-agent | `health/HealthChecker.java` | 设计列出 4 个独立健康检查类（`TcpHealthCheck` 等），实现内联在 `HealthChecker.checkNode()` 中。功能正确但不利于扩展。 |
| I4 | config | `config/agent/ha-agent.yml` | `stream.changes` + `stream.fullsync` 替代设计中的 `stream.key` 单字段。实际更灵活，但与设计文档不一致。 |
| I5 | config | `config/agent/ha-agent.yml` | `admin.auth.type` + `admin.auth.token` 嵌套结构替代设计中的 `admin.token` 扁平结构。功能等价，配置 schema 不同。 |

---

## 6. 修复行动项（按优先级排序）

- [x] **[Critical-C1]** `ha-agent/failover/FailoverOrchestrator.java` — Failover 后重启 SyncApplier
- [x] **[Critical-C2]** `cdc-collector/publish/PublishBuffer.java` — 实现磁盘文件回放能力
- [x] **[Critical-C3]** `cdc-collector/CdcCollector.java` + `StreamPublishService.java` — 删除中转节点清理改为条件执行
- [x] **[Major-M1]** `ha-agent/backup/BackupCoordinator.java` + `HaAgent.java` — 接线备份超时定时任务
- [x] **[Major-M2]** `ha-agent/failover/FailoverOrchestrator.java` — Failover 中取消进行中备份
- [ ] **[Major-M3]** `ha-agent/lifecycle/GracefulShutdown.java` — 补齐优雅关闭步骤
- [ ] **[Major-M4]** `ha-agent/health/HealthChecker.java` — 实现分层健康检查间隔
- [x] **[Major-M5]** `ha-agent/lifecycle/ClusterStateManager.java` — 实现 ServiceState 自动评估 + HAProxy 联动
- [x] **[Major-M6]** `sync-applier/applier/RelationshipApplier.java` — 使用带标签的 REL_MERGE 模板
- [x] **[Major-M7]** `sync-applier/fullsync/FullSyncReceiver.java` — 完善全量同步状态机
- [ ] **[Major-M8]** `cdc-collector/fullsync/FullSyncCoordinator.java` — 全量同步时暂停/恢复增量轮询
- [ ] **[Major-M9]** `ha-agent/recovery/OldPrimaryRecovery.java` — 修正 pendingCleanup 清除时机
- [ ] **[Major-M10]** `common/redis/RedisClientFactory.java` — 实现 Sentinel 模式
- [ ] **[Major-M11]** `common/redis/StreamPublisher.java` — 批量发布使用原子 Fencing Token 校验
- [ ] **[Major-M12]** `sync-applier/validation/DuplicateDetector.java` — 改为 LRU 淘汰策略
- [ ] **[Minor-m1~m9]** 各模块小项修复（见上表）
- [x] **[BUG-001]** `ApocTriggerInstaller.java` — APOC 删除触发器递归死循环（2026-04-15）
- [x] **[BUG-002]** `ChangeApplier.java` — IndexManager 会话冲突（2026-04-15）
- [x] **[BUG-003]** `CdcCollector.java` — Switchover 后 Fencing Token 未同步（2026-04-15）

---

## 7. 各模块合规性总览

### 7.1 Common 模块 (合规度: 85%)

| 设计要求 | 合规 | 说明 |
|---------|------|------|
| 数据模型 (records/enums) | ✅ | ChangeEvent/EntityData/NodeServiceState 等完全匹配 |
| ChangeEventType 枚举值 | ✅ | 全部 11 个值齐全 |
| StreamPublisher Lua 脚本 | ⚠️ | `publish()` 有 Lua，`publishBatch()` 非原子 (M11) |
| StreamConsumer 完整接口 | ⚠️ | `claim()` 有，`consume()` 返回类型不符 (m2) |
| DistributedLock 接口 | ✅ | `tryAcquire` → `Optional<LockHandle>`，Lua 续期/释放 |
| CheckpointManager 复合游标 | ⚠️ | CDC 部分完整，Sync 部分缺 lastFullSyncAt/pendingCount (m3) |
| RedisClientFactory 多模式 | ❌ | 仅 Standalone (M10) |
| 序列化/反序列化 | ✅ | JSON 序列化完整 |
| 配置加载 + 环境变量替换 | ✅ | `${VAR:-default}` 语法支持 |

### 7.2 CDC Collector 模块 (合规度: 75%)

| 设计要求 | 合规 | 说明 |
|---------|------|------|
| Keyset Pagination 轮询 | ✅ | 节点/关系/删除事件三路正确实现 |
| PollingState 复合游标 | ✅ | lastTs + lastElementId + lastDeleteTs + lastDeleteEid |
| DeleteEventCapture 中转节点 | ⚠️ | 查询正确，但清理时机有缺陷 (C3) |
| PublishBuffer 文件缓冲 | ❌ | 写文件有，读回放无 (C2) |
| FullSyncCoordinator | ❌ | 未暂停/恢复增量轮询 (M8) |
| DiffCalculator LRU Cache | ✅ | access-order LinkedHashMap |
| switchTarget (Failover) | ✅ | FencingToken 已同步到 StreamPublishService (BUG-003 已修复) |
| APOC 触发器自排除 | ✅ | 触发器排除 `_CDCDeleteEvent` 标签，防止递归死循环 (BUG-001 已修复) |
| 启动时清理残留中转节点 | ❌ | 未实现 (m5) |

### 7.3 Sync Applier 模块 (合规度: 76%)

| 设计要求 | 合规 | 说明 |
|---------|------|------|
| CypherTemplates 模板 | ✅ | NODE_MERGE / NODE_DELETE / REL_MERGE / REL_DELETE 正确 |
| 实际使用 REL_MERGE 模板 | ❌ | RelationshipApplier 未使用标签匹配 (M6) |
| IndexManager 动态索引 | ✅ | 按标签 + 关系类型动态创建 |
| IndexManager 会话隔离 | ✅ | 索引创建在事务开始之前执行，避免 DDL/DML 冲突 (BUG-002 已修复) |
| DuplicateDetector LRU | ❌ | FIFO 而非 LRU (M12) |
| FullSyncReceiver 状态机 | ⚠️ | 四状态存在，转换不完整 (M7) |
| PendingRecovery PEL 恢复 | ⚠️ | 核心 PEL 回放有，缺 Fencing/Dedup 过滤 (m6) |
| FencingTokenFilter | ✅ | 正确过滤旧 Token 事件 |
| pause/resume (备份) | ✅ | 实现完整 |

### 7.4 HA Agent 模块 (合规度: 72%)

| 设计要求 | 合规 | 说明 |
|---------|------|------|
| 8 阶段 Failover 编排 | ⚠️ | 8 阶段存在，但 SyncApplier 未重启 (C1)，未取消备份 (M2) |
| StandbySelector HEALTHY+ONLINE | ✅ | 正确过滤并按 checkpoint 排序 |
| HAProxy 多实例路由管理 | ✅ | HaProxyUpdater + HaProxyStateSyncer |
| HAProxy 状态同步修正 | ⚠️ | 一致性检查过于粗糙 (m8)，修正不完整 |
| BackupCoordinator 超时安全 | ❌ | 方法存在但未接线 (M1) |
| OldPrimaryRecovery 8 步 | ⚠️ | 缺 Step 7 等待 ONLINE (M9) |
| 3 个 APOC Trigger 安装 | ✅ | 安装/卸载均完整，触发器含 `_CDCDeleteEvent` 自排除过滤 (BUG-001 已修复) |
| IndexInstaller 动态索引 | ✅ | 主备节点分别创建对应索引 |
| GracefulShutdown | ❌ | 缺多个步骤 (M3) |
| 健康检查分层间隔 | ❌ | 单一间隔 (M4) |
| ServiceState 自动化 | ❌ | 未实现评估循环 (M5) |
| 防误切 (3 重保护) | ✅ | confirmationWait + minInterval + maxAutoPerHour |

### 7.5 配置文件 (合规度: 88%)

| 设计要求 | 合规 | 说明 |
|---------|------|------|
| ha-agent.yml 完整配置 | ✅ | 所有设计要求的配置项均存在，额外增加了更多项 |
| haproxy.cfg 读写分离 | ✅ | 写(7687) → neo4j_primary，读(7688) → neo4j_all roundrobin |
| Neo4j 配置来源统一 | ✅ | 当前统一通过 Docker Compose 环境变量（`NEO4J_*`）注入，`config/neo4j/neo4j.conf` 已移除 |
| docker-compose 部署 | ⚠️ | HA Agent 被注释掉 (m9)，build 路径不存在 |
| Maven 模块 + Java 17 | ✅ | 模块列表正确，Java 17，依赖链完整 |
| logback.xml 日志配置 | ✅ | ha-agent 已配置 logback.xml，Jetty/Javalin/Neo4j Driver 设为 WARN |

---

## 8. 评审结论

### 结论: REQUEST CHANGES

**核心价值：** 项目整体架构与设计文档高度对齐，四模块的包结构、核心数据模型、Keyset Pagination CDC 轮询、Fencing Token 全链路防脑裂、APOC Trigger 自动化管理、HAProxy 多活路由等关键设计均已正确实现。代码质量整体良好，命名规范一致。

**主要风险：**
1. **数据丢失风险** — Failover 后 SyncApplier 不重启(C1) + PublishBuffer 无磁盘回放(C2) + 删除事件提前清理(C3) 三个 Critical 问题叠加，可能导致 Failover 场景下数据永久丢失
2. **运维陷阱** — 备份超时未接线(M1) 可能导致 Sync Applier 永久暂停；优雅关闭不完整(M3) 可能导致状态不一致
3. **性能隐患** — 关系回放全表扫描(M6) 在数据量增大时会成为瓶颈

**建议优先级：**
1. **第一优先** — 修复 C1/C2/C3（数据安全）
2. **第二优先** — 修复 M1/M2/M5/M6（运维安全 + 性能）
3. **第三优先** — 修复其余 Major 和 Minor 问题
