# Neo4j HA 代码评审报告 — 实现 vs 设计文档

> 评审日期: 2026-04-17
> 评审模式: Nuclear Fusion Code Review
> 评审范围: `src/common`、`src/cdc-collector`、`src/sync-applier`、`src/ha-agent`
> 对照文档:
> - `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md` (v2.2)
> - `docs/nuclear-fusion/design/modules/{common,cdc-collector,sync-applier,ha-agent}-design.md`
> 评审结论: **REQUEST CHANGES — 发现 3 个 Critical 缺陷（其中 1 个为 BUG-016 回归），7 个 Major 问题**
>
> **修复状态（2026-04-17 更新）：** 3 个 Critical 和 7 个功能性 Major 均已修复并记录于 ha-agent-design.md §15 的 **BUG-021..BUG-031**。交叉引用：
> - C1 → **BUG-021** · C2 → **BUG-022** · C3 → **BUG-023**
> - M1 → **BUG-024** · M2 → **BUG-025** · M3 → **BUG-026** · M7 → **BUG-027** · M8 → 并入 **BUG-021** · M9 → **BUG-028** · M10 → **BUG-029**
> - 附带修复：PendingRecovery 对称性 → **BUG-030** · recordFailover 耗时 → **BUG-031**
> - 未修复的观测性/整洁性问题（M4 剩余部分、M5、M6）见下方"未修复"小节

---

## 1. 评审范围与概况

- 架构已演进到 v2.0 集中式 HA Agent，`failover-manager` 和 `client-router` 两个模块设计文档已标记为 DEPRECATED，职责并入 ha-agent，代码目录与文档一致。
- 实际代码共约 80 个 Java 文件。已核对 `ha-agent-design.md §15` 中列出的 BUG-001..BUG-020 在当前代码中的落实情况：**绝大多数已修复**（BUG-001/002/003/004/007/008/009/010/011/012/015/017/018/019/020），但 **BUG-013 / BUG-016 在 `SyncApplier.consumeLoop` 的指标写入路径上出现回归**（虽然 serviceState 评估路径已修）。
- 两个废弃模块的设计文档标注清晰，代码目录也已清理，符合 v2.0 演进叙述。

## 2. 评估结论

- **结果：REQUEST CHANGES**
- Critical: 3 | Major: 10 | Minor: 10 | Info: 6
- **亮点**:
  - 关键的 fencing/checkpoint/触发器自排除等核心正确性约束整体到位
  - v2.0 "集中式 HA Agent" 的改造较为彻底（`DrainWaiter` 除外，见下）
  - Redis 阻塞池与共享池的隔离实现符合 BUG-019 规约
  - HAProxy `haproxy.cfg` 的 `init-addr none` + `resolvers docker_dns` 已落实（BUG-020）

---

## 3. Critical — 必须修复

| # | 文件:行号 | 维度 | 问题描述 | 修复建议 |
|---|----------|------|---------|---------|
| C1 | `src/cdc-collector/.../CdcCollector.java:142-176` | 正确性 / 数据一致性 | 当 `publishService.publishBatch(events)` 返回 `null`（事件落入 `PublishBuffer` 本地缓冲），`publishSuccess=false`，删除清理被跳过；**但** `pollingState.setLastTs/LastElementId`（L163-164）、删除游标（L167-173）和 `saveCheckpoint()`（L176）**仍然照常推进**。下轮轮询使用新游标跳过这些变更；`_CDCDeleteEvent` 中转节点滞留无人清理；如果缓冲后续回放失败，**变更在主库 Neo4j 上事实丢失**。与架构 §5.4 "失败时写入 PublishBuffer 本地缓冲" 的语义（期望事件至少有一份在 buffer 中）只有在 buffer 永不丢失时成立，而 buffer 是本地文件，机器故障或 1GB 上限触发删除都会导致真实数据丢失。 | 仅在"publish 成功 或 成功写入 buffer 且 buffer 能保证持久回放" 的前提下推进游标。推荐：publish 失败时保持游标、下轮继续重试；对 buffer 写入成功但 Redis 不通的场景，记录 "已缓冲但未发布" 边界并阻止游标前进直到回放完成。 |
| C2 | `src/sync-applier/.../applier/IndexManager.java:15` + `SyncApplier.java:42,83-84,95` | 正确性（多备场景） | `indexedLabels` 是 `SyncApplier` 共享的**全局** `Set`。当集群存在 ≥2 个 standby 时，第一个 standby 会话成功执行 `CREATE RANGE INDEX IF NOT EXISTS FOR (n:Label) ON (n._elementId)` 后，label 被加入全局集合；处理第二个 standby 会话时 `ensureIndex()` 直接 `contains` 命中返回，**跳过在第二个 standby 数据库上的建索引调用**。结果：第二个 standby 上的 `MERGE (n:Label {_elementId: ...})` 无法命中 `_elementId` 索引，沦为全表扫描，性能随数据量指数级退化（与设计 §7.1 "MERGE 必须携带标签才能命中索引" 的前提冲突）。 | 将缓存 key 改为 `(nodeId / databaseId, label)`；或者彻底去掉本地缓存 — 依赖 Neo4j 自身的 `IF NOT EXISTS` 幂等（代价：每批次对每个新 label 发一次 DDL，可接受）。`BulkImporter` 共享同一 `IndexManager` 实例的问题同理。 |
| C3 | `src/sync-applier/.../SyncApplier.java:264-274` | 设计偏离 / BUG-016 回归 | `consumeLoop` 用 `now - cp.lastEventTs()` 计算每个备节点 lag，写入 `metrics.syncLagMs`。这正是 BUG-016 报告中修复过的错误口径——在业务低写入时段，该值随墙钟线性增长，**已追平的备节点也会被指标呈现为高 lag**，触发 `Neo4jHASyncLagCritical`、`Neo4jHASyncingTooLong` 等告警误报。`HaAgent.evaluateServiceStates()` 已经按 BUG-016 修复成 `primaryCdc.lastTs - standbySync.lastEventTs`，但指标写入路径没有同步修正，与 ha-agent-design §3.1 的注释"lag 口径为主库 CDC checkpoint 与备库 Sync checkpoint 的时间戳差值"仍不一致。 | 改为 `max(0, primaryCdc.lastTs - standbyCp.lastEventTs)`，与 HaAgent 评估服务状态的计算口径统一；或者提取公共方法复用。 |

---

## 4. Major — 强烈建议修复

| # | 文件:行号 | 维度 | 问题描述 | 修复建议 |
|---|----------|------|---------|---------|
| M1 | `src/ha-agent/.../recovery/OldPrimaryRecovery.java:76-80` + `SyncApplier.java:213-222` | 灾备 / 功能完整性 | 旧主恢复评估为 `FULL_SYNC` 时调用 `syncApplier.triggerFullSync(oldPrimaryId)`，该方法**仅打一行日志**，没有向 CDC / Redis 发布 `FULL_SYNC_START` 控制事件。设计 §12.2 Step 6 明确要求启动 Sync Applier 对旧主同步，但当前实现下全量同步路径实际为空操作，数据差距较大的旧主永远无法自动追平。 | 注入 `CdcCollector` / `FullSyncCoordinator`，在该方法中 `coordinator.startFullSync(oldPrimaryId)`，或走 `AdminHttpServer` 内现有 `/cluster/fullsync` 的相同路径。 |
| M2 | `src/ha-agent/.../HaAgent.java:170-176` | 并发 | 主节点健康检查 `onDown` 回调里 `new Thread(() -> executeFailover(...))`，**没有任何互斥**。在抖动场景下可同时启动两次 Failover：两次都会 `fencingTokenManager.increment()`、双写 node-registry 并切换 HAProxy，状态会错乱。 | 用 `AtomicBoolean failoverInProgress` 或单线程 Executor 保证单飞；如需支持多集群，每个集群一把锁。 |
| M3 | `src/ha-agent/.../routing/HaProxyStateSyncer.java:72-80` | 正确性 / HA | `isConsistent` 仅做 `state.contains(expectedPrimary)` 的字符串包含判断；`applyExpectedState` 只发送一条"新主 ready"命令，**不对旧主执行 drain/maint**。HAProxy 重启后的状态修正不完整（设计 §7.2 / 架构 §8.5 要求"状态与预期不一致 → 发送修正命令"涵盖所有 server）。 | 解析 `show servers state` 返回的结构化字段（每行按空格切分或用 `show stat` CSV），按 `HaProxyUpdater.switchPrimary` 的完整三步序列修正：旧主 drain → 新主 ready → 旧主 maint。 |
| M4 | `src/common/.../metrics/HaMetrics.java` 全类 + `architecture.md §9.1` | 可观测性 / 设计偏离 | 架构 §9.1 列出 25+ 指标，实现中缺失或命名错误的有：`neo4j_ha_node_role`、`neo4j_ha_node_health`、`neo4j_ha_node_service_state`、`neo4j_ha_cdc_events_consumed_total`、`neo4j_ha_cdc_events_applied_total`（代码为 `neo4j_ha_sync_events_applied_total`）、`neo4j_ha_sync_lag_events`、`neo4j_ha_stream_length`、`neo4j_ha_stream_pending`、`neo4j_ha_fencing_token`、`neo4j_ha_haproxy_instance_reachable`（应带 `instance` 标签，现仅计数错误）、`neo4j_ha_haproxy_state_sync_total` / `_fix_total`、`neo4j_ha_fullsync_total` / `_duration_ms`、`neo4j_ha_backup_duration_ms`、`neo4j_ha_failover_duration_ms`（代码为 `neo4j_ha_failover_duration`）。另外 `HaMetrics.recordFailover(boolean, long)` 第 107-114 行接收 `durationMs` 但**未调用** `failoverDuration.record(...)`，耗时指标永远为 0。告警规则（§9.2）依赖这些指标名，直接会失效。 | 按 §9.1 表格补齐；或者反向更新架构文档与现状对齐（不推荐，因为告警规则也会失效）。 |
| M5 | `src/ha-agent/.../bootstrap/IndexInstaller.java:16-46` | 设计偏离 | `ensureIndexes` 对所有角色一视同仁地创建 `_updated_at` + `_elementId` 节点索引和 `_updated_at` 关系索引。架构 §4.5.1 / §4.5.2 明确要求：主节点侧重 `_updated_at` + `_CDCDeleteEvent.timestamp`，备节点侧重 `_elementId`。当前实现会在 standby 上创建无用的 `_updated_at` 索引，增加备机写放大；也没有创建 `_CDCDeleteEvent` 专用索引。 | 新增 `NodeRole` 参数，按角色分支创建索引，与 §4.5.3 伪代码一致。 |
| M6 | `src/ha-agent/.../lifecycle/GracefulShutdown.java:28-56` | 设计偏离 | 关闭钩子仅停 CDC/Sync + 关 Driver/Redis；未停 HealthChecker、未停 `AdminHttpServer`、未在 node-registry 标记 Agent 离线（设计 §10 步骤 2、5、7）。 | 按 §10 顺序扩展关闭流程；确保各组件有 `stop()` 入口。 |
| M7 | `src/cdc-collector/.../CdcCollector.java:116-122` + `FailoverOrchestrator.java:119-123` | 正确性 | `switchTarget(newDriver, newNodeId, newToken)` 在新主 `nodeId` 下 `loadCdcCheckpoint`。旧主的 checkpoint 保存在旧 key 下，新主通常无 checkpoint → `PollingState.initial()` 从 `(0, "")` 扫描全库，对 Redis 产生海量重复，依赖下游 `DuplicateDetector` 去重。设计文档中 checkpoint 应为"集群级"或与 fencing 绑定。 | 让 checkpoint key 以集群/数据库级（而非 nodeId）存储，或在 Failover 时显式迁移旧主 checkpoint 到新主 key；需要文档同步澄清语义。 |
| M8 | `src/cdc-collector/.../publish/StreamPublishService.java:67-75` | 正确性 | `retryBuffered` 用 `catch (Exception e)` 捕获所有异常（包括 `FencingTokenRejectedException`）并把批次**重新塞回 buffer**。这会掩盖"当前 CDC 持有的 token 已被 fence"的信号，导致 CDC 继续"重试 → 再次被 fence → 再塞回 buffer"的死循环，违反设计 §6.4 / 架构 §7.4 "Token 不匹配时立即 step-down" 的预期。 | 单独 `catch (FencingTokenRejectedException e)` 并向上抛出（或通过回调通知 `FailoverOrchestrator` / 停止 CDC），不能 re-buffer。 |
| M9 | `src/sync-applier/.../fullsync/BulkImporter.java:42-119` | 设计偏离 / 性能 | 节点全量导入使用 `MERGE (n {_elementId: ...}) ... SET n:Label1:Label2`，MERGE **不带标签**。与 sync-applier-design §3.2 / §3.3 "MERGE 必须携带标签" 的基石原则冲突——在全库范围内按 `_elementId` 做全图扫描。关系路径同样端点 MATCH 不带标签。 | 改为按标签分组 UNWIND（每组一个带标签 MERGE），或在事件里拆出主标签作为 MERGE 标签后再 `SET` 其余标签；关系路径端点 MATCH 加标签。 |
| M10 | `src/ha-agent/.../failover/FailoverOrchestrator.java:82-90,160-168,242-258` | 设计偏离 | `checkSafeToFailover()` 将 `minIntervalMs` / `maxAutoPerHour` 用在**所有** `executeFailover()` / `executeSwitchover()`，包括运维手动 API (`/cluster/failover`、`/cluster/switchover`)。设计 §11 的"最小 Failover 间隔 / 最大自动切换次数" 明确限定在**自动触发**路径。会阻塞紧急人工操作。 | 手动路径带 `force=true`/跳过计数；仅自动触发时累加计数器。 |

---

## 5. Minor — 建议改进

| # | 文件:行号 | 问题描述 | 修复建议 |
|---|----------|---------|---------|
| m1 | `src/ha-agent/.../http/AdminHttpServer.java:47` | `GET /health` 始终 200，`/cluster/status` 响应缺少设计 §9 示例中的 `agentId` / `uptime` / `fencingToken` / 各节点 sync 详情字段 | 实现依赖检查并在 Redis/Neo4j 不可达时返回 503；扩充 JSON |
| m2 | `src/ha-agent/.../backup/BackupCoordinator.java:33-47` | `prepare` 未返回 `lastCheckpoint`（设计 §13.2），也未在备节点 DOWN 时拒绝备份请求 | 补全字段与前置校验 |
| m3 | `src/ha-agent/.../health/HealthChecker.java:94,155-158` | L3 失败阈值硬编码 2 次（L1/L2 用配置 `failThreshold`）；`database` 硬编码 `"neo4j"` | 抽取为配置项；从 `NodeInfo` 读库名 |
| m4 | `src/ha-agent/.../recovery/ApocTriggerUninstaller.java:25-26` + `OldPrimaryRecovery.java:68,84` | 数据库名硬编码 `"neo4j"`，与 `ApocTriggerInstaller` 的参数化不一致 | 传入配置 database |
| m5 | `src/cdc-collector/.../fullsync/FullSyncCoordinator.java:49-65` | 控制事件（FULL_SYNC_START/END）发到 `changesStreamKey` 而非 `control` stream（架构 §5.1 划分了 changes/fullsync/control 三个 stream） | 引入 `control` stream 或统一到 `fullsync` 并文档化 |
| m6 | `src/common/.../redis/StreamPublisher.java:88-95` | `publishFullSyncBatch` 不做 fencing 校验，与增量 `publish` 不一致 | 加同样的 Lua 校验 |
| m7 | `src/common/.../redis/StreamPublisher.java:62-85` | `publishBatch` 的 fencing 校验是 `GET` + Pipeline `XADD`，非原子（与 `publish` 的 Lua 不一致），存在 TOCTOU 窗口 | 改为 Lua/MULTI 原子路径 |
| m8 | `src/cdc-collector/.../publish/PublishBuffer.java:68-85` | `drain` 仅在内存空时读磁盘；内存+磁盘同时存在数据时违反 FIFO | 强制 flush 后读磁盘，或统一有序队列 |
| m9 | `src/cdc-collector/.../capture/NodeChangeCapture.java:20-27` | `MATCH (n) WHERE ...` 未排除 `_CDCDeleteEvent`；依赖触发器的 label 排除防护 | 增加 `WHERE NOT n:_CDCDeleteEvent` 双重保险，与 `NodeExporter` 对齐 |
| m10 | `src/sync-applier/.../consumer/PendingRecovery.java:37-55` | PEL 恢复批次不经过 `FencingTokenFilter` / `DuplicateDetector`，与主循环过滤链不一致 | 复用过滤器或在 recover 内单独 fencing 校验 |

---

## 6. Info — 参考

| # | 位置 | 说明 |
|---|------|------|
| i1 | `src/ha-agent/.../failover/DrainWaiter.java` | 类存在但无任何调用方，实际走 `syncApplier.drainPending()`。与设计保留的"独立 DrainWaiter 类"叙述不符 |
| i2 | `src/common/.../redis/RedisClientFactory.java:65-91` | `createBlockingPool` 每次创建新池，未关旧池；重复调用会泄漏 |
| i3 | `src/common/.../util/RetryUtil.java:12-37` | 只有固定延迟，没有退避/jitter |
| i4 | `src/cdc-collector/.../CdcCollectorConfig.java:8,22` | `pollTimeoutMs` 已配置但未在 `Session.run` 上生效，设计 §6 要求超时并重试 1 次 |
| i5 | `src/common/.../model/EntityData.java:6-24` | 相较 common-design §3.4 增加 `startNodeLabels`/`endNodeLabels`；为扩展非冲突 |
| i6 | 架构 §5.2 JSON 示例 vs `ChangeEventType` | 文档示例写 `REL_CREATED` 等缩写，代码枚举为 `RELATIONSHIP_CREATED`；序列化实际走枚举名，两处不一致 |

---

## 7. BUG-001..BUG-020 落实情况核查

| BUG | 状态 | 说明 |
|-----|-----|------|
| 001 | ✅ | `ApocTriggerInstaller` 所有触发器均含 `WHERE NOT "_CDCDeleteEvent" IN apoc.node.labels(...)` |
| 002 | ✅ | `ChangeApplier.ensureIndexesForBatch` 在 `executeWrite` 之前执行索引创建 |
| 003 | ✅ | `CdcCollector.start/switchTarget` 均调用 `publishService.setFencingToken(...)` |
| 004 | ✅ | `SyncApplier.start()` 首行 `standbyDrivers.clear()` |
| 005 | ✅ | `FencingTokenValidator.isValid` 无 auto-bump；`SyncApplier.start()` 调用 `fencingFilter.updateToken(currentFencingToken)` |
| 006 | ✅ | `ha-agent/pom.xml` 有 `maven-dependency-plugin` |
| 007 | ✅ | `HaAgent.evaluateServiceStates` 用 per-node `stableSinceByNode` Map |
| 008 | ✅ | `CdcCollector.saveCheckpoint` 持久化 `lastStreamId`；`start` 恢复 |
| 009 | ✅ | `SyncApplier.drainPending` 使用 `processing` 标志 + 30s 截止 |
| 010 | ✅ | `FailoverOrchestrator`/`OldPrimaryRecovery` 使用 `WITH e LIMIT 10000 ... RETURN count(*)` 循环 |
| 011 | ✅ | `executeSwitchover` 现在会卸载旧主 APOC Trigger 并清理残留 |
| 012 | ✅ | `ApocTriggerInstaller` 使用统一 `WITH` 大写 |
| 013 | ✅ | `IncrementalConsumer` 不再直接写指标；**C3 修复（BUG-023）** 后 `SyncApplier.consumeLoop` 也不再写，lag 指标统一由 `HaAgent.evaluateServiceStates` 计算后写入 |
| 014 | ☑️ | 已标记技术债（备份暂停全局 applier），与当前 1+1 部署场景兼容 |
| 015 | ✅ | `addTarget` 新建 filter 后 `updateToken(currentFencingToken)` |
| 016 | ✅ | `HaAgent.evaluateServiceStates` 与指标口径均已统一（**BUG-023** 关闭回归） |
| 017 | ✅ | `ha-smoke-test.sh` 在 switchover 前等待 ONLINE |
| 018 | ✅ | `SyncApplier.start` 两处 per-node try/catch 跳过不可达节点 |
| 019 | ✅ | `RedisClientFactory` 共享池 `testWhileIdle` + `minIdle=2` + `testOnReturn=false`；阻塞池独立 |
| 020 | ✅ | `haproxy.cfg` 含 `resolvers docker_dns` + `init-addr last,libc,none`；`test-compose.yml` HAProxy `restart: on-failure:5` |

---

## 8. 修复行动项（按优先级）

- [ ] **[Critical] C1** `CdcCollector.pollLoop` — publish 失败时不推进 polling/checkpoint 游标
- [ ] **[Critical] C2** `IndexManager` — 按 `nodeId` 划分索引缓存，或去除本地缓存
- [ ] **[Critical] C3** `SyncApplier.consumeLoop` — 将 lag 口径改为"主备 checkpoint 差值"，与 `evaluateServiceStates` 统一
- [ ] **[Major] M1** `OldPrimaryRecovery.triggerFullSync` — 实际触发 FullSyncCoordinator.startFullSync
- [ ] **[Major] M2** `HaAgent.onDown` — 为 executeFailover 加并发互斥
- [ ] **[Major] M3** `HaProxyStateSyncer.applyExpectedState` — 发送完整的 drain/ready/maint 三步
- [ ] **[Major] M4** `HaMetrics` — 按架构 §9.1 补齐指标并记录 `failoverDuration`
- [ ] **[Major] M5** `IndexInstaller` — 按角色区分索引集合
- [ ] **[Major] M6** `GracefulShutdown` — 按设计 §10 顺序补全
- [ ] **[Major] M7** `CdcCollector.switchTarget` — checkpoint 迁移/继承策略
- [ ] **[Major] M8** `StreamPublishService.retryBuffered` — 单独处理 `FencingTokenRejectedException`
- [ ] **[Major] M9** `BulkImporter` — MERGE 携带标签
- [ ] **[Major] M10** `FailoverOrchestrator` — 区分手动/自动触发的限流规则
- [ ] (Minor / Info 见对应表格)

---

## 9. 评审结论

### 结论: REQUEST CHANGES

项目整体骨架、关键控制面（failover 编排、fencing token、HAProxy 多活、APOC 触发器自排除、Redis 连接池隔离）与设计一致，已积累 20 个迭代 bug 的修复，可维护性良好。但当前代码仍存在：

1. **数据面正确性风险**（C1 CDC 游标推进 + C2 多 standby 索引共享）可能在生产负载下直接导致数据不一致/备节点退化；
2. **回归监管缺失**（C3 BUG-016 在另一条路径重新出现）表明 metrics/evaluate 两处 lag 口径未统一，后续仍会回归；
3. **观测性偏差**（M4）使架构 §9.2 的告警规则大面积失效；
4. **灾备闭环缺口**（M1 旧主全量同步空实现）在 Stream 被 TRIM 时无法自动恢复旧主。

建议先修复 3 个 Critical + M1/M2/M3/M4，再补齐其余 Major，即可视为可上线。

### 后续建议

- 为 lag 口径、checkpoint key、fencing 校验原子性等"易回归"点添加单元测试/集成测试断言
- 将"指标清单"从架构文档抽为可自动校验的 `HaMetrics` 常量表，消除文档与代码的漂移
- BUG-014（全局 pause）在未来引入多 standby 前必须解决，否则备份会全网暂停
