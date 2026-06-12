# Neo4j 社区版主备高可用系统 — 设计方案评审报告

> 评审日期: 2026-04-10
> 评审文档:
> - `docs/nuclear-fusion/requirements/2026-04-10-neo4j-ha-requirements.md` (v1.0)
> - `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md` (v2.2)
> - `docs/nuclear-fusion/design/modules/common-design.md`
> - `docs/nuclear-fusion/design/modules/ha-agent-design.md` (v2.0)
> - `docs/nuclear-fusion/design/modules/cdc-collector-design.md`
> - `docs/nuclear-fusion/design/modules/sync-applier-design.md`
> - `config/agent/ha-agent.yml`, `config/haproxy/haproxy.cfg`, `docker/docker-compose.yml`
>
> 评审结论: **APPROVE with conditions**

---

## 1. 评审范围与上下文

- **系统定位：** 为 Neo4j Community Edition 2026.2.3 构建主备异步复制高可用方案，通过外部旁路 CDC + Redis Stream 实现准实时数据同步与自动 Failover。
- **约束前提：** 社区版无原生 CDC/集群；复用平台已有 Redis；APOC Core 内置 + APOC Extended 需额外安装；团队 Java 技术栈；目标 RPO<1s、RTO<30s。
- **评审文档清单：**

| 文档 | 类型 | 版本 |
|------|------|------|
| 需求解析文档 | 需求 | v1.0 |
| 系统架构设计文档 | 架构 | v2.2 |
| Common 模块设计 | 模块 | v1.0 |
| HA Agent 模块设计 | 模块 | v2.0 |
| CDC Collector 模块设计 | 模块 | v1.0 |
| Sync Applier 模块设计 | 模块 | v1.0 |

---

## 2. 评分总览

| # | 评审维度 | 评分 | 关键发现 |
|---|---------|------|---------|
| 1 | 架构合理性 | 4/5 | v2.0 集中式架构清晰简洁，模块划分合理；但 HA Agent 自身为单点 |
| 2 | 可行性 | 3.5/5 | 核心路径可行，但 APOC Extended 兼容性未验证、`$deletedNodes` 降级方案存疑 |
| 3 | 功能完善性 | 4/5 | 需求覆盖全面，CDC/Sync/Failover/全量同步均有详细设计 |
| 4 | 性能瓶颈 | 3.5/5 | 热点路径已识别并给出预估，但 CDC 轮询对主节点的读压力缺少实测基准 |
| 5 | 高可用与高可靠 | 4/5 | Fencing Token + 多层探活 + HAProxy 多活设计扎实；HA Agent SPOF 有缓解方案 |
| 6 | 稳定性 | 3.5/5 | 背压、LRU Cache 有限长，但 APOC Trigger 对写入延迟的影响未量化 |
| 7 | 成本 | 4.5/5 | 最小2台主机 + 复用Redis，成本极低；开源组件许可证无风险 |
| 8 | 存储与数据安全 | 3.5/5 | 备份协调设计优雅，但缺少 Redis Stream 持久化策略和传输加密方案 |
| 9 | 可维护性 | 4/5 | 结构化日志、Admin API、配置外部化；文档质量高 |
| 10 | 可管理性与审计 | 4/5 | Prometheus metrics + Failover 审计日志 + HAProxy Stats 覆盖良好 |
| 11 | 风险评估 | 3.5/5 | 核心风险已识别并有缓解，但 APOC Extended 版本锁定风险和 Neo4j 大版本升级风险需补充 |
| 12 | 容灾与灾备 | 2.5/5 | 方案聚焦同城主备，缺少异地灾备和备份恢复演练设计 |
| 13 | 部署与发布 | 3.5/5 | Docker Compose 完整，但缺少 CI/CD、滚动升级、制品管理方案 |
| | **综合评分** | **3.7/5** | |

---

## 3. 设计亮点

1. **APOC Trigger + 中转节点的删除捕获方案（架构 §4.3-4.4）** — 巧妙利用 `phase:'before'` + `apoc.trigger.toNode()` 创建瞬态中转节点，彻底避免软删除的存储膨胀和查询污染问题。设计思路原创、工程可行、降级路径明确。

2. **Keyset Pagination（架构 §4.2）** — 识别出 `timestamp()` 毫秒精度下的同毫秒遗漏风险，用 `(_updated_at, _elementId)` 复合游标保证无间隙分页。这是对常见 CDC 轮询方案的关键改进。

3. **v2.0 集中式架构（HA Agent 设计 §1-3）** — 从 v1.x 多进程 Sidecar 演进到单进程集中管理，大幅降低了系统复杂度：消除了 control stream 跨进程通信、简化了 Failover 编排（进程内直接方法调用）、统一了 HAProxy 路由管理。

4. **NodeServiceState 精细控制（HA Agent 设计 §3.1）** — 将"进程是否存活"（HealthState）与"数据是否就绪"（ServiceState）正交设计，配合 `syncLagThreshold + stableDuration` 双条件判定，避免数据未同步完成就上线接收流量。

5. **Fencing Token + Lua 原子操作的脑裂防护（架构 §7）** — 单调递增 Token + 发布时原子校验，确保旧主恢复后无法继续发布变更。设计简洁且正确。

---

## 4. 问题清单

### Critical — 必须修复

| # | 维度 | 问题描述 | 涉及文档章节 | 建议修复方案 |
|---|------|---------|-------------|-------------|
| C1 | 可行性 | **APOC Extended 在 Neo4j 2026.2.3 上的兼容性未验证。** `apoc.trigger.toNode()` 和 `apoc.any.properties()` 是整个删除捕获方案的基础，但文档仅声明"需额外安装"，未确认 APOC Extended 2026.x 是否已发布且兼容。如不兼容，删除事件捕获将失效。 | 架构 §4.3.2, CDC §5 | **在实现前必须完成 PoC 验证：** (1) 确认 APOC Extended 2026.x 版本存在且与 Neo4j 2026.2.3 兼容；(2) 执行架构 §4.3.5 的验证步骤；(3) 如不兼容，需立即切换到降级方案并验证 `labels(dn)`/`properties(dn)` 在 `before` phase 的可用性。将验证结果写入设计文档。 |
| C2 | 高可用 | ~~**已修复。**~~ Failover 期间 CDC Collector 停止但 APOC Trigger 仍在运行，`_CDCDeleteEvent` 中转节点可能积累。 | HA Agent §6, §12 | **已修复：** Failover 编排新增 Phase 8（best-effort 清理旧主）；新增 §12 旧主恢复与降级流程（8 步），含 Trigger 卸载 + 中转节点清理 + 增量/全量同步决策。详见 HA Agent 设计文档 §12 和架构文档 §7.5。 |

### High — 强烈建议修复

| # | 维度 | 问题描述 | 涉及文档章节 | 建议修复方案 |
|---|------|---------|-------------|-------------|
| H1 | 稳定性 | **APOC Trigger 对主节点写入延迟的影响未量化。** 3 个 Trigger 在每次写事务的 `before` phase 同步执行，包括 JSON 序列化（`apoc.convert.toJson`）和 CREATE 操作（`_CDCDeleteEvent`）。高写入并发下可能显著增加事务延迟。 | 架构 §4.3 | 在 PoC 阶段设计基准测试：(1) 无 Trigger 时的写入 TPS/延迟基线；(2) 安装 3 个 Trigger 后的同场景 TPS/延迟；(3) 高并发删除场景下 `_CDCDeleteEvent` 创建的性能影响。将结果写入设计文档 §4 作为性能基准。 |
| H2 | 功能完善性 | **旧主恢复后的降级流程缺少详细设计。** Failover 编排 Phase 7 标记旧主为 DOWN，但旧主恢复后如何降级为 Standby 缺少完整流程：APOC Trigger 卸载、数据清洗/全量同步、加入 Sync Applier 消费组。 | HA Agent §6 | 补充 "旧主恢复流程" 章节：(1) 检测旧主恢复（HealthChecker 发现 TCP/Bolt 恢复）；(2) 卸载旧主上的 APOC Trigger；(3) 清理旧主残留 `_CDCDeleteEvent`；(4) 评估数据差距——小差距增量追赶，大差距全量同步；(5) 将旧主加入备节点消费组；(6) 等待 SYNCING → ONLINE 后更新 HAProxy 读 backend。 |
| H3 | 数据安全 | **Redis Stream MAXLEN 裁剪可能导致未消费的变更事件丢失。** MAXLEN=100000 的近似裁剪在备节点长时间离线或消费严重滞后时，会删除尚未消费的消息。当前设计依赖"触发全量同步"兜底，但全量同步的触发条件（checkpoint 检测）与 TRIM 操作之间没有原子保障。 | 架构 §5.6 | (1) TRIM 前先检查所有活跃 Consumer Group 的最小消费位点，仅 TRIM 已被所有 Group 消费的消息；(2) 或改用 MINID 裁剪策略替代 MAXLEN，基于时间而非数量控制；(3) 增加 `stream_lag_to_oldest` 监控指标，当备节点消费位点接近 Stream 头部时提前告警。 |
| H4 | 性能瓶颈 | **CDC 轮询查询缺少全数据库无标签节点的处理方案。** `MATCH (n) WHERE n._updated_at > $lastTs` 是无标签扫描，在大数据量下效率低下。如果数据库中存在无标签节点（Neo4j 允许），这些节点的变更将无法被索引加速。 | 架构 §4.2, CDC §3.1 | (1) 在需求约束中明确声明"所有业务节点必须至少有一个标签"；(2) CDC 轮询改为按标签分批查询（`MATCH (n:Label) WHERE ...`），遍历所有已知标签；(3) 定期扫描无标签节点（`MATCH (n) WHERE size(labels(n))=0`）作为健康检查。 |
| H5 | 稳定性 | **CDC Collector 的 DiffCalculator LRU Cache 重启后丢失，导致首轮全部生成 UPDATE 事件（无 beforeState）。** 虽然文档标注"可接受"，但如果 Sync Applier 依赖 beforeState 做冲突检测或审计，这将导致信息缺失。 | CDC §3.2 | (1) 明确声明 Sync Applier 不依赖 beforeState 的完整性（当前 MERGE 全量覆盖模式确实不依赖）；(2) 可选优化：将 LRU Cache 快照定期持久化到本地文件或 Redis，重启后恢复。 |
| H6 | 架构合理性 | **架构文档 §2 技术选型表中 Neo4j 版本写为 "5.x"，与需求文档和 Docker Compose 中的 "2026.2.3" 不一致。** | 架构 §2 | 将技术选型表中 Neo4j 版本修正为 "2026.2.3"。 |

### Medium — 建议改进

| # | 维度 | 问题描述 | 涉及文档章节 | 建议修复方案 |
|---|------|---------|-------------|-------------|
| M1 | 容灾与灾备 | **缺少异地灾备方案和定期备份恢复演练计划。** 当前方案仅覆盖同机房主备，对机房级故障（火灾、断电、网络隔离）无应对。 | 全局 | 补充章节：(1) 备份数据异地存储方案（如对象存储）；(2) 定期（季度）灾备演练计划模板；(3) 全量备份恢复到新环境的操作手册。 |
| M2 | 部署与发布 | **缺少 HA Agent 的滚动升级/蓝绿发布方案。** HA Agent 为集中式单实例，升级意味着短暂停机（数据同步暂停）。 | HA Agent §13 | 补充升级操作手册：(1) 通知运维同步将暂停 N 秒；(2) 停旧实例 → 起新实例（Docker restart 已保证自动恢复）；(3) 验证新版本 checkpoint 恢复正确性；(4) 回滚方案：保留旧版本镜像 tag，`docker-compose up -d` 回退。 |
| M3 | 可维护性 | **配置项 `ha-agent.yml` 缺少配置变更的热加载能力描述。** 当前设计中所有参数似乎需要重启 Agent 才能生效。 | HA Agent §4, config/ha-agent.yml | (1) 明确哪些参数支持热加载（如 poll interval、batch size）、哪些需要重启（如 node list、Redis 连接）；(2) 可选：Admin API 增加 `/config/reload` 端点用于热加载安全参数。 |
| M4 | 功能完善性 | **全量同步使用 `SKIP $offset LIMIT $batchSize` 分页，大 offset 下性能退化（O(offset)）。** | 架构 §6.2, CDC §3.3 | 改为 keyset pagination：`ORDER BY elementId(n) ASC` + `WHERE elementId(n) > $lastElementId LIMIT $batchSize`，与增量同步保持一致的分页策略。 |
| M5 | 稳定性 | **PublishBuffer 本地文件回放时的顺序保证未说明。** 多个 buffer 文件的回放顺序是否严格按文件名时间戳排序？回放期间如果新事件继续产生，是否先回放缓冲再发布新事件？ | CDC §4 | 明确：(1) buffer 文件按文件名时间戳升序回放；(2) 回放期间新事件追加到新 buffer 文件；(3) 回放完成后切换回正常发布；(4) 回放失败的处理策略（重试/跳过/告警）。 |
| M6 | 安全 | **Admin API 的 Token 认证是静态配置，缺少 Token 轮换机制。** `ADMIN_TOKEN` 硬编码在环境变量中，泄露后需重启才能更换。 | HA Agent §9, config/ha-agent.yml | (1) 支持多 Token 同时有效（灰度轮换）；(2) 或支持从外部 Secret Manager（如 Vault）动态获取。 |
| M7 | 部署与发布 | **Docker Compose 中 HA Agent 被注释，缺少首次部署的启用指南。** 新用户不清楚何时取消注释、需要先完成什么前置步骤。 | docker/docker-compose.yml | 在注释中补充步骤：(1) 完成 Java 代码实现和 `docker/ha-agent/Dockerfile` 构建；(2) 确保 Redis 外部连接可达；(3) 取消注释 ha-agent 和 ha-agent-buffer volume 段；(4) `docker compose up -d` 启动。 |
| M8 | 可管理性 | **多备节点场景下的 Consumer Group 命名与管理不够清晰。** 架构 §5.3 说明每个备节点使用独立 Consumer Group，但 `ha-agent.yml` 中 `sync.consumer.group` 只有一个 "sync-applier"，未体现多备节点的 Group 命名规则。 | 架构 §5.3, config/ha-agent.yml | 配置中的 `sync.consumer.group` 改为模板格式 `sync-{node-id}`，HA Agent 为每个备节点自动生成 Group 名。文档中补充多备节点的 Consumer Group 生命周期管理（新节点加入创建 Group、节点永久移除时删除 Group）。 |

### Low / Info — 参考建议

| # | 维度 | 说明 |
|---|------|------|
| L1 | 一致性 | 架构文档 §5.3 前后描述有轻微矛盾：先说"共享 Consumer Group"，随后修正为"每个备节点独立 Group"。建议删除前一段落避免混淆。 |
| L2 | 可维护性 | `failover-manager-design.md` 和 `client-router-design.md` 虽标记为 DEPRECATED，仍保留在 `modules/` 目录下。建议移至 `docs/nuclear-fusion/design/deprecated/` 或在文件开头增加醒目废弃声明。 |
| L3 | 成本 | 需求文档 §4 要求支持 1主N备（N≤5），但架构文档和配置模板仅以 1主1备为例。建议在架构文档中补充 N>1 时的配置示例和注意事项。 |
| L4 | 可测试性 | 缺少混沌测试 / 故障注入测试的具体方案。需求文档提及"故障注入测试"但设计文档未展开。 |
| L5 | 一致性 | `common-design.md` 的依赖列表写 "jedis 5.x 或 lettuce 6.x"，但 `pom.xml` 已确定使用 Jedis 5.1.0。建议统一。 |

---

## 5. 风险矩阵

| # | 风险描述 | 概率 | 影响 | 等级 | 缓解措施现状 | 补充建议 |
|---|---------|------|------|------|-------------|---------|
| R1 | APOC Extended 与 Neo4j 2026.2.3 不兼容，删除捕获方案失效 | 中 | 高 | **Critical** | 有降级方案（APOC Core only）但未验证 | 实现前必须 PoC 验证（C1） |
| R2 | HA Agent 单点故障导致同步长时间中断 | 低 | 高 | **Medium** | Docker restart:always + checkpoint 恢复 | 考虑 active-passive 双 Agent 方案（远期） |
| R3 | APOC Trigger 严重拖慢主节点写入性能 | 中 | 中 | **Medium** | 无（未量化） | 基准测试（H1） |
| R4 | Redis 宕机导致 CDC 事件丢失 | 低 | 高 | **Medium** | PublishBuffer 本地缓冲 | 验证 buffer 回放的顺序正确性（M5） |
| R5 | 旧主恢复后数据不一致（脑裂残留） | 低 | 高 | **Medium** | Fencing Token 防脑裂 | 补充旧主降级清理流程（H2） |
| R6 | Redis Stream MAXLEN 裁剪删除未消费消息 | 中 | 中 | **Medium** | 触发全量同步兜底 | 改进 TRIM 策略（H3） |
| R7 | Neo4j 大版本升级导致 APOC API 变更 | 中 | 中 | **Medium** | 无 | 文档中记录 APOC API 版本锁定策略 |
| R8 | 全量同步期间主节点写入持续，同步完成后仍有数据差距 | 低 | 低 | **Low** | CATCHING_UP 阶段增量追赶 | 已有完善方案 |
| R9 | 机房级故障导致主备同时不可用 | 低 | 高 | **Medium** | 无 | 异地备份方案（M1） |

---

## 6. 各维度详细评审

### 6.1 架构合理性 (4/5)

**优点：**
- v2.0 集中式单进程架构显著降低了系统复杂度，消除了 v1.x 的跨进程通信和状态同步问题
- 模块划分清晰：common（共享）→ cdc-collector / sync-applier（功能模块）→ ha-agent（编排层），依赖方向单向无环
- 技术选型务实：Redis Stream 替代 Kafka 在此场景下合理（图变更频率远低于 Kafka 的设计目标，复用已有 Redis 降低运维成本）
- HAProxy 多活 + admin socket 动态路由是成熟方案

**不足：**
- 架构文档 §2 技术选型表中 Neo4j 版本仍为 "5.x"，与实际 2026.2.3 不一致（H6）
- HA Agent 本身是单点——虽然有缓解措施（restart:always + checkpoint 恢复），但 Agent 宕机期间数据同步完全停止。设计文档坦诚标注了这一点（HA Agent §13），缓解措施合理，但远期应考虑 active-passive 双实例

### 6.2 可行性 (3.5/5)

**优点：**
- 核心技术路径（Cypher 轮询 + Redis Stream + MERGE 回放）均基于成熟、稳定的技术
- 技术选型考虑了团队 Java 技术栈和已有 Redis 基础设施
- 降级方案的思路正确（APOC Core only fallback）

**不足：**
- **关键依赖未验证（C1）：** `apoc.trigger.toNode()` 是 APOC Extended 的函数，Neo4j 2026.2.3 对应的 APOC Extended 版本是否已发布？API 签名是否匹配？降级方案中 `labels(dn)` 在 `$deletedNodes` 上是否可用？这些都标注了"需验证"但尚未执行
- 实现工作量可能被低估：CDC 轮询的"冰山部分"包括——所有业务标签的动态索引管理、多标签节点的去重处理、关系的起止节点标签获取、JSON 序列化性能

### 6.3 功能完善性 (4/5)

**优点：**
- 需求文档的 10 个功能模块在设计中均有对应方案
- CDC 全覆盖：创建/更新（Cypher 轮询）+ 删除（APOC Trigger 中转节点）
- 全量同步 + 增量同步 + 断点续传的完整生命周期
- Failover 8 步编排流程完整（确认 → Fence → 停同步 → 选主 → 切 CDC → 切路由 → 更新状态 → 审计）

**不足：**
- **旧主恢复后的降级流程缺少设计（H2）**：这是 Failover 的后半程，但设计文档只到 Phase 7 就结束了
- 全量同步的分页使用 `SKIP/LIMIT`，大数据量下性能退化（M4）
- 多标签节点在 CDC 轮询中可能被重复捕获（按 Label A 查到一次，按 Label B 又查到一次）——设计文档未说明去重策略

### 6.4 性能瓶颈 (3.5/5)

**优点：**
- CDC Collector 给出了 4 级性能预估表（CDC §8），从低负载到峰值有清晰预期
- 索引策略设计完善（架构 §4.5），包括动态索引创建和性能影响评估
- Sync Applier 的批量提交 + UNWIND 合并优化合理

**不足：**
- CDC 轮询的 `MATCH (n) WHERE n._updated_at > $ts` 是**全标签扫描**，Neo4j 需要遍历所有标签的索引（H4）
- APOC Trigger 对主节点写入性能的影响未量化（H1）
- LRU Cache 50000 的容量设计缺少依据——如果实体数量远超 5 万，cache 命中率可能很低，DiffCalculator 将频繁退化为"无 beforeState"

### 6.5 高可用与高可靠 (4/5)

**优点：**
- **Fencing Token 设计正确**：单调递增 + Lua 原子校验 + 发布时每批验证，是经典的脑裂防护模式
- **多层探活**（L1-L4）由浅入深，避免单一检查方式的误判
- **HAProxy 多活**：多实例相同配置独立运行 + HA Agent admin socket 统一管理 + StateSyncer 定期补偿
- **防误切机制**完善：二次确认 + 最小间隔 + 最大次数限制

**不足：**
- HA Agent 自身是单点（已知风险，有缓解）
- Failover 过程中如果 `standbySelector.selectBest()` 抛出 `NoHealthyStandbyException`，catch 块只做了告警但没有具体的降级策略（如：是否仍然保持旧主的写入路由？是否标记集群为 DEGRADED？）

### 6.6 稳定性 (3.5/5)

**优点：**
- 背压机制清晰（架构 §5.6）：PEL 监控 + MAXLEN 裁剪 + 全量同步兜底
- PublishBuffer 提供 Redis 不可用时的本地缓冲能力
- 优雅关闭流程完整（等待当前批次 + 保存 checkpoint + 释放资源）

**不足：**
- **APOC Trigger 是同步执行的**（`before` phase），每次删除操作都会在事务中同步执行 CREATE `_CDCDeleteEvent`。高并发删除场景下可能成为性能瓶颈甚至导致事务超时
- PublishBuffer 回放的顺序保证和并发控制未说明（M5）
- 时钟依赖：`_updated_at` 基于 `timestamp()`（系统时钟），NTP 跳变可能导致 CDC 轮询遗漏或重复——虽然 keyset pagination 的第二游标 `_elementId` 能缓解，但时钟回退仍可能导致"新写入的 `_updated_at` 小于 checkpoint"从而被跳过

### 6.7 成本 (4.5/5)

**优点：**
- **最小部署仅 2 台主机** + 复用平台 Redis，硬件成本极低
- 全部开源组件（Neo4j Community + Redis + HAProxy + APOC），无商业许可费用
- Java + Maven 标准技术栈，团队学习曲线低

**不足：**
- 无（成本是本方案最大优势之一）

### 6.8 存储与数据安全 (3.5/5)

**优点：**
- 备份协调设计优雅（HA Agent §12）：pause Sync Applier → 备份 → resume，全程不影响主节点
- 备份超时安全阀（2h 自动恢复 Sync Applier），避免人工遗忘 complete
- Failover 期间自动取消备份，优先保证服务

**不足：**
- **Redis Stream 持久化依赖 Redis 自身的 RDB/AOF**，但设计文档和 Redis 配置中未明确 AOF 策略。如果 Redis 使用默认配置（`appendonly no`），Redis 崩溃重启后 Stream 数据全丢
- **传输加密缺失**：Neo4j Bolt 连接和 Redis 连接均未配置 TLS。虽然需求 §2.5 提到"Redis AUTH + TLS（可选）"，但架构文档未展开 TLS 配置方案
- Checkpoint 存储在 Redis 中——如果 Redis 和 HA Agent 同时故障，checkpoint 丢失后 Sync Applier 无法确定消费位点，将触发全量同步（设计可接受，但应在文档中明确说明此风险）

### 6.9 可维护性 (4/5)

**优点：**
- 结构化 JSON 日志（Logback + Logstash Encoder），兼容 ELK 体系
- Admin API 完整（状态查询、手动 Failover、备份管理、Prometheus metrics）
- 配置外部化（YAML + 环境变量替换），12-factor 风格
- 设计文档质量高，架构图清晰，关键设计决策均有解释

**不足：**
- 配置热加载能力缺失（M3）
- DEPRECATED 模块文件仍保留在主目录中（L2）

### 6.10 可管理性与审计 (4/5)

**优点：**
- Prometheus metrics 覆盖全面：同步延迟、Stream 长度、PEL 深度、Failover 计数、备份状态
- Failover 审计日志包含完整事件链（start/cancel/complete/fail）
- HAProxy Stats 面板提供实时连接状态可视化
- `/cluster/status` 端点一目了然展示全集群拓扑

**不足：**
- 多备节点的 Consumer Group 管理未标准化（M8）
- 缺少容量规划的具体指标：多大数据量需要扩备节点？Stream 多长需要扩大 MAXLEN？

### 6.11 风险评估 (3.5/5)

**优点：**
- 需求文档 §5 已识别 5 项关键技术风险，每项都有缓解措施
- Fencing Token 防脑裂、PublishBuffer 防 Redis 宕机、全量同步兜底——核心风险均有应对

**不足：**
- **APOC Extended 版本锁定风险**未识别：APOC Extended 是社区维护的独立项目，更新节奏可能与 Neo4j 不同步。Neo4j 升级时 Extended 可能滞后
- **Neo4j 大版本升级风险**未识别：`elementId()` 函数的返回格式、`apoc.trigger.install` API 签名等可能在大版本间变化
- 最坏场景分析缺失：如果 Redis 故障 + 主节点故障同时发生，系统行为是什么？

### 6.12 容灾与灾备 (2.5/5)

**优点：**
- 同机房主备切换流程完整（Failover 8 步）
- 备份协调设计合理

**不足：**
- **完全缺少异地灾备方案**：当前设计假设主备在同一网络/机房内，对机房级故障没有应对
- 缺少备份数据的异地存储策略
- 缺少灾备演练计划和 checklist
- 备份恢复流程未设计（如何从备份恢复到全新环境？）
- RPO/RTO 指标仅针对节点级故障，未针对机房级/区域级故障定义

### 6.13 部署与发布 (3.5/5)

**优点：**
- Docker Compose 配置完整，包含所有组件的健康检查、端口映射、卷挂载
- 环境变量参数化（`.env` 文件），开发/生产环境可通过不同 `.env` 切换
- 依赖顺序通过 `depends_on: condition: service_healthy` 正确声明

**不足：**
- **缺少 CI/CD 流水线设计**：Maven 构建 → Docker 镜像 → 推送 → 部署的自动化流程未定义
- **缺少制品管理**：构建产物（JAR/Docker image）如何版本化和存储
- **缺少滚动/蓝绿升级方案**（M2）
- Docker Compose 中 HA Agent 被注释且缺少启用指南（M7）
- 首次部署的初始化步骤（APOC Trigger 安装、索引创建）虽由 HA Agent 自动执行，但未提供手动执行的 fallback 脚本

---

## 7. 改进行动项

按优先级排序的待办清单：

- [ ] **[Critical] C1** — PoC 验证 APOC Extended 2026.x 与 Neo4j 2026.2.3 的兼容性，执行 §4.3.5 验证步骤
- [x] **[Critical] C2** — ~~补充 Failover 后旧主 `_CDCDeleteEvent` 残留清理和 APOC Trigger 卸载逻辑~~ **已修复**
- [ ] **[High] H1** — 设计基准测试量化 APOC Trigger 对主节点写入延迟的影响
- [ ] **[High] H2** — 补充旧主恢复降级为 Standby 的完整流程设计
- [ ] **[High] H3** — 改进 Redis Stream TRIM 策略，避免裁剪未消费消息
- [ ] **[High] H4** — CDC 轮询改为按标签分批查询，约束业务节点必须有标签
- [ ] **[High] H5** — 明确 Sync Applier 不依赖 beforeState，可选优化 LRU Cache 持久化
- [ ] **[High] H6** — 修正架构文档 §2 技术选型表中 Neo4j 版本为 2026.2.3
- [ ] **[Medium] M1** — 补充异地备份存储方案和灾备演练计划
- [ ] **[Medium] M2** — 补充 HA Agent 升级操作手册和回滚方案
- [ ] **[Medium] M3** — 明确配置项的热加载 vs 重启生效策略
- [ ] **[Medium] M4** — 全量同步分页改用 keyset pagination
- [ ] **[Medium] M5** — 明确 PublishBuffer 回放顺序和并发控制
- [ ] **[Medium] M6** — Admin Token 轮换机制
- [ ] **[Medium] M7** — Docker Compose 中补充 HA Agent 启用指南
- [ ] **[Medium] M8** — 多备节点 Consumer Group 命名规则标准化

---

## 8. 评审结论

### 结论: APPROVE with conditions

本方案在 Neo4j 社区版无原生 HA 能力的约束下，设计了一套**工程可行、成本极低、架构清晰**的主备高可用方案。APOC Trigger + 中转节点的删除捕获方案、Keyset Pagination 的无间隙 CDC、集中式 HA Agent 架构是三个突出的设计亮点。方案的核心数据流路径（CDC → Redis Stream → Sync Applier）成熟可靠，Failover 编排流程完整且有多重防误切机制。

**主要风险**集中在 APOC Extended 兼容性验证和 Failover 后旧主恢复流程的缺失。这两个 Critical 问题不影响方案架构的正确性，但必须在进入代码实现前解决。

### 通过条件:
- [ ] **C1 完成：** APOC Extended 兼容性 PoC 验证通过，或降级方案验证通过并更新设计文档
- [x] **C2 完成：** ~~补充旧主 `_CDCDeleteEvent` 清理和 APOC Trigger 卸载逻辑到 HA Agent 设计文档~~ **已修复**
- [ ] **H6 完成：** 修正架构文档中 Neo4j 版本号不一致问题
