# Neo4j 社区版主备高可用系统 — 设计方案分析报告

> 日期: 2026-04-10
> 模式: Nuclear Fusion Project Analysis

---

## 1. 项目概览

- **项目定位：** 为 Neo4j Community Edition 构建外部主备高可用系统，通过 CDC 轮询 + Redis Stream + Failover Manager 实现准实时数据同步与自动故障切换。
- **技术栈：** Neo4j CE 2026.2.3 + APOC Core/Extended + Redis 7 (Sentinel) + HAProxy 2.8 + Java 17
- **当前状态：** 设计文档齐全（需求 + 架构 + 6 个模块设计），基础设施脚手架已搭建（Docker Compose + 配置文件 + Maven Parent POM），但**零实现代码**。

---

## 2. 架构分析

### 2.1 整体架构评价

架构风格选择合理 — Master-Standby Async Replication 是社区版限制下的现实方案。选择 Redis Stream 替代 Kafka 在运维简化与延迟方面有明确优势。模块划分清晰（CDC Collector / Sync Applier / Failover Manager / Client Router / HA Agent / Common），职责单一，依赖方向正确（均向 common 依赖，无循环依赖）。

### 2.2 设计亮点

1. **APOC Trigger + 中转节点的删除捕获方案** — 相比软删除方案，避免了存储膨胀和查询污染，是一个巧妙的工程权衡。
2. **Fencing Token 防脑裂** — 基于 Lua 脚本的原子性校验 + 发布，正确处理了旧主复活场景。
3. **多层健康检查（L1-L4）** — 从 TCP 到 Cypher 再到写入测试，渐进式探活设计成熟。
4. **Consumer Group per Standby** — 正确识别了"每个备节点需要完整回放"的需求，避免了消息分发陷阱。
5. **PublishBuffer 本地缓冲** — Redis 不可用时的降级策略，保证了 CDC 事件不丢失。
6. **防误切机制** — 二次确认 + 最小切换间隔 + 最大自动切换次数，避免抖动导致的来回切换。

---

## 3. 问题清单

### Critical（必须修复，阻塞实现）

#### C1. `_elementId` 属性从未在主节点上设置 — 同步链路断裂

**文件：** `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md:241-251`, `docs/nuclear-fusion/design/modules/sync-applier-design.md:86-111`

**问题：** Sync Applier 的核心回放模板是：

```cypher
MERGE (n {_elementId: $elementId})
SET n = $properties
```

这要求备节点上的节点有 `_elementId` **属性**（property），且通过它来做幂等匹配。但 APOC Trigger（`cdc-timestamp`）只设置了 `_created_at` 和 `_updated_at`，**从未设置 `_elementId` 属性**。CDC Collector 使用 Neo4j 的 `elementId()` 函数获取内部标识符，这是一个函数返回值而非存储属性。

**后果链：**
1. 首次 MERGE：在备节点创建节点，`_elementId` 作为 MERGE 条件被设置为属性 ✓
2. 后续 UPDATE：`SET n = $properties` 是**全量属性替换**。主节点的 properties 中**不含** `_elementId`，因此这条 SET 会**删除**备节点上的 `_elementId` 属性
3. 再下一次该节点被更新时，`MERGE (n {_elementId: ...})` 找不到匹配 → **创建重复节点**
4. 数据一致性彻底破坏

**修复建议：**
- 方案 A（推荐）：在 APOC Trigger `cdc-timestamp` 中增加 `SET n._elementId = elementId(n)`，使主节点上的每个实体都持有 `_elementId` 属性，则 `SET n = $properties` 自然包含它
- 方案 B：修改 Sync Applier 模板，在全量替换后补设：`SET n._elementId = $elementId`
- 关系同理，需要对 `_createdRelationships` 也设置 `r._elementId = elementId(r)`

#### C2. `_elementId` 索引无法被 MERGE 使用 — 全表扫描

**文件：** `docs/nuclear-fusion/design/modules/sync-applier-design.md:187-189`

**问题：** 设计中创建的索引是：

```cypher
CREATE INDEX idx_element_id IF NOT EXISTS FOR (n:_HasElementId) ON (n._elementId)
```

但 MERGE 语句是：

```cypher
MERGE (n {_elementId: $elementId})
```

MERGE **没有指定 `_HasElementId` 标签**，Neo4j 不会使用该标签上的索引。对于百万级节点的数据库，每次 MERGE 都是全节点扫描（O(N)），同步性能将不可接受。

**修复建议：**
- 不使用自定义标签索引，改为对每个业务标签创建 `_elementId` 属性索引，或使用复合索引策略
- 或者在 MERGE 时带上标签：`MERGE (n:Person {_elementId: $elementId})`，但这要求按事件中的标签动态选择
- 最简方案：在备节点使用 `CREATE RANGE INDEX FOR (n) ON (n._elementId)` — 但 Neo4j 不支持无标签的属性索引，必须绑定标签。因此可以考虑给所有同步节点加一个通用标签（如 `_Synced`），并在该标签上建索引

#### C3. `_updated_at` 时间戳精度不足 — 同一毫秒内变更丢失

**文件：** `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md:180-181`

**问题：** CDC 轮询使用 `WHERE n._updated_at > $lastTs`（严格大于）。`timestamp()` 返回 epoch 毫秒。如果同一事务或同一毫秒内有多个节点被修改，只有最后一个会被记录为 `lastTs`。下一轮轮询使用 `> lastTs` 时，**同一毫秒内的其他变更会被跳过**。

**影响：** 在高并发写入场景下（批量导入、UNWIND 批量更新），同毫秒内可能有数十个节点变更，这些变更有丢失风险。

**修复建议：**
- 改用 `>=` 配合事件级去重（基于 elementId + timestamp 联合去重）
- 或使用更高精度的时间戳：`datetime().epochMillis` 不够的话，考虑维护一个应用级单调递增序号
- 或在 APOC Trigger 中使用 `apoc.atomic.add` 维护一个全局递增计数器作为版本号

#### C4. 主节点 `_updated_at` 字段无索引 — CDC 轮询全表扫描

**文件：** `docs/nuclear-fusion/design/modules/cdc-collector-design.md:53-66`

**问题：** CDC Collector 每 100ms 执行一次 `MATCH (n) WHERE n._updated_at > $lastTs`。没有在 `_updated_at` 上建索引，这对大图来说每次都是全节点扫描。关系上的查询 `MATCH ()-[r]->() WHERE r._updated_at > $lastTs` 同样。

**影响：** 100ms 轮询间隔下，每秒 10 次全表扫描，会严重拖垮主节点性能。

**修复建议：**
- 对所有业务标签创建 `_updated_at` 的 RANGE 索引
- 或使用通用标签 + 索引策略
- 关系索引在 Neo4j 5.x 中需要使用 `CREATE RANGE INDEX FOR ()-[r:REL_TYPE]-() ON (r._updated_at)` 对每种关系类型

### Major（强烈建议修复）

#### M1. HAProxy / Sentinel 配置使用硬编码 IP，与 Docker Compose DNS 不一致

**文件：** `config/haproxy/haproxy.cfg:25-26`, `config/redis/sentinel.conf:4`

**问题：**
- HAProxy 后端指向 `10.0.1.1:7687` 和 `10.0.1.2:7687`，但 Docker Compose 使用桥接网络，容器通过 DNS 名称（`neo4j-primary`, `neo4j-standby`）解析
- Sentinel 监控的 master 地址是 `10.0.1.3:6379`，但 Docker Compose 中 Redis 服务名为 `redis`
- HAProxy 健康检查端口 8080 指向 HA Agent，但 HA Agent 在 Docker Compose 中被注释掉

**后果：** 当前 Docker Compose 启动后，HAProxy 和 Sentinel 均无法正常工作。

**修复建议：**
- Docker 环境下将 IP 替换为服务名：`neo4j-primary:7687`, `neo4j-standby:7687`, `redis:6379`
- 或使用 Docker Compose 的 `networks` 配置固定 IP
- 为不同环境（Docker / 裸机）准备不同的配置文件模板

#### M2. Redis 配置文件不支持 `${...}` 环境变量替换

**文件：** `config/redis/redis.conf:7`, `config/redis/sentinel.conf:8`

**问题：** `redis.conf` 中使用 `requirepass ${REDIS_PASSWORD}`，`sentinel.conf` 中使用 `sentinel auth-pass neo4j-redis ${REDIS_PASSWORD}`。但 Redis **不支持在配置文件中展开环境变量**。启动后密码字面量是 `${REDIS_PASSWORD}` 字符串，认证会失败。

**修复建议：**
- 使用 Docker Compose 的 `command` 参数传递密码：`command: redis-server --requirepass ${REDIS_PASSWORD}`
- 或使用 `envsubst` 预处理配置文件（在 entrypoint 中）
- 或直接使用 Docker Compose 的 environment 配置

#### M3. 三个 Sentinel 共享同一份可写配置文件 — 运行时互相覆盖

**文件：** `docker/docker-compose.yml:78-109`

**问题：** 三个 Sentinel 容器挂载同一个宿主机文件 `../config/redis/sentinel.conf`，且**未使用 `:ro`**。Redis Sentinel 在运行时会**修改自己的配置文件**（记录当前 master 地址、已发现的其他 sentinel 等）。三个进程同时写同一个文件会导致：
- 配置相互覆盖
- Sentinel 行为不可预测
- 可能导致文件损坏

**修复建议：**
- 为每个 Sentinel 使用独立的配置文件（`sentinel-1.conf`, `sentinel-2.conf`, `sentinel-3.conf`）
- 或在 Docker 启动时将模板复制到容器内独立目录

#### M4. 备节点 Neo4j 无写保护 — 数据脏写风险

**文件：** `docker/docker-compose.yml:33-53`

**问题：** 主备两个 Neo4j 实例使用完全相同的配置，均可接受写入。Neo4j CE **没有只读模式**。如果有人直接连接备节点的 7688 端口写入数据（绕过 HAProxy），会导致主备数据不一致且无法自动修复。

**影响：** 破坏单写架构的核心假设，可能导致数据冲突。

**修复建议：**
- 在 HA Agent 层面拦截：Sync Applier 启动时通过 APOC Trigger 阻止非系统用户的写入
- 或使用 Neo4j 的用户权限控制，给备节点创建只读用户
- 或在网络层限制备节点的 Bolt 端口只对 HA Agent 可访问
- 文档中明确标注此为已知限制

#### M5. 全量同步使用 `SKIP $offset` — 大数据集性能退化

**文件：** `docs/nuclear-fusion/design/modules/cdc-collector-design.md:114-117`

**问题：** 全量同步分批导出使用：

```cypher
MATCH (n) RETURN n ORDER BY elementId(n) SKIP $offset LIMIT $batchSize
```

`SKIP` 在 Neo4j 中不是游标跳过，而是扫描并丢弃前 N 条结果。当 offset 到达百万级时，每批查询都要扫描前面所有数据，性能呈 O(N²) 退化。

**修复建议：**
- 改用 keyset pagination：记录每批最后一个 elementId，下批查询 `WHERE elementId(n) > $lastId`
- 或使用 `CALL apoc.periodic.iterate` 进行流式导出
- 或使用 Neo4j Admin 的 `neo4j-admin database dump` 进行物理快照

#### M6. 配置文件中残留废弃字段 `softDeleteField: "_deleted"`

**文件：** `config/agent/ha-agent.yml:43`

**问题：** 架构文档明确废弃了软删除方案，改用 APOC Trigger + 中转节点。但配置文件中仍保留 `softDeleteField: "_deleted"` 和 `elementIdField: "_elementId"`。这会让实现者产生混淆：到底该不该使用软删除？

**修复建议：**
- 移除 `softDeleteField` 配置项
- 添加注释说明删除捕获方案

#### M7. Failover Manager 单点故障 — 设计但未落地

**文件：** `docs/nuclear-fusion/design/modules/failover-manager-design.md:192-199`

**问题：** 设计文档第 8 节提到了 Failover Manager 自身高可用的方案（多实例 + Redis 选举），但：
- Docker Compose 中没有 Failover Manager 的服务定义
- 模块设计中没有展开实现细节（锁竞争、状态同步、活跃实例的监控接口）
- 如果唯一的 FM 实例宕机，整个 HA 系统的故障切换能力归零

**修复建议：**
- 优先级提升：FM 高可用不应作为 P2 特性
- 在 Docker Compose 中至少部署 2 个 FM 实例
- 模块设计中补充 FM 主备切换的详细流程

### Minor（建议修复，不阻塞）

#### m1. Maven 子模块 POM 缺失

**文件：** `pom.xml:14-21`

**问题：** 父 POM 声明了 6 个模块（`src/common`, `src/cdc-collector` 等），但这些目录下只有 `.gitkeep`，没有 `pom.xml`。`mvn compile` 会直接失败。

#### m2. Docker Compose 使用废弃的 `version: '3.8'`

**文件：** `docker/docker-compose.yml:1`

**问题：** 现代 Docker Compose V2 不再需要顶层 `version` 字段，Docker 官方已废弃此语法。

#### m3. 缺少 README.md

**问题：** 项目根目录没有 README。架构文档中的目录结构列出了 `README.md`，但实际不存在。新成员无法快速了解项目启动方式。

#### m4. Neo4j 配置文件来源调整

**现状：** 项目已移除 `config/neo4j/neo4j.conf`，Neo4j 配置统一由 Docker Compose 的 `NEO4J_*` 环境变量注入。

**说明：** 原“`neo4j.conf` 配置键命名兼容性”问题随配置来源调整而关闭。后续只需维护 Compose 中对应环境变量键的版本兼容性。

#### m5. `_CDCDeleteEvent` 中转节点无索引

**文件：** `docs/nuclear-fusion/design/2026-04-10-neo4j-ha-architecture.md:193-195`

**问题：** CDC Collector 每轮轮询都查询 `MATCH (e:_CDCDeleteEvent) WHERE e.timestamp > $lastTs`。如果因异常导致中转节点积累，无索引时查询变慢。虽然正常情况下中转节点数量极少（秒级清理），但应作为防御性设计加上索引。

#### m6. Docker Compose 中 Neo4j 和 Redis 无启动顺序依赖

**文件：** `docker/docker-compose.yml`

**问题：** HA Agent 服务（虽然注释了）依赖 Neo4j 和 Redis，但 Neo4j Primary/Standby 与 Redis 之间没有 `depends_on`。实际运行时，如果 Redis 先启动而 Neo4j 还未就绪，HA Agent 可能启动失败。`healthcheck` 可以缓解，但 `depends_on` 默认不等待 health check（除非使用 `condition: service_healthy`）。

---

## 4. 风险评估

### 4.1 技术风险

| 风险 | 概率 | 影响 | 当前缓解 | 建议 |
|------|------|------|---------|------|
| APOC Extended 在 Neo4j 2026.2.3 上不可用或 API 变更 | 中 | 高 — 删除事件无法捕获 | 有降级方案（仅 Core） | 降级方案需实测验证，建议在 CI 中加入 APOC 兼容性测试 |
| APOC Trigger 性能开销影响主节点写入延迟 | 中 | 中 — 每次写入事务都增加 trigger 执行时间 | 无 | 需基准测试：trigger 对 TPS 的影响百分比 |
| Redis Stream MAXLEN 裁剪导致备节点落后触发全量同步 | 中 | 高 — 全量同步期间备节点不可用 | 有全量同步机制 | 监控 Stream 长度 + 备节点 checkpoint 延迟的告警 |
| 同一毫秒时间戳导致 CDC 变更丢失 | 中 | 高 — 静默数据不一致 | 无 | 见 C3 |
| Neo4j CE 版本升级后 APOC/Cypher 行为变化 | 低 | 高 | 无 | 建立版本兼容性矩阵，升级前回归测试 |
| 网络分区导致旧主继续接受写入（Fencing Token 检查依赖 Redis 可达） | 低 | 高 — 脑裂 | Fencing Token | 如果旧主与 Redis 同时断开，旧主无法验证 Token，需本地缓存最后已知 Token + 超时自降级 |

### 4.2 工程风险

| 风险 | 说明 | 建议 |
|------|------|------|
| 零代码状态，设计到实现的鸿沟 | 6 个模块 + 复杂的分布式协调逻辑，实现周期长 | 按优先级分期：Phase 1 (common + cdc + sync) → Phase 2 (failover + agent) → Phase 3 (router + monitoring) |
| 集成测试困难 | 涉及 Neo4j + Redis + HAProxy 多组件联调 | 优先建立 Docker Compose 的集成测试环境，使用 Testcontainers |
| APOC Trigger 行为的不确定性 | `$deletedNodes` 在 `before` phase 中的具体行为取决于版本实现 | 在编码前先写 PoC 验证 trigger + 中转节点方案 |
| 运维复杂度 | 多进程（HA Agent + FM + Neo4j + Redis + HAProxy）协调 | 完善 scripts/ 目录，编写一键部署/验证/故障演练脚本 |

---

## 5. 总结评估

| 维度 | 评分(1-5) | 说明 |
|------|----------|------|
| 架构质量 | 4 | 模块划分清晰，依赖方向正确，核心分布式问题（脑裂、幂等、断点续传）都有方案。扣分项：部分关键细节有逻辑漏洞（`_elementId` 属性链路、索引策略）。 |
| 设计完整度 | 3.5 | 需求→架构→模块设计文档齐全，覆盖了正常流程和大部分异常流程。扣分项：索引策略缺失、Failover Manager 高可用细节不足、配置与实际环境不一致。 |
| 可落地性 | 3 | 伪代码和类结构设计明确，技术选型合理。扣分项：C1-C4 的逻辑缺陷如果不在编码前修正会导致返工；APOC Trigger 方案需要先 PoC 验证。 |
| 工程成熟度 | 2 | 零代码、零测试、零脚本、零 README。配置文件有多处不可用的问题（IP 不一致、环境变量不展开、Sentinel 配置冲突）。 |
| 风险控制 | 3.5 | 需求文档的风险章节识别了主要风险并提出了缓解措施。但 C1（`_elementId` 属性丢失）和 C3（时间戳精度）属于设计级缺陷，未被识别。 |

---

## 6. 建议行动项（优先级排序）

### 编码前必须修复

1. **[x] [C1]** 修正 `_elementId` 属性的写入策略 — 已在 APOC Trigger 中增加 `SET n._elementId = elementId(n)` + Sync Applier 模板防御性补设
2. **[x] [C2]** 重新设计 `_elementId` 索引策略 — 已改为按业务标签动态建索引 + MERGE 携带标签命中索引
3. **[x] [C3]** 修正 CDC 轮询的时间戳比较逻辑 — 已改为 keyset pagination (`_updated_at` + `_elementId` 复合游标)
4. **[x] [C4]** 补充主节点上 `_updated_at` 的索引设计 — 已新增架构文档 §4.5 索引策略章节

### 编码前建议修复

5. **[ ] [M1]** 修正 HAProxy / Sentinel 配置的地址 — 适配 Docker Compose DNS 环境
6. **[ ] [M2]** 解决 Redis 配置文件的环境变量问题
7. **[ ] [M3]** 每个 Sentinel 使用独立的配置文件
8. **[ ] [M4]** 设计备节点写保护机制
9. **[ ] [M5]** 全量同步改用 keyset pagination
10. **[ ] [M6]** 清理配置文件中的废弃字段

### PoC 验证项

11. **[ ]** 在 Neo4j 2026.2.3 上验证 APOC Trigger `before` phase 的 `$deletedNodes` 行为
12. **[ ]** 验证 `apoc.trigger.toNode()` 和 `apoc.trigger.toRelationship()` 在 APOC Extended 2026.x 中是否可用
13. **[ ]** 基准测试：APOC Trigger 对主节点写入 TPS 的影响
14. **[ ]** 验证 `elementId()` 在 trigger `before` phase 中对即将删除的节点是否返回有效值
