# Neo4j 社区版主备高可用系统 — 系统架构设计文档

> 日期: 2026-04-10
> 版本: v2.2 (HAProxy 多活高可用 + 集中式 HA Agent + 数据备份最佳实践)
> 模式: Nuclear Fusion Full Pipeline — Phase 2

---

## 1. 架构概览

### 1.1 架构风格

**主备异步复制架构（Master-Standby Async Replication）**

核心思路：在 Neo4j 社区版外部构建一个**旁路同步层**，通过捕获主节点数据变更、经由 Redis Stream 传输、在备节点回放，实现准实时的主备数据同步。故障检测、数据同步、切换编排由**一个集中式 HA Agent** 统一管理。

### 1.2 系统上下文图

```
                        ┌─────────────────────────────────────────────┐
                        │              Client Applications            │
                        │  配置多个 HAProxy 地址，连不上自动切换下一个   │
                        └─────────┬─────────────────┬─────────────────┘
                                  │ Bolt            │ Bolt (fallback)
                                  ▼                 ▼
              ┌──────────────────────┐   ┌──────────────────────┐
              │   HAProxy-1 (多活)   │   │   HAProxy-2 (多活)   │
              │  Write:7687 Read:7688│   │  Write:7687 Read:7688│
              │  (相同配置，独立运行) │   │  (相同配置，独立运行) │
              └─────┬────────┬───────┘   └─────┬────────┬───────┘
                    │        │                 │        │
                    └────┬───┴─────────────────┴───┬────┘
                  Write  │                         │  Read
                         ▼                         ▼
              ┌───────────────────────┐   ┌───────────────────────────┐
              │   Neo4j Primary       │   │   Neo4j Standby (1..N)    │
              │   (Master Node)       │   │   (Read Replica)          │
              └──────────┬────────────┘   └─────────────┬─────────────┘
                         │ Bolt (远程)                   │ Bolt (远程)
                         ▼                              ▼
              ┌──────────────────────────────────────────────────────┐
              │              HA Agent (集中式单进程)                   │
              │                                                      │
              │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
              │  │ CDC Collector │  │ Sync Applier │  │  Health    │ │
              │  │ (轮询主节点)  │  │ (写入备节点)  │  │  Checker   │ │
              │  └──────┬───────┘  └──────┬───────┘  └─────┬──────┘ │
              │         │ XADD           │ XREADGROUP      │        │
              │  ┌──────┴────────────────┴─────────────────┴──────┐ │
              │  │             Failover Orchestrator               │ │
              │  │  - Fencing Token    - Node Registry              │ │
              │  │  - HAProxy Updater (管理所有 HAProxy 实例)       │ │
              │  │  - State Syncer (定期同步路由状态到所有 HAProxy)  │ │
              │  └────────────────────────────────────────────────┘ │
              └──────────────────────┬─────────────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────────────┐
              │                Redis Cluster / Sentinel              │
              │  ┌────────────────────────────────────────────────┐ │
              │  │  Stream: neo4j:cdc:{database}:changes         │ │
              │  │  Stream: neo4j:cdc:{database}:fullsync        │ │
              │  │  Hash:   neo4j:ha:checkpoint:{node-id}        │ │
              │  │  String: neo4j:ha:leader-lock                 │ │
              │  │  String: neo4j:ha:fencing-token               │ │
              │  │  Hash:   neo4j:ha:node-registry               │ │
              │  └────────────────────────────────────────────────┘ │
              └──────────────────────────────────────────────────────┘
```

### 1.3 部署拓扑

```
┌──────────── Host A ──────────┐  ┌──────────── Host B ──────────┐
│  Neo4j Primary (bolt:7687)   │  │  Neo4j Standby (bolt:7687)   │
│  HAProxy-1 (可选，就近路由)   │  │  HAProxy-2 (可选，就近路由)   │
└──────────────────────────────┘  └──────────────────────────────┘
                     │                              │
                     └──────────┬───────────────────┘
                                ▼ Bolt (远程连接)
┌──────────── Host C ─────────────────────────────────────────────┐
│  HA Agent (集中式，管理所有 Neo4j 节点 + 所有 HAProxy 实例)      │
│  Prometheus + Grafana                                           │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼ 复用平台已有实例
┌─────────────────────────────────────────────────────────────────┐
│  Redis (平台基础设施，HA Agent 通过 standalone 模式连接)          │
└─────────────────────────────────────────────────────────────────┘
```

最小部署：2 台主机（1 台 Neo4j 主 + 1 台运行备库/HA Agent）。Redis 复用平台已有实例，无需单独部署。
Docker Compose 单机部署：Neo4j + HAProxy + HA Agent 在同一 Docker 网络中，Redis 通过外部地址连接。

**⚠️ Redis 持久化必备说明：**
对接的 Redis 必须开启 **AOF（建议 `everysec`）+ RDB 快照** 并由平台负责备份。Redis 中存有整个 HA 控制面的权威状态（节点角色、Fencing Token、checkpoint 等），数据丢失会导致 Agent 重启时误选主、造成静默数据回滚。详见 §12.5 与运维手册 §9。

**HAProxy 多活部署说明：**
- 2 个以上 HAProxy 实例使用相同的 `haproxy.cfg`，各自独立运行
- 每个 HAProxy 都能独立完成读写路由，任一实例宕机不影响另一个
- 客户端配置多个 HAProxy 地址，默认连第一个，连不上自动切换下一个
- HA Agent 通过各 HAProxy 实例的 admin socket 统一管理路由状态

---

## 2. 技术选型

| 层级 | 技术选型 | 版本 | 理由 |
|------|---------|------|------|
| 图数据库 | Neo4j Community Edition | 5.x | 开源免费、功能满足业务需求 |
| 消息通道 | Redis Stream（复用平台已有） | **≥ 6.2**（推荐 7.x） | 亚毫秒延迟、Consumer Group、轻量运维；6.2+ 必须，因 `XTRIM MINID` 命令需 6.2 引入 |
| 分布式锁 | Redis (SET NX EX)（复用平台已有） | **≥ 6.2**（推荐 7.x） | Leader选举、Fencing Token |
| 客户端路由 | HAProxy | 2.8+ | 成熟稳定、支持TCP层Bolt协议转发 |
| 主体语言 | Java | 17+ | Neo4j Driver成熟、并发模型好 |
| 运维脚本 | Python | 3.10+ | 部署/备份/监控脚本 |
| 构建工具 | Maven / Gradle | - | Java生态标准 |
| 容器化 | Docker + Docker Compose | - | 统一部署环境 |
| 监控 | Prometheus + Grafana | - | 业界标准可观测方案 |
| 日志 | SLF4J + Logback (JSON) | - | 结构化日志、兼容ELK |

> **注意：** Redis 为平台已有基础设施，本项目复用，不单独部署。HA Agent 通过 standalone 模式直连外部 Redis。Redis 自身的高可用由平台统一保障。
>
> ⚠️ **Redis 最低版本要求：6.2+**。`StreamMaintenanceTask` 使用 `XTRIM MINID ~` 命令进行 consumer-aware 的 Stream 安全裁剪（详见 §5 及 BUG-038），该命令在 Redis 6.2 中引入。Redis 6.0.x 会报 `ERR syntax error`，导致 Stream 精细化清理失败（核心同步不受影响，但 Stream 只能依赖 MAXLEN 兜底裁剪）。推荐使用 Redis 7.x。
>
> ⚠️ **关键前提 — Redis 必须开启持久化：** 本方案把集群的**权威控制状态**（节点角色注册表、Fencing Token、CDC/Sync checkpoint、Stream 消息）**全部**存储在 Redis 中。Redis 数据丢失等同于 HA 控制面失忆，可能导致重启后**把数据陈旧的节点误选为主 → 静默数据回滚**。因此：
>
> - 对接的 Redis 实例 **必须同时开启 AOF + RDB**（或至少 AOF everysec）
> - 平台的 Redis 高可用/备份策略必须保证 `neo4j:ha:*` 前缀的 Key 不被无意清理
> - 运维变更（Redis 迁移、版本升级、主从切换）需预先在测试环境演练 key 丢失场景
> - 详见 §12.5 "Redis 下线与恢复"、以及运维手册 `ha-agent-cluster-operations.md §9 "Redis 数据丢失后的集群重建"`

---

## 3. 模块分解

> **v2.0 架构简化：** 原设计中 failover-manager 和 client-router 为独立进程。v2.0 将它们合并到 ha-agent 中，整个 HA 系统只有一个 Java 进程。
> 当前仓库中已不再保留 `src/failover-manager` 与 `src/client-router` 目录。

### 3.1 模块列表与职责

```
neo4j-HA/
├── src/
│   ├── ha-agent/            # HA Agent — 集中式集群管理主进程（唯一可执行模块）
│   ├── cdc-collector/       # CDC Collector — 变更数据捕获（远程连接主节点）
│   ├── sync-applier/        # Sync Applier — 变更回放（远程连接备节点）
│   └── common/              # Common — 共享模型、工具类、配置
```

| 模块 | 职责 | 运行方式 | 依赖 |
|------|------|---------|------|
| **ha-agent** | 集中式主进程：启动/协调 CDC 与 Sync、健康检查所有节点、Failover 编排、HAProxy 路由管理、Prometheus metrics | 独立进程（1个实例管理整个集群） | common, cdc-collector, sync-applier |
| **cdc-collector** | 远程连接主节点、轮询变更、序列化、发布到 Redis Stream | 作为 ha-agent 内部模块运行 | common |
| **sync-applier** | 消费 Redis Stream、反序列化、远程连接备节点回放 Cypher | 作为 ha-agent 内部模块运行 | common |
| **common** | 数据模型（ChangeEvent 等）、Redis 客户端、Neo4j 客户端、配置加载 | 共享库 | — |

**已合并的模块（v1.x → v2.0）：**

| 原模块 | 合并到 | 说明 |
|--------|--------|------|
| failover-manager | ha-agent | 健康检查、Failover 编排、Fencing Token 管理直接内置于 ha-agent |
| client-router | ha-agent | HAProxy Runtime API 调用内置于 ha-agent 的 Failover 流程 |

### 3.2 模块间依赖关系

```
                    ┌──────────────┐
                    │   common     │
                    └──────┬───────┘
                           │ 被所有模块依赖
                    ┌──────┼──────┐
                    ▼      ▼      ▼
              ┌──────┐ ┌────────┐ ┌───────────────────────────┐
              │ cdc- │ │ sync-  │ │        ha-agent           │
              │coll. │ │applier │ │  (集中式主进程)             │
              └──┬───┘ └──┬─────┘ │                           │
                 │        │       │  内含:                     │
                 │        │       │  - HealthChecker           │
                 │        │       │  - FailoverOrchestrator    │
                 │        │       │  - HaProxyUpdater          │
                 │        │       │  - AdminAPI                │
                 └────────┴───────┤  - MetricsExporter         │
                  作为内部模块启动  │                           │
                                  └───────────────────────────┘
```

---

## 4. CDC 变更捕获方案设计

### 4.1 方案对比

| 方案 | 原理 | 优点 | 缺点 | 适用性 |
|------|------|------|------|--------|
| **A: Cypher 轮询** | 定期查询带时间戳/版本号的节点和关系变更 | 社区版兼容、实现简单、版本无关 | 需要业务层配合（时间戳字段）、轮询开销 | **主方案** |
| B: Transaction Log 解析 | 解析 Neo4j 二进制事务日志 | 无需修改数据模型、捕获完整变更 | 格式内部未公开、版本间可能变化、实现复杂 | 高级备选 |
| C: APOC Trigger | 利用 APOC 的 trigger 机制监听变更 | 实时性好、声明式 | APOC 版本兼容性、trigger性能影响、不支持所有变更类型 | 辅助方案 |
| D: 自定义 Neo4j Plugin | 编写 Neo4j 内核扩展拦截事务 | 最全面、最实时 | 耦合内核版本、社区版API有限 | 不推荐 |

### 4.2 主方案：Cypher 轮询 + 增量时间戳

**前提：** 所有节点和关系都必须维护 `_updated_at` (Long, epoch millis)、`_created_at` 和 `_elementId` (String, 主节点 elementId) 字段。通过 APOC Trigger 在事务提交前自动设置，业务层无需感知。`_elementId` 仅在创建时设置，作为主备间实体唯一标识。

**轮询流程：**

> **设计决策：** 使用 **keyset pagination**（基于 `(_updated_at, _elementId)` 复合游标）替代 `WHERE _updated_at > $lastTs`。原因：`timestamp()` 精度为毫秒，同一毫秒内多个节点变更时，严格大于（`>`）会导致同毫秒的部分变更被跳过。keyset pagination 通过添加 `_elementId` 作为第二排序键保证不遗漏。

```
┌──────────────────────────────────────────────────────────────────┐
│                    CDC Collector                                   │
│                                                                    │
│  Checkpoint = (lastTs, lastElementId)                              │
│                                                                    │
│  1. 读取上次 checkpoint (lastTs, lastElementId)                    │
│  2. 执行增量查询（keyset pagination，无间隙）:                      │
│     MATCH (n) WHERE n._updated_at > $lastTs                       │
│       OR (n._updated_at = $lastTs AND n._elementId > $lastEid)    │
│     RETURN n, labels(n), properties(n)                            │
│     ORDER BY n._updated_at ASC, n._elementId ASC                  │
│     LIMIT $batchSize                                              │
│                                                                    │
│  3. 查询关系变更（同样 keyset pagination）:                         │
│     MATCH ()-[r]->() WHERE r._updated_at > $lastTs                │
│       OR (r._updated_at = $lastTs AND r._elementId > $lastRelEid) │
│     RETURN r, type(r), startNode(r)._elementId,                   │
│            endNode(r)._elementId                                  │
│     ORDER BY r._updated_at ASC, r._elementId ASC                  │
│     LIMIT $batchSize                                              │
│                                                                    │
│  4. 查询删除事件（扫描中转节点 _CDCDeleteEvent）                    │
│     MATCH (e:_CDCDeleteEvent)                                     │
│     WHERE e.timestamp > $lastDeleteTs                             │
│       OR (e.timestamp = $lastDeleteTs                             │
│           AND elementId(e) > $lastDeleteEid)                      │
│     RETURN e ORDER BY e.timestamp ASC, elementId(e) ASC           │
│     LIMIT $batchSize                                              │
│                                                                    │
│  5. 封装为 ChangeEvent 列表                                        │
│  6. 批量 XADD 到 Redis Stream                                     │
│  7. 清理已发布的 _CDCDeleteEvent 中转节点                           │
│  8. 更新 checkpoint (lastTs, lastElementId) ← 本批最后一条的值      │
│  9. sleep(pollInterval) → 回到 1                                  │
└──────────────────────────────────────────────────────────────────┘
```

**轮询参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `cdc.poll.interval` | 100ms | 轮询间隔 |
| `cdc.poll.batchSize` | 500 | 每次查询最大返回数 |
| `cdc.poll.timeout` | 5s | 单次查询超时 |
| `cdc.timestamp.field` | `_updated_at` | 时间戳字段名 |
| `cdc.elementId.field` | `_elementId` | keyset 第二排序键 |

### 4.3 APOC Trigger 设计（创建/更新/删除全覆盖）

**APOC 依赖说明：**
- Neo4j 版本: Community Edition 2026.2.3
- APOC Core: 2026.x（随 Neo4j 内置，提供 `apoc.trigger.install`、`$deletedNodes`、`$removedLabels` 等基础能力）
- APOC Extended: 2026.x（需额外安装，提供 `apoc.trigger.toNode()`、`apoc.any.properties()` 等虚拟节点函数）

**安装方式（Docker）：**
```bash
NEO4J_PLUGINS='["apoc", "apoc-extended"]'
```

**安装方式（手动）：**
```bash
# 下载对应版本 jar 放入 plugins 目录
cp apoc-extended-2026.x.x.jar $NEO4J_HOME/plugins/
```

**Neo4j 启动配置（当前实现）：**
当前项目使用 Docker Compose 环境变量注入 Neo4j 配置（`NEO4J_*`），不再维护独立的 `config/neo4j/neo4j.conf` 文件。

**Trigger 安装执行上下文（与当前代码一致）：**
- `apoc.trigger.install($db, ...)` 的执行会话使用 `system` 数据库（`SessionConfig.forDatabase("system")`）
- 触发器目标数据库仍由参数 `$db` 指定（当前为 `neo4j`），因此触发器运行在业务库 `neo4j` 上
- Neo4j 2026.x 启动初期 `system` 库可能短暂处于 FOLLOWER；HA Agent 对该错误执行重试（10 次、每次间隔 3s）
- 若重试后仍失败，HA Agent 不因 Trigger 安装失败退出，而是降级继续启动（删除事件捕获能力暂不可用，待后续重试/人工修复）

#### 4.3.1 Trigger 1：自动维护时间戳 + `_elementId`（创建/更新）

> **设计决策：** `_elementId` 是主备节点间实体唯一标识的关键属性。Neo4j 内部 ID 在不同实例间不一致，因此必须将主节点的 `elementId()` 值作为显式属性存储，供备节点 `MERGE` 匹配和索引命中。该属性仅在节点/关系**创建时**设置一次，后续更新不改变。

```cypher
CALL apoc.trigger.install('neo4j', 'cdc-timestamp',
  '// === 1. 新建节点：设置全部系统属性 ===
   UNWIND $createdNodes AS n
     SET n._created_at = timestamp(), n._updated_at = timestamp(), n._elementId = elementId(n)
   WITH 1 AS dummy

   // === 2. 节点属性变更：更新时间戳 ===
   UNWIND $assignedNodeProperties AS prop
   WITH prop.node AS n
     WHERE prop.key <> "_elementId" AND prop.key <> "_created_at" AND prop.key <> "_updated_at"
     SET n._updated_at = timestamp()
   WITH 1 AS dummy

   // === 3. 防护 SET n = $props（全量替换）导致 _elementId 丢失 ===
   // 当业务代码使用 SET n = {k:v} 时，_elementId 会被删除，出现在 $removedNodeProperties 中
   // 此分支检测到 _elementId 被移除后立即恢复
   UNWIND $removedNodeProperties AS prop
   WITH prop.node AS n, prop.key AS k
     WHERE k = "_elementId"
     SET n._elementId = elementId(n)
   WITH 1 AS dummy

   // === 4. 新建关系：设置全部系统属性 ===
   UNWIND $createdRelationships AS r
     SET r._created_at = timestamp(), r._updated_at = timestamp(), r._elementId = elementId(r)
   WITH 1 AS dummy

   // === 5. 关系属性变更：更新时间戳 ===
   UNWIND $assignedRelationshipProperties AS prop
   WITH prop.relationship AS r
     WHERE prop.key <> "_elementId" AND prop.key <> "_created_at" AND prop.key <> "_updated_at"
     SET r._updated_at = timestamp()
   WITH 1 AS dummy

   // === 6. 防护关系的 SET r = $props 全量替换 ===
   UNWIND $removedRelationshipProperties AS prop
   WITH prop.relationship AS r, prop.key AS k
     WHERE k = "_elementId"
     SET r._elementId = elementId(r)',
  {phase: 'before'})
```

**设计说明：**
- 分支 1/4（`$createdNodes/$createdRelationships`）：新建时设置 `_elementId = elementId()`、`_created_at`、`_updated_at`
- 分支 2/5（`$assignedNodeProperties/$assignedRelationshipProperties`）：属性变更时更新 `_updated_at`，过滤系统属性避免递归
- 分支 3/6（`$removedNodeProperties/$removedRelationshipProperties`）：**防护 `SET n = $props` 全量替换场景**。当业务代码使用全量替换删除了 `_elementId` 时，Trigger 自动恢复。这使得业务层无需强制区分 `SET n =` 和 `SET n +=`
- 所有分支在 `phase: 'before'` 中执行，与业务事务同提交/同回滚，无一致性风险

#### 4.3.2 Trigger 2：删除事件捕获（节点）

在 `phase: 'before'` 阶段，节点尚未被真正删除。利用 APOC Extended 的 `apoc.trigger.toNode()` 将即将删除的节点转为虚拟节点，提取完整的标签和属性快照，写入中转节点 `_CDCDeleteEvent`。

```cypher
CALL apoc.trigger.install('neo4j', 'cdc-capture-node-deletes',
  'UNWIND $deletedNodes AS dn
   WITH apoc.trigger.toNode(dn, $removedLabels, $removedNodeProperties) AS node, dn
   CREATE (:_CDCDeleteEvent {
     eventType: "NODE_DELETED",
     elementId: elementId(dn),
     labels: apoc.convert.toJson(apoc.node.labels(node)),
     properties: apoc.convert.toJson(apoc.any.properties(node)),
     timestamp: timestamp()
   })',
  {phase: 'before'})
```

#### 4.3.3 Trigger 3：删除事件捕获（关系）

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

#### 4.3.4 降级方案：仅 APOC Core（不安装 Extended）

如果无法安装 APOC Extended，可尝试在 `phase: 'before'` 阶段直接使用原生 Cypher 函数。此时节点尚未被删除，`labels()` 和 `properties()` 可能仍然可用：

```cypher
-- 降级方案（需在 Neo4j 2026.2.3 上实际验证）
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

> **⚠️ 验证要求：** 降级方案需在目标版本上实测。部分 Neo4j/APOC 版本中 `$deletedNodes` 传入的是虚拟引用，原生 `labels()` 可能返回空。如验证失败，必须安装 APOC Extended。

#### 4.3.5 验证步骤

部署后需在测试环境执行以下验证：

```cypher
-- 1. 创建测试节点
CREATE (n:TestNode {name: 'test', value: 42})

-- 2. 删除节点
MATCH (n:TestNode {name: 'test'}) DETACH DELETE n

-- 3. 检查中转节点是否被正确创建
MATCH (e:_CDCDeleteEvent) RETURN e

-- 预期结果:
-- e.eventType = "NODE_DELETED"
-- e.labels 包含 "TestNode"
-- e.properties 包含 {"name": "test", "value": 42}
```

### 4.5 索引策略

> **原则：** CDC 轮询和 Sync Applier 的 MERGE 都依赖属性查找。Neo4j 属性索引必须绑定标签。无索引时每次查询都是全表扫描，100ms 轮询间隔下将严重拖垮性能。

#### 4.5.1 主节点索引（由 HA Agent Primary 启动时创建）

```cypher
-- 每个业务标签都需要 _updated_at 索引，用于 CDC 增量轮询
-- 示例：如果业务中有 Person、Company、Document 等标签
CREATE RANGE INDEX idx_updated_Person IF NOT EXISTS FOR (n:Person) ON (n._updated_at)
CREATE RANGE INDEX idx_updated_Company IF NOT EXISTS FOR (n:Company) ON (n._updated_at)

-- 关系类型的 _updated_at 索引（Neo4j 5.x+ 支持）
CREATE RANGE INDEX idx_updated_KNOWS IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r._updated_at)
CREATE RANGE INDEX idx_updated_WORKS_AT IF NOT EXISTS FOR ()-[r:WORKS_AT]-() ON (r._updated_at)

-- _CDCDeleteEvent 中转节点索引（用于删除事件轮询 + 清理）
CREATE RANGE INDEX idx_cdc_delete_ts IF NOT EXISTS FOR (n:_CDCDeleteEvent) ON (n.timestamp)

-- _elementId 索引（用于 keyset pagination 排序和全量同步时快速查找）
CREATE RANGE INDEX idx_eid_Person IF NOT EXISTS FOR (n:Person) ON (n._elementId)
CREATE RANGE INDEX idx_eid_Company IF NOT EXISTS FOR (n:Company) ON (n._elementId)
```

#### 4.5.2 备节点索引（由 HA Agent Standby 启动时创建）

```cypher
-- 每个业务标签的 _elementId 索引，用于 Sync Applier 的 MERGE 查找
CREATE RANGE INDEX idx_eid_Person IF NOT EXISTS FOR (n:Person) ON (n._elementId)
CREATE RANGE INDEX idx_eid_Company IF NOT EXISTS FOR (n:Company) ON (n._elementId)

-- 关系类型的 _elementId 索引
CREATE RANGE INDEX idx_eid_KNOWS IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r._elementId)
```

#### 4.5.3 动态索引创建机制

业务标签不可能在设计时穷举。HA Agent 启动时需**动态扫描**当前数据库中的所有标签和关系类型，批量创建索引：

```java
// IndexInstaller.ensureIndexes() — 在 HA Agent 启动时执行
public class IndexInstaller {

    void ensureIndexes(Session session, NodeRole role) {
        // 1. 扫描所有标签
        List<String> labels = session.run("CALL db.labels() YIELD label RETURN label")
            .list(r -> r.get("label").asString());

        // 2. 扫描所有关系类型
        List<String> relTypes = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
            .list(r -> r.get("relationshipType").asString());

        // 3. 按角色创建索引
        for (String label : labels) {
            if (label.startsWith("_")) continue; // 跳过系统标签（_CDCDeleteEvent 单独处理）
            if (role == NodeRole.PRIMARY) {
                createIndex(session, "node", label, "_updated_at");
                createIndex(session, "node", label, "_elementId");
            } else {
                createIndex(session, "node", label, "_elementId");
            }
        }
        for (String relType : relTypes) {
            if (role == NodeRole.PRIMARY) {
                createIndex(session, "rel", relType, "_updated_at");
            }
            createIndex(session, "rel", relType, "_elementId");
        }

        // 4. _CDCDeleteEvent 专用索引（仅主节点）
        if (role == NodeRole.PRIMARY) {
            session.run("CREATE RANGE INDEX idx_cdc_delete_ts IF NOT EXISTS " +
                         "FOR (n:_CDCDeleteEvent) ON (n.timestamp)");
        }
    }
}
```

**运行时新标签处理：** 当 CDC Collector 或 Sync Applier 遇到尚未建索引的新标签时，触发 `IndexInstaller.ensureIndex(label)` 增量创建。首次查询可能走全扫，后续轮询即可命中索引。

#### 4.5.4 索引性能影响评估

| 操作 | 无索引 | 有索引 | 说明 |
|------|--------|--------|------|
| CDC 轮询 `WHERE _updated_at > $ts` | O(N) 全表扫描 | O(log N + k) | k = 本轮变更数 |
| Sync Applier `MERGE {_elementId: ...}` | O(N) 全表扫描 | O(log N) | 单次查找 |
| 删除事件扫描 `_CDCDeleteEvent` | O(M) | O(log M + k) | M = 中转节点数（通常极少） |
| 索引维护开销 | 无 | 每次写入增加 ~5-10% | 可接受，换取查询提速 |

---

### 4.4 删除事件处理：APOC Trigger + 中转节点

**原方案（已废弃）：** 软删除 + 异步清理 → 导致主节点存储膨胀、查询污染、关系级联残留。

**新方案：** APOC Trigger 在 `before` 阶段捕获删除快照，写入中转节点，主节点执行真删除。

```
业务执行: MATCH (n:Person {name:'Alice'}) DETACH DELETE n
      │
      ▼ (APOC Trigger, phase:before, 节点尚未删除)
      │
      ├── 1. apoc.trigger.toNode() 将节点转为虚拟节点
      ├── 2. 提取 labels + properties + elementId
      ├── 3. CREATE (:_CDCDeleteEvent {快照数据, timestamp})
      │
      ▼ (Trigger 执行完毕, Neo4j 继续原始事务)
      │
      ├── 4. Node 被真正 DETACH DELETE ✅ 主节点无残留
      │
      ▼ (CDC Collector 下一轮轮询, ~100ms后)
      │
      ├── 5. MATCH (e:_CDCDeleteEvent) WHERE e.timestamp > $lastTs
      ├── 6. 封装为 NODE_DELETED / REL_DELETED ChangeEvent
      ├── 7. XADD 到 Redis Stream
      ├── 8. DETACH DELETE e  (清理中转节点, 生命周期仅秒级)
      │
      ▼ (备节点 Sync Applier)
      │
      └── 9. MATCH (n {_elementId: $eid}) DETACH DELETE n
```

**中转节点 `_CDCDeleteEvent` 特性：**

| 属性 | 说明 |
|------|------|
| 生命周期 | 极短（100ms ~ 几秒，下次 CDC 轮询即消费并清理） |
| 存储开销 | 可忽略（瞬态存在，不积累） |
| 查询影响 | 零（业务查询不涉及 `_CDCDeleteEvent` 标签） |
| 失败兜底 | CDC Collector 启动时扫描清理残留中转节点 |

**对比旧方案（软删除）的改进：**

| 维度 | 软删除（已废弃） | APOC Trigger + 中转节点 |
|------|-----------------|------------------------|
| 主节点存储 | 持续膨胀 | **干净**（真删除，中转节点秒级清理） |
| 查询污染 | 每个查询需过滤 `_deleted` | **无污染** |
| 业务侵入性 | DELETE 改为 UPDATE | **零侵入**（Trigger 自动处理） |
| 关系级联 | 残留关系可被遍历 | **DETACH DELETE 干净删除** |
| 实现复杂度 | 清理任务 + 安全时间窗口 | Trigger 注册一次即可 |

---

## 5. Redis Stream 同步通道设计

### 5.1 Stream 结构

```
Key 命名规范: neo4j:cdc:{database}:{stream-type}

主要 Stream:
  neo4j:cdc:neo4j:changes        — 增量变更事件流（核心）
  neo4j:cdc:neo4j:fullsync       — 全量同步数据流
  neo4j:cdc:neo4j:control        — 控制指令流（切换/暂停/恢复）

辅助 Key:
  neo4j:ha:checkpoint:{node-id}  — 各备节点的消费位点 (Hash)
  neo4j:ha:leader-lock           — 主节点Leader锁 (String + TTL)
  neo4j:ha:fencing-token         — 当前Fencing Token (String)
  neo4j:ha:node-registry         — 节点注册表 (Hash)
  neo4j:ha:metrics               — 运行指标 (Hash)
```

### 5.2 ChangeEvent 消息格式

```json
{
  "eventId": "uuid-v7",
  "eventType": "NODE_CREATED | NODE_UPDATED | NODE_DELETED | REL_CREATED | REL_UPDATED | REL_DELETED",
  "database": "neo4j",
  "timestamp": 1712736000000,
  "fencingToken": 42,
  "txId": "tx-seq-12345",
  "entity": {
    "type": "NODE | RELATIONSHIP",
    "elementId": "4:xxx:123",
    "labels": ["Person", "Employee"],
    "properties": {
      "name": "Alice",
      "_updated_at": 1712736000000
    },
    "beforeState": {
      "name": "Alice_old"
    },
    "startNodeElementId": "4:xxx:100",
    "endNodeElementId": "4:xxx:200",
    "relationshipType": "KNOWS"
  },
  "metadata": {
    "sourceNode": "node-primary-01",
    "batchId": "batch-001",
    "batchSeq": 3,
    "batchSize": 10
  }
}
```

### 5.3 Consumer Group 设计

```
Stream: neo4j:cdc:neo4j:changes
  └── Consumer Group: neo4j-ha-sync
        ├── Consumer: standby-node-01   (备节点1)
        ├── Consumer: standby-node-02   (备节点2)
        └── Consumer: standby-node-03   (备节点3)

注意：每个备节点消费 ALL 消息（非分区），因为每个备节点需要完整回放。
实现方式：每个备节点使用独立的 Consumer Group，而非共享 Consumer Group。

实际结构：
Stream: neo4j:cdc:neo4j:changes
  ├── Consumer Group: sync-standby-01
  │     └── Consumer: applier
  ├── Consumer Group: sync-standby-02
  │     └── Consumer: applier
  └── Consumer Group: sync-standby-03
        └── Consumer: applier
```

**为什么每个备节点独立 Consumer Group？**
- 共享 Consumer Group 会导致消息被分发（每条消息只有一个消费者收到）
- 每个备节点都需要回放全量变更
- 独立 Group 使每个备节点可以独立维护自己的消费进度

### 5.4 发布流程（CDC Collector → Redis Stream）

```java
// 伪代码
void publishBatch(List<ChangeEvent> events) {
    // 1. 验证 Fencing Token（防止旧主继续发布）
    long currentToken = redis.get("neo4j:ha:fencing-token");
    if (currentToken != myFencingToken) {
        log.error("Fencing token mismatch, stepping down");
        stepDown();
        return;
    }

    // 2. Pipeline 批量 XADD
    Pipeline pipe = redis.pipelined();
    for (ChangeEvent event : events) {
        pipe.xadd(
            "neo4j:cdc:neo4j:changes",
            StreamEntryID.NEW_ENTRY,         // 自动生成 ID
            Map.of(
                "data", serialize(event),     // JSON 序列化
                "type", event.getEventType(),
                "ts", event.getTimestamp()
            ),
            XAddParams.xAddParams()
                .maxLen(100000)               // 保留最近10万条
                .approximateTrimming()        // 近似裁剪（性能更好）
        );
    }
    pipe.sync();

    // 3. 更新 checkpoint
    redis.hset("neo4j:ha:checkpoint:primary",
        "lastPublishedTs", String.valueOf(events.getLast().getTimestamp()),
        "lastStreamId", lastStreamId.toString()
    );
}
```

### 5.5 消费流程（Redis Stream → Sync Applier）

```java
// 伪代码
void consumeLoop() {
    String lastId = loadCheckpoint();  // 从 Redis Hash 恢复
    if (lastId == null) lastId = "0";  // 首次从头消费

    while (running) {
        // 1. 阻塞读取（最多等待 blockMs）
        List<StreamEntry> entries = redis.xreadgroup(
            GROUP, CONSUMER,
            XReadGroupParams.xReadGroupParams()
                .count(100)           // 每批最多100条
                .block(1000),         // 阻塞1秒
            Map.of("neo4j:cdc:neo4j:changes", ">")  // ">" 表示只读新消息
        );

        if (entries == null || entries.isEmpty()) continue;

        // 2. 批量回放
        try (Transaction tx = neo4jSession.beginTransaction()) {
            for (StreamEntry entry : entries) {
                ChangeEvent event = deserialize(entry.getFields().get("data"));
                applyChange(tx, event);
            }
            tx.commit();
        }

        // 3. 批量 ACK
        StreamEntryID[] ids = entries.stream()
            .map(StreamEntry::getID)
            .toArray(StreamEntryID[]::new);
        redis.xack("neo4j:cdc:neo4j:changes", GROUP, ids);

        // 4. 更新 checkpoint
        saveCheckpoint(ids[ids.length - 1].toString());
    }
}
```

### 5.6 背压处理

| 场景 | 检测方式 | 处理策略 |
|------|---------|---------|
| 备节点消费慢 | PEL 长度 > 阈值 | 告警 + 减小主节点发布批量 |
| Redis 内存不足 | `INFO memory` 监控 | MAXLEN 裁剪 + 触发全量同步标记 |
| 备节点长时间离线 | checkpoint 与 Stream 头部差距过大 | 放弃增量同步，触发全量同步 |
| 网络抖动 | XREADGROUP 超时 | 自动重连 + 从 checkpoint 恢复 |

**MAXLEN 策略：**
```
XADD neo4j:cdc:neo4j:changes MAXLEN ~ 100000 * field value
```
- 保留最近约 10 万条消息
- 使用 `~` 近似裁剪，避免精确裁剪的性能开销
- 当备节点的 checkpoint 落后于 Stream 最早消息时，需触发全量同步

---

## 6. 数据同步协议设计

### 6.1 同步模式总览

```
┌────────────────┐     ┌──────────────────┐     ┌────────────────┐
│   全量同步      │────▶│   增量同步        │────▶│  稳态运行       │
│  (Full Sync)   │     │  (Incremental)   │     │  (Steady)      │
└────────────────┘     └──────────────────┘     └────────────────┘
       ▲                       ▲                        │
       │                       │                        │
       │     ┌─────────────────┼────────────────────────┘
       │     │ checkpoint 落后过多
       │     │ 或 Stream 数据已裁剪
       └─────┘
```

### 6.2 全量同步流程

**触发条件：**
- 备节点首次加入集群
- 备节点 checkpoint 落后于 Stream 最早消息（数据已被 TRIM）
- 手动触发（运维 API）
- Failover 后旧主降级需要追赶

**流程：**

```
Primary                          Redis Stream                    Standby
   │                                  │                             │
   │  1. 暂停增量CDC                   │                             │
   │──────────────────────────────────▶│  2. 发送 FULL_SYNC_START    │
   │                                  │────────────────────────────▶│
   │  3. 记录当前 txId 为同步快照点     │                             │
   │                                  │                             │
   │  4. 分批导出全量数据               │                             │
   │  MATCH (n) RETURN n              │                             │
   │  ORDER BY id(n) SKIP $offset     │                             │
   │  LIMIT $batchSize                │                             │
   │──────────────────────────────────▶│  5. 写入 fullsync stream   │
   │          (重复 4-5 直到完成)       │────────────────────────────▶│
   │                                  │                             │  6. 清空本地
   │                                  │                             │     数据库
   │                                  │                             │  7. 批量导入
   │                                  │                             │     MERGE/CREATE
   │──────────────────────────────────▶│  8. FULL_SYNC_END          │
   │                                  │────────────────────────────▶│
   │  9. 恢复增量CDC                   │                             │
   │  （从快照点 txId 开始）            │                             │  10. 切换到
   │                                  │                             │      增量消费
```

**全量同步数据格式：**
```json
{
  "syncType": "FULL_SYNC_BATCH",
  "batchIndex": 42,
  "totalBatches": 100,
  "snapshotTxId": "tx-50000",
  "entities": [
    {
      "type": "NODE",
      "elementId": "4:xxx:123",
      "labels": ["Person"],
      "properties": {"name": "Alice", "_updated_at": 1712736000000}
    }
  ]
}
```

### 6.3 增量同步流程

稳态下的增量同步即 §5.4 / §5.5 描述的发布-消费循环。

**关键保证：**
- **有序性**：Redis Stream 天然保证消息顺序（单 Stream 单生产者）
- **至少一次**：Consumer Group + PEL + XACK 保证不丢消息
- **幂等性**：Sync Applier 使用 `MERGE` 而非 `CREATE`，保证重复回放幂等

### 6.4 断点续传

```
Checkpoint 存储结构 (Redis Hash):

  === CDC Collector (主节点) ===
  Key: neo4j:ha:checkpoint:primary
  Fields:
    lastPublishedTs       — 最后发布事件的时间戳
    lastPublishedEid      — 最后发布事件的 _elementId（keyset 游标）
    lastDeleteTs          — 最后处理的删除事件时间戳
    lastDeleteEid         — 最后处理的删除事件 elementId（keyset 游标）
    lastStreamId          — 最后写入的 Stream Message ID

  === Sync Applier (备节点) ===
  Key: neo4j:ha:checkpoint:{node-id}
  Fields:
    lastStreamId          — 最后成功消费的 Stream Message ID
    lastEventTs           — 最后事件的时间戳
    syncMode              — INCREMENTAL | FULL_SYNC | CATCHING_UP
    lastFullSyncAt        — 上次全量同步完成时间
    pendingCount          — PEL 中待确认消息数
```

**恢复逻辑：**
1. Sync Applier 启动时读取 `neo4j:ha:checkpoint:{node-id}`
2. 检查 `lastStreamId` 是否仍在 Stream 中（`XRANGE lastStreamId lastStreamId`）
3. 如果存在 → 从 `lastStreamId` 之后继续增量消费
4. 如果不存在（已被 TRIM） → 触发全量同步

---

## 7. 故障检测与自动 Failover

### 7.1 健康检查设计

**多层探活策略（由浅入深）：**

| 层级 | 方法 | 检测内容 | 间隔 | 超时 |
|------|------|---------|------|------|
| L1 | TCP Connect | 端口 7687 可达 | 1s | 2s |
| L2 | Bolt Handshake | Bolt 协议握手成功 | 2s | 3s |
| L3 | Cypher Query | `RETURN 1` 执行成功 | 5s | 5s |
| L4 | Write Check | 主节点写测试 `CREATE (n:_HealthCheck) DELETE n` | 10s | 10s |

**故障判定规则：**
```
L1 连续失败 3 次 → 标记 SUSPECT
L2 连续失败 3 次 → 标记 SUSPECT
L3 连续失败 2 次 → 标记 UNHEALTHY
L4 连续失败 2 次 → 标记 DOWN（仅主节点）

状态机：
HEALTHY → SUSPECT → UNHEALTHY → DOWN
   ▲                                │
   └────── 任一层级恢复成功 ──────────┘
```

### 7.2 Leader 选举（Redis 分布式锁）

```
Leader Lock:
  Key:    neo4j:ha:leader-lock
  Value:  {nodeId}:{fencingToken}
  TTL:    15s (每5s续期一次)

选举流程：
  SET neo4j:ha:leader-lock {nodeId}:{token} NX EX 15

续期：
  // Lua 脚本保证原子性
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
  else
    return 0
  end
```

### 7.3 Failover 完整流程

> **v2.0 变更：** 所有步骤在同一个 HA Agent 进程内执行，无需跨进程 control stream 通信。

```
                    ┌─────────────────────┐
                    │  检测到主节点 DOWN    │
                    │  (HealthChecker)     │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 等待确认期 (5s)      │
                    │ 避免网络抖动误判      │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
              No    │ 主节点是否恢复?      │  Yes → 取消Failover
              ◄─────└──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 1. 递增 Fencing Token│
                    │ fencingTokenManager  │
                    │   .increment()      │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 2. 停止数据同步      │
                    │ cdcCollector.stop()  │
                    │ syncApplier.stop()   │
                    │ syncApplier          │
                    │   .drainPending()   │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 3. 选择最佳备节点    │
                    │ (checkpoint最新的)   │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 4. 切换 CDC 目标     │
                    │ cdcCollector         │
                    │  .switchTarget(新主) │
                    │ apocTriggerInstaller │
                    │  .install(新主)      │
                    │ indexInstaller       │
                    │  .ensureIndexes(新主)│
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 5. 重启数据同步      │
                    │ cdcCollector.start() │
                    │ (旧主恢复后变为备节点│
                    │  自动启动Sync回放)   │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 6. 更新 HAProxy 路由 │
                    │ haProxyUpdater       │
                    │  .switchPrimary()   │
                    │ (Runtime API 直接    │
                    │  切换，无需 reload)  │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 7. 更新集群状态      │
                    │ nodeRegistry         │
                    │  .updateRoles()     │
                    │ 记录审计日志          │
                    │ 发送告警通知          │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │ 8. 清理旧主          │
                    │ (best-effort)       │
                    │ 旧主可达时:           │
                    │  卸载 APOC Trigger   │
                    │  清理 _CDCDeleteEvent│
                    │ 旧主不可达时:         │
                    │  标记 pendingCleanup │
                    │  恢复后再处理        │
                    └─────────────────────┘
```

### 7.4 Fencing Token 防脑裂

```
场景：网络分区后旧主恢复，试图继续发布 CDC 事件

防护机制：
1. 每次 Failover 递增 Fencing Token（单调递增整数）
2. CDC Collector 每次 XADD 前检查自己持有的 Token 是否等于 Redis 中的当前 Token
3. Sync Applier 每次回放前检查事件中的 Token 是否 >= 自己记录的最新 Token
4. 如果 Token 不匹配，拒绝操作并触发 step-down

Lua 脚本（原子检查+发布）：
  local currentToken = redis.call("GET", "neo4j:ha:fencing-token")
  if currentToken ~= ARGV[1] then
    return -1  -- Token mismatch, reject
  end
  return redis.call("XADD", KEYS[1], "*", "data", ARGV[2])
```

### 7.5 旧主恢复与降级流程

> **v2.0 变更：** 旧主自身不运行任何 Agent 进程，恢复检测和角色切换由集中式 HA Agent 统一处理。
> **v2.3 补充：** 增加 APOC Trigger 卸载和 `_CDCDeleteEvent` 残留清理步骤，确保旧主降级后不会继续产生中转节点。

```
HA Agent HealthChecker 探测到旧主恢复（L1→L2→L3 均通过）
  │
  ├── Step 1: 卸载旧主上的 APOC Trigger（3 个）
  │     CALL apoc.trigger.drop('neo4j', 'cdc-timestamp')
  │     CALL apoc.trigger.drop('neo4j', 'cdc-capture-node-deletes')
  │     CALL apoc.trigger.drop('neo4j', 'cdc-capture-rel-deletes')
  │     → 防止旧主作为备节点后继续在写入时产生 _CDCDeleteEvent
  │
  ├── Step 2: 清理残留 _CDCDeleteEvent 中转节点
  │     MATCH (e:_CDCDeleteEvent) ... DETACH DELETE e
  │     → 清除 Failover 期间 / 宕机前积累的未消费中转节点
  │
  ├── Step 3: 更新角色 → STANDBY, serviceState → SYNCING
  │     → HAProxy 读 backend 设为 maint
  │
  ├── Step 4: 确保备节点索引就绪（_elementId 索引）
  │
  ├── Step 5: 评估数据差距
  │     ├── checkpoint 有效且 Stream 中有对应消息 → 增量同步
  │     └── checkpoint 过期或不存在 → 全量同步
  │
  ├── Step 6: 启动 Sync Applier 对旧主进行数据同步
  │
  ├── Step 7: 等待 syncLag < 阈值 → serviceState → ONLINE
  │     → HAProxy 读 backend 设为 ready
  │
  └── Step 8: 清除 pendingCleanup 标记，记录审计日志
```

**为什么必须卸载旧主的 APOC Trigger？**

旧主降级为 Standby 后，Sync Applier 会向其写入同步数据（MERGE 回放）。如果 APOC Trigger 仍然存在：
- `cdc-timestamp` Trigger 会覆盖 `_updated_at` 为备节点本地时间戳，与主节点不一致
- `cdc-capture-node-deletes` / `cdc-capture-rel-deletes` 会在备节点上产生无用的 `_CDCDeleteEvent` 中转节点
- 这些中转节点永远不会被消费（CDC Collector 只轮询主节点），导致存储泄漏

**为什么必须清理残留 `_CDCDeleteEvent`？**

Failover 瞬间（Phase 3 停止 CDC Collector 到 Phase 8 清理旧主之间），旧主上可能存在未被 CDC 消费的 `_CDCDeleteEvent` 中转节点。这些来源于：
1. Failover 前 CDC 最后一次轮询到 Trigger 产生新中转节点之间的时间窗口
2. HAProxy 路由切换前客户端仍在旧主上执行的删除操作

清理是安全的——旧主的数据将通过全量/增量同步从新主覆盖，这些中转节点记录的删除事件已经通过新主的 CDC 链路传播（或者新主上根本不存在这些被删的节点）。

详见 HA Agent 设计文档 §12（旧主恢复与降级）。

---

## 8. 客户端路由与 HAProxy 多活

> **v2.1 变更：** HAProxy 从单实例改为多活部署。多个 HAProxy 实例使用相同配置独立运行，客户端配置多地址 fallback。HA Agent 通过各实例的 admin socket 统一管理路由状态并定期同步。

### 8.1 HAProxy 多活架构

**设计原则：** HAProxy 是纯无状态 TCP 代理，天然适合多活。每个实例加载相同的 `haproxy.cfg`，各自独立运行，互不感知。

```
客户端连接流程:
  1. 尝试连接 HAProxy-1 (bolt://haproxy-1:17687)
  2. 如果连接失败 → 自动切换到 HAProxy-2 (bolt://haproxy-2:27687)
  3. 任一 HAProxy 实例都能独立完成完整的读写路由
```

**客户端 fallback 示例：**

```python
# Python
WRITE_URIS = ["bolt://haproxy-1:17687", "bolt://haproxy-2:27687"]

def get_write_driver():
    for uri in WRITE_URIS:
        try:
            driver = GraphDatabase.driver(uri, auth=("neo4j", password))
            driver.verify_connectivity()
            return driver
        except ServiceUnavailable:
            continue
    raise Exception("All HAProxy instances unavailable")
```

```java
// Java
List<String> writeUris = List.of("bolt://haproxy-1:17687", "bolt://haproxy-2:27687");

Driver createWriteDriver() {
    for (String uri : writeUris) {
        try {
            var driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", password));
            driver.verifyConnectivity();
            return driver;
        } catch (ServiceUnavailableException e) {
            continue;
        }
    }
    throw new RuntimeException("All HAProxy instances unavailable");
}
```

### 8.2 HAProxy 配置模板

所有 HAProxy 实例使用完全相同的配置文件，不需要任何差异化：

```haproxy
global
    stats socket /var/run/haproxy/admin.sock mode 660 level admin expose-fd listeners

# 写流量 → 主节点
frontend neo4j_write
    bind *:7687
    default_backend neo4j_primary

backend neo4j_primary
    option tcp-check
    server neo4j-primary neo4j-primary:7687 check inter 2s fall 3 rise 2
    server neo4j-standby neo4j-standby:7687 check inter 2s fall 3 rise 2 backup disabled

# 读流量 → 所有节点（负载均衡）
frontend neo4j_read
    bind *:7688
    default_backend neo4j_all

backend neo4j_all
    balance roundrobin
    option tcp-check
    server neo4j-primary neo4j-primary:7687 check inter 2s fall 3 rise 2
    server neo4j-standby neo4j-standby:7687 check inter 2s fall 3 rise 2
```

### 8.3 HA Agent Admin API

集中式 HA Agent 暴露一组管理端点（供运维和监控使用）：

| 端点 | 返回 | 用途 |
|------|------|------|
| `GET /health` | 200/503 | HA Agent 自身存活 |
| `GET /cluster/status` | JSON | 集群所有节点状态、角色、同步延迟、HAProxy 实例状态 |
| `GET /cluster/nodes/{id}` | JSON | 单节点详细状态 |
| `POST /cluster/failover` | 200 | 手动触发 Failover（需 Admin Token） |
| `GET /metrics` | Prometheus | Prometheus 指标 |

### 8.4 Failover 时路由切换（多 HAProxy 实例）

```
HA Agent（集中式进程内）:
  1. FailoverOrchestrator 确定新主节点
  2. HaProxyUpdater 遍历所有 HAProxy 实例，逐个通过 admin socket 发送命令:
     for each haproxy in instances:
       - "set server neo4j_primary/neo4j-primary state drain"   (旧主排空)
       - "set server neo4j_primary/neo4j-standby state ready"   (新主上线)
       - "set server neo4j_primary/neo4j-primary state maint"   (旧主维护)
       - 个别实例发送失败不阻塞，记录日志，后续由 StateSyncer 补偿
  3. 所有 HAProxy 立即生效（无需 reload），新连接路由到新主
  4. 已有连接断开后重连到新主节点
```

### 8.5 HAProxy 状态同步机制

**问题场景：** Failover 后某个 HAProxy 实例重启，它会从初始配置启动（neo4j-primary 为 active），但实际的主节点可能已经切换到 neo4j-standby。

**解决方案：** HA Agent 内置 `HaProxyStateSyncer`，定期将当前集群角色推送到所有 HAProxy 实例。

```
HaProxyStateSyncer 定时任务（每 10s 执行一次）:
  1. 从 ClusterStateManager 获取当前主节点 ID
  2. 遍历所有 HAProxy 实例的 admin socket
  3. 查询每个 HAProxy 实例当前的 server state（show servers state）
  4. 如果 state 与预期不一致 → 发送修正命令
  5. 如果某个实例 socket 不可达 → 记录日志 + metric，等下次重试
```

这保证了：
- HAProxy 重启后在 10s 内自动修正到正确路由状态
- HA Agent 与某个 HAProxy 之间网络临时中断后自动恢复
- Failover 时个别 HAProxy 未收到命令，也会在下一轮同步中修正

### 8.6 多集群扩展

HAProxy 是通用 TCP 代理，支持同时为多个 Neo4j 主备集群提供路由。只需为每个集群分配不同的端口，在同一份 `haproxy.cfg` 中配置多组 frontend/backend：

```haproxy
# ============ 集群 A（例：知识图谱） ============
frontend cluster_a_write
    bind *:7687
    default_backend cluster_a_primary

backend cluster_a_primary
    option tcp-check
    server a-primary neo4j-a-primary:7687 check inter 2s fall 3 rise 2
    server a-standby neo4j-a-standby:7687 check inter 2s fall 3 rise 2 backup disabled

frontend cluster_a_read
    bind *:7688
    default_backend cluster_a_all

backend cluster_a_all
    balance roundrobin
    option tcp-check
    server a-primary neo4j-a-primary:7687 check inter 2s fall 3 rise 2
    server a-standby neo4j-a-standby:7687 check inter 2s fall 3 rise 2

# ============ 集群 B（例：用户关系） ============
frontend cluster_b_write
    bind *:7689
    default_backend cluster_b_primary

backend cluster_b_primary
    option tcp-check
    server b-primary neo4j-b-primary:7687 check inter 2s fall 3 rise 2
    server b-standby neo4j-b-standby:7687 check inter 2s fall 3 rise 2 backup disabled

frontend cluster_b_read
    bind *:7690
    default_backend cluster_b_all

backend cluster_b_all
    balance roundrobin
    option tcp-check
    server b-primary neo4j-b-primary:7687 check inter 2s fall 3 rise 2
    server b-standby neo4j-b-standby:7687 check inter 2s fall 3 rise 2
```

**客户端按业务连不同端口：**

```
知识图谱业务 → bolt://haproxy:7687（写）/ bolt://haproxy:7688（读）
用户关系业务 → bolt://haproxy:7689（写）/ bolt://haproxy:7690（读）
```

**HA Agent 配合扩展：** 在 `ha-agent.yml` 的 `cluster.nodes` 中声明多个集群的节点，并在 `haproxy.primaryBackend` 中指定对应的 backend 名称，HA Agent 即可在 Failover 时精确控制各集群的路由切换。各集群独立管理，互不影响。

---

## 9. 监控告警设计

### 9.1 Prometheus Metrics

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `neo4j_ha_node_role` | Gauge | 节点角色 (1=primary, 0=standby) |
| `neo4j_ha_node_health` | Gauge | 健康状态 (0-3: healthy/suspect/unhealthy/down) |
| `neo4j_ha_node_service_state` | Gauge | 服务状态 (0=OFFLINE, 1=SYNCING, 2=ONLINE)，只有 ONLINE 的节点才接收读流量和作为 Failover 候选 |
| `neo4j_ha_cdc_events_published_total` | Counter | CDC 已发布事件总数 |
| `neo4j_ha_cdc_events_consumed_total` | Counter | 已消费事件总数 |
| `neo4j_ha_cdc_events_applied_total` | Counter | 已回放事件总数 |
| `neo4j_ha_sync_lag_ms` | Gauge | 同步延迟（毫秒） |
| `neo4j_ha_sync_lag_events` | Gauge | 同步延迟（事件数） |
| `neo4j_ha_stream_length` | Gauge | Redis Stream 长度 |
| `neo4j_ha_stream_pending` | Gauge | PEL 待确认消息数 |
| `neo4j_ha_failover_total` | Counter | Failover 次数 |
| `neo4j_ha_failover_duration_ms` | Histogram | Failover 耗时 |
| `neo4j_ha_fullsync_total` | Counter | 全量同步次数 |
| `neo4j_ha_fullsync_duration_ms` | Histogram | 全量同步耗时 |
| `neo4j_ha_fencing_token` | Gauge | 当前 Fencing Token |
| `neo4j_ha_haproxy_instance_reachable` | Gauge | HAProxy 实例可达性 (1=可达, 0=不可达)，按 instance 标签区分 |
| `neo4j_ha_haproxy_state_sync_total` | Counter | HAProxy 状态同步执行次数 |
| `neo4j_ha_haproxy_state_sync_fix_total` | Counter | HAProxy 状态修正次数（检测到不一致并修正） |
| `neo4j_ha_backup_state` | Gauge | 备份状态 (0=IDLE, 1=PREPARING, 2=IN_PROGRESS) |
| `neo4j_ha_backup_last_success_timestamp` | Gauge | 上次成功备份的 Unix 时间戳 |
| `neo4j_ha_backup_duration_ms` | Gauge | 上次备份持续时间（prepare 到 complete） |

### 9.2 告警规则

| 告警名 | 条件 | 严重级别 | 说明 |
|--------|------|---------|------|
| Neo4jHAPrimaryDown | `neo4j_ha_node_health{role="primary"} >= 3` for 10s | CRITICAL | 主节点宕机 |
| Neo4jHASyncLagHigh | `neo4j_ha_sync_lag_ms > 5000` for 30s | WARNING | 同步延迟过高 |
| Neo4jHASyncLagCritical | `neo4j_ha_sync_lag_ms > 30000` for 30s | CRITICAL | 同步严重滞后 |
| Neo4jHAStreamOverflow | `neo4j_ha_stream_length > 80000` | WARNING | Stream 即将达到上限 |
| Neo4jHAPendingHigh | `neo4j_ha_stream_pending > 1000` for 60s | WARNING | 消费积压 |
| Neo4jHAFailoverOccurred | `increase(neo4j_ha_failover_total[5m]) > 0` | INFO | 发生了主备切换 |
| Neo4jHANoStandby | `count(neo4j_ha_node_role == 0) == 0` for 60s | CRITICAL | 没有可用备节点 |
| Neo4jHANoOnlineStandby | `count(neo4j_ha_node_service_state{role="standby"} == 2) == 0` for 60s | WARNING | 没有 ONLINE 状态的备节点（无法 Failover，读流量全在主节点） |
| Neo4jHASyncingTooLong | `neo4j_ha_node_service_state == 1` for 30m | WARNING | 备节点 SYNCING 超过 30 分钟未上线 |
| Neo4jHAProxyDown | `neo4j_ha_haproxy_instance_reachable == 0` for 30s | WARNING | 某个 HAProxy 实例不可达 |
| Neo4jHAProxyAllDown | `sum(neo4j_ha_haproxy_instance_reachable) == 0` for 10s | CRITICAL | 所有 HAProxy 实例不可达 |
| Neo4jHABackupOverdue | `time() - neo4j_ha_backup_last_success_timestamp > 172800` | WARNING | 超过 2 天未成功备份 |
| Neo4jHABackupStuck | `neo4j_ha_backup_state == 2` for 2h | WARNING | 备份操作超时未完成（Sync Applier 被挂起过久） |

### 9.3 Grafana Dashboard

核心面板：
1. **集群拓扑图** — 主/备状态、角色、连接数
2. **同步延迟趋势** — 时间序列，延迟 ms + 事件数
3. **CDC 吞吐量** — 发布/消费/回放 速率 (events/s)
4. **Redis Stream 状态** — 长度、PEL、内存占用
5. **Failover 事件时间线** — 切换历史 + 耗时
6. **节点健康状态** — 各层级探活成功率
7. **备份状态** — 上次备份时间、备份耗时、备份状态

---

## 10. 配置管理

### 10.1 配置文件结构

> **v2.0 变更：** 从单节点 Sidecar 配置改为集群级配置。不再需要 `nodeId` 和 `role`，改为 `cluster.nodes` 列表。

```yaml
# config/agent/ha-agent.yml
cluster:
  nodes:
    - id: "node-01"
      role: "primary"
      neo4j:
        uri: "bolt://neo4j-primary:7687"
        username: "neo4j"
        password: "${NEO4J_PASSWORD}"
        database: "neo4j"
    - id: "node-02"
      role: "standby"
      neo4j:
        uri: "bolt://neo4j-standby:7687"
        username: "neo4j"
        password: "${NEO4J_PASSWORD}"
        database: "neo4j"

  haproxy:
    instances:
      - id: "haproxy-1"
        adminSocket: "/var/run/haproxy-1/admin.sock"
      - id: "haproxy-2"
        adminSocket: "/var/run/haproxy-2/admin.sock"
    stateSyncInterval: 10s

redis:
  mode: "standalone"            # 复用平台已有 Redis 实例
  standalone:
    host: "${REDIS_HOST:-localhost}"
    port: ${REDIS_PORT:-6379}
  password: "${REDIS_PASSWORD}"

cdc:
  enabled: true
  mode: "cypher-polling"      # cypher-polling | tx-log | apoc-trigger
  poll:
    interval: 100ms
    batchSize: 500
    timeout: 5s
  timestampField: "_updated_at"
  index:
    autoCreate: true
    skipSystemLabels:
      - "_CDCDeleteEvent"
      - "_HealthCheck"

sync:
  consumer:
    group: "sync-applier"
    batchSize: 100
    blockTimeout: 1000ms
  apply:
    mode: "merge"             # merge | create-or-replace
    batchCommit: true
    maxRetries: 3
    retryDelay: 1s

stream:
  key: "neo4j:cdc:neo4j:changes"
  maxLen: 100000
  trimStrategy: "approximate"

failover:
  healthCheck:
    interval: 2s
    timeout: 5s
    failThreshold: 3
    successThreshold: 2
  fencingToken:
    key: "neo4j:ha:fencing-token"
  drainTimeout: 5s
  confirmationWait: 5s

backup:
  maxDuration: 2h             # 备份超时自动恢复 Sync Applier
  checkpointKey: "neo4j:ha:backup-checkpoint"

admin:
  port: 8080
  token: "${ADMIN_TOKEN}"

monitoring:
  prometheus:
    enabled: true
    port: 9090
    path: "/metrics"
  logging:
    level: "INFO"
    format: "json"
    file: "/var/log/neo4j-ha/agent.log"
```

---

## 11. 安全设计

| 层面 | 措施 |
|------|------|
| Neo4j 认证 | Bolt 协议原生用户名/密码 |
| Redis 认证 | requirepass + ACL（限制 HA Agent 只能操作 `neo4j:*` 前缀 Key） |
| 通信加密 | Redis TLS（可选）、内网部署 |
| 配置敏感项 | 密码通过环境变量注入，不硬编码 |
| Admin API | Token-based 认证 |
| 网络隔离 | 同步通道走专用 VLAN / 安全组 |

---

## 12. 灾备恢复

本节描述各组件故障后的系统行为、自动恢复机制和运维操作。

### 12.1 Neo4j 主节点永久下线

| 阶段 | 时间 | 行为 |
|------|------|------|
| 故障检测 | 0-6s | HA Agent HealthChecker L1-L4 逐层探活失败，标记 SUSPECT → UNHEALTHY → DOWN |
| 确认等待 | 6-11s | confirmationWait (5s)，防止网络抖动误判 |
| Failover 执行 | 11-13s | 递增 Fencing Token → 停止 CDC/Sync → 提升备节点为新主 → 切换 HAProxy → 更新 node-registry |
| 稳态 | 13s+ | 系统以单节点模式运行，读写都在新主上 |

**稳态影响：**
- 新主节点正常提供读写服务
- CDC Collector 在新主上运行，但没有 Sync Applier 目标（无备节点）
- HA Agent 持续探活旧主（每 2s 一次），发现不可达就跳过
- 告警 `Neo4jHANoStandby` 触发，通知运维补充新节点

**运维恢复：** 部署一台新的 Neo4j 实例作为 Standby，加入集群配置。HA Agent 自动检测到新节点后根据 checkpoint 判断增量或全量同步。

### 12.2 Neo4j 备节点永久下线

不触发 Failover，影响更小：

| 组件 | 行为 |
|------|------|
| 主节点 | 不受影响，正常读写 |
| HAProxy 读 backend | TCP check 自动摘除备节点，读流量全部到主节点 |
| CDC Collector | 正常运行，持续发布变更到 Redis Stream |
| Sync Applier | 已停止，Redis Stream 中事件持续积累（受 `maxLen: 100000` 限制自动 trim） |

**运维恢复：** 部署新 Standby 节点后，HA Agent 从 checkpoint 判断：Stream 中消息仍在 → 增量追赶；已被 trim 超过 checkpoint 位点 → 自动触发全量同步。

### 12.3 HA Agent 下线与恢复

**HA Agent 是无状态进程，所有关键状态持久化在 Redis 中。**

**下线期间的影响：**

| 功能 | 影响 |
|------|------|
| Neo4j 主节点读写 | **不受影响**（APOC Trigger 在 Neo4j 内部运行，变更继续写入系统属性） |
| HAProxy 路由 | **不受影响**（已建立的路由不变） |
| 数据同步 | **暂停**（CDC Collector 和 Sync Applier 随 Agent 停止） |
| 健康检查 | **暂停**（无人探活 Neo4j 节点） |
| Failover | **无法执行**（如果此时主节点也宕机，无法自动切换） |

**自动恢复流程（Docker restart: always，通常 1-3 秒重启）：**

```
HaAgent 重启:
  1. 从 Redis node-registry 恢复集群角色（谁是 PRIMARY/STANDBY/DOWN）
  2. 从 Redis checkpoint 恢复 CDC 同步位点（_updated_at + _elementId 游标）
  3. 从 Redis fencing-token 恢复当前 Fencing Token
  4. 重新连接所有 Neo4j 节点（远程 Bolt），验证连通性
  5. 重启 CDC Collector（从 checkpoint 继续，自动追赶下线期间积累的变更）
  6. 重启 Sync Applier（从 Stream 位点继续）
  7. 重启 HealthChecker + HaProxyStateSyncer
  整个恢复过程：约 5-10 秒，无需人工干预
```

**下线期间变更不丢失：** Neo4j 主节点的 APOC Trigger 仍在运行，每次写操作仍设置 `_updated_at`。Agent 恢复后 CDC Collector 从 checkpoint 位点继续轮询，自动追赶所有缺失的变更。

### 12.4 HAProxy 实例下线与恢复

**场景 A：容器崩溃后原地重启**

`restart: always` 自动拉起，容器名、网络地址、端口不变。客户端无需改动。HA Agent 的 HaProxyStateSyncer 在下一轮同步中（10s 内）自动修正路由状态。

**场景 B：容器销毁后在同一主机重建**

```bash
docker compose up -d haproxy-1
```

Docker Compose 使用相同服务名和网络地址重建。客户端无需改动。

**场景 C：HAProxy 迁移到新主机（IP 变更）**

另一个 HAProxy 实例仍在服务，客户端 fallback 后已在使用。此时：
- 如果客户端配置的是**域名** → 更新 DNS 解析指向新机器即可，客户端无需改动
- 如果客户端配置的是 **IP** → 需要更新客户端配置中的地址

**建议：客户端统一使用域名配置 HAProxy 地址，避免机器更换时修改客户端配置。**

### 12.5 Redis 下线与恢复

> Redis 复用平台已有实例，其自身高可用由平台统一保障。本节描述 Redis 不可用以及 Redis 数据丢失场景下的 HA Agent 行为。

#### 12.5.1 Redis 在项目中的角色

Redis **不是**一个可选的缓存加速层，它承载了整个 HA 控制面的**权威状态**：

| Redis Key | 类型 | 存储内容 | 丢失后的后果 |
|-----------|------|---------|------------|
| `neo4j:ha:node-registry` | Hash | 每个节点的 role / health / serviceState / boltUri | **Agent 启动时按 `ha-agent.yml` 回退 → 可能把陈旧节点选为主** |
| `neo4j:ha:fencing-token` | String | 单调递增整数（防脑裂 token） | 回落到 0 → 旧主复活可能绕过 fencing 发布事件 |
| `neo4j:ha:cdc-checkpoint:<nodeId>` | Hash | CDC 轮询游标 (lastTs, lastElementId, lastStreamId, updatedAt) | 主库被从头扫描，海量重复事件压垮 Redis |
| `neo4j:ha:sync-checkpoint:<nodeId>` | Hash | Stream 消费游标 (lastStreamId, lastEventTs, syncMode) | 备节点触发不必要的全量同步 |
| `neo4j:cdc:neo4j:changes` | Stream | 增量变更事件流 | 所有尚未 ACK 的事件丢失，触发全量修复 |

其中 `node-registry` 是**最不能丢的**：它是"上一次集群中谁是主"的唯一记录。丢失后 Agent 只能退回到 `ha-agent.yml` 的 `role` 字段，而该字段**不会随 switchover 自动更新**，可能把数据陈旧的节点再次选为主，**造成静默数据回滚**。

#### 12.5.2 必备的 Redis 持久化配置

**生产环境对接的 Redis 必须满足：**

1. **开启 AOF（Append-Only File）持久化**，推荐 `appendfsync everysec`（性能与持久性平衡）
2. **同时开启 RDB 快照**作为 AOF 的补充（防 AOF 损坏）
3. **Redis 主从 / Sentinel / Cluster 高可用**由平台保障；本项目**不自建 Redis 高可用**
4. **备份策略**：至少每日一次 AOF + RDB 快照异地备份，保留 ≥7 天
5. **变更约束**：任何涉及 `neo4j:ha:*` 或 `neo4j:cdc:*` 前缀 Key 的运维操作（迁移、清理、FLUSHDB）都必须**先停止 HA Agent**，在测试环境演练过

**检查当前 Redis 持久化配置：**

```bash
redis-cli CONFIG GET appendonly      # 应返回 "yes"
redis-cli CONFIG GET save            # 应返回非空（RDB 快照规则）
redis-cli CONFIG GET appendfsync     # 推荐 "everysec"
```

如果平台提供的 Redis 不满足上述要求，**必须与平台侧沟通开启持久化**，不得上线生产。

#### 12.5.3 不同 Redis 故障场景的 Agent 行为

| 场景 | Agent 侧行为 | 数据面影响 | 是否需要人工介入 |
|------|-------------|-----------|----------------|
| Redis 短暂不可用（秒级抖动） | CDC Collector 将事件暂存到本地 PublishBuffer（磁盘文件）；Redis 恢复后自动回放缓冲 | 同步延迟临时上升 | 无 |
| Redis 长时间不可用（分钟级） | 数据同步暂停，主节点读写不受影响；PublishBuffer 最多缓冲约 1GB | 备节点落后，同步延迟持续增长 | 若缓冲接近 1GB 需运维关注 Redis 恢复时间 |
| Redis 主从切换（平台侧） | 短暂连接中断；HA Agent 通过连接池自动重连 | 同步短暂停顿 | 无（平台完成主从切换即可） |
| Redis 数据丢失（持久化失败、被清库、迁移出错） | **⚠️ Agent 重启后可能按 `ha-agent.yml` 误选主** | **⚠️ 可能造成静默数据回滚** | **必须按 §12.5.4 流程手动判定最后的 master 后再启动** |

#### 12.5.4 Redis 数据丢失后的重建流程（概要）

当 Redis 无法恢复持久化数据（磁盘损坏、AOF 损坏、误删库等），**严禁直接启动 HA Agent**。正确流程：

1. **立即停止所有 HA Agent 实例**，防止按陈旧配置启动
2. **从 Neo4j 节点本身判定最后的 master**，基于三个独立信号（详见 §12.5.5）：
   - ① 每个节点最大的 `_updated_at`（数据新旧）
   - ② 各节点 `_CDCDeleteEvent` 中转节点数量（只有主节点会产生）
   - ③ 各节点是否安装有 `cdc-*` APOC Trigger（只有主节点会装）
3. **修改 `ha-agent.yml`** — 将判定出的节点 `role` 改为 `primary`，其余改为 `standby`
4. 可选：预写较大的 fencing token 到 Redis（例如当前毫秒时间戳）防止未来冲突
5. 启动 HA Agent，**严格校验** `Cluster initialized. Primary: <node>` 与判定一致
6. 观察 2 分钟，确认备节点进入 ONLINE，同步延迟回落到毫秒级

**完整的操作步骤、诊断脚本、冲突场景判定规则、验收标准：参见 `docs/nuclear-fusion/operations/ha-agent-cluster-operations.md §9 "Redis 数据丢失后的集群重建"`。**

诊断脚本：

```bash
NEO4J_PASSWORD=xxx bash scripts/deploy/detect-last-master.sh
```

#### 12.5.5 判定"最后 master"的三个信号及其强度

| 信号 | 判定强度 | 为何可靠 |
|------|---------|---------|
| **cdc-\* APOC Trigger 安装情况** | **最强** | Switchover / OldPrimaryRecovery 流程强制在**旧主降级时卸载** Trigger（§7.5 Step 1）；某节点装有 Trigger 说明它是 Agent 上次记录的 PRIMARY |
| **`_CDCDeleteEvent` 中转节点** | 强 | 只有 master 的 APOC Trigger 会生成此标签节点；switchover 会清理旧主残留 |
| **最大 `_updated_at`** | 兜底 | 主从架构下只有主接受写入，数据最新者**必然**是最后的 master |

三个信号一致指向同一节点时可高置信度判定；如果出现冲突（最强信号和兜底信号指向不同节点），说明 switchover 中途异常停机，**需要人工 diff 数据**而不是盲动。

#### 12.5.6 预防建议

| 预防手段 | 效果 |
|---------|------|
| 开启 AOF `everysec` + RDB 快照 | 降低 Redis 数据丢失概率至接近零 |
| 定期异地备份 `redis-cli BGREWRITEAOF` 产物 | 提供回档兜底 |
| 监控 `neo4j_ha_node_role` 指标与 `ha-agent.yml` 的一致性 | switchover 后主动同步更新 `ha-agent.yml`（可自动化） |
| 每月一次"Redis 数据丢失演练" | 验证 §12.5.4 流程可执行 |
| 监控 Redis `lastsave` / `aof_last_rewrite_time_sec` | 确保持久化真正在发生 |

### 12.6 灾备恢复总结

| 故障组件 | 自动恢复 | 数据服务影响 | 数据丢失风险 | 需要人工操作 |
|---------|---------|------------|------------|------------|
| Neo4j 主节点 | Failover 切到备节点 (~13s) | 写中断 ~13s | 最后 ~100ms 未同步的变更 | 补充新备节点 |
| Neo4j 备节点 | 自动摘除 | 无 | 无 | 补充新备节点 |
| HA Agent | Docker restart (~3s) + 状态恢复 (~7s) | 无（主节点不受影响） | 无 | 无 |
| HAProxy 单实例 | Docker restart (~2s) / 客户端 fallback | 无（另一实例接管） | 无 | 如 IP 变更需更新配置 |
| HAProxy 全部下线 | Docker restart | 全部中断至恢复 | 无 | 无 |
| Redis（平台）短暂不可用 | 平台保障恢复 + HA Agent 本地缓冲 | 同步暂停，读写不受影响 | 无（缓冲保护） | 联系平台运维 |
| Redis 数据丢失（持久化失败） | **不能自动恢复** | 同步暂停，**重启 Agent 有误选主风险** | **可能数据回滚（如误启动）** | **按 §12.5.4 手动判定最后 master 后再启动 Agent**；参见运维手册 §9 |
| HA Agent + Neo4j 主节点同时下线 | 无法自动 Failover | 写中断至 Agent 恢复 | 可能丢失未同步变更 | 人工确认集群状态 |

### 12.7 数据备份策略

**核心思路：利用备节点做离线备份，主节点全程不受影响。**

> 操作手册：`docs/nuclear-fusion/operations/backup-recovery-runbook.md`

#### 12.7.1 推荐方案：备节点离线备份

主备架构天然提供了一个可以安全做离线操作的数据副本。备份流程：

```
备节点离线备份流程:

  1. POST /cluster/backup（HA Agent Admin API）
       → HA Agent 暂停 Sync Applier（备节点数据静止）
       → 记录当前 checkpoint 位点

  2. 停止备节点 Neo4j 容器
       docker compose stop neo4j-standby

  3. 拷贝备节点数据卷
       cp -r /var/lib/docker/volumes/neo4j-standby-data/_data \
             /backup/neo4j-$(date +%Y%m%d-%H%M%S)

  4. 重启备节点
       docker compose start neo4j-standby

  5. HA Agent 自动检测到备节点恢复
       → 从 checkpoint 增量追赶下线期间的变更
       → 追赶完成后恢复正常同步

  全程主节点不停机，业务零影响。
```

#### 12.7.2 备份方式对比

| 方式 | 主节点停机 | 备节点停机 | 数据一致性 | 适用场景 |
|------|----------|----------|-----------|---------|
| **备节点拷贝**（推荐） | 不需要 | 短暂（分钟级） | 保证一致 | 生产环境日常备份 |
| `neo4j-admin dump` | 需要 | — | 保证一致 | 维护窗口、版本迁移 |
| 主节点在线拷贝 | 不需要 | — | **不保证一致** | 不推荐 |

#### 12.7.3 定时备份方案

通过 cron 定时调用 HA Agent 的备份 API，实现自动化：

```bash
# /etc/cron.d/neo4j-backup — 每天凌晨 3 点执行
0 3 * * * root /opt/neo4j-ha/scripts/backup/backup-standby.sh
```

```bash
#!/bin/bash
# scripts/backup/backup-standby.sh

BACKUP_DIR="/backup/neo4j"
RETENTION_DAYS=7
HA_AGENT="http://localhost:8080"
TOKEN="${ADMIN_TOKEN}"

# 1. 通知 HA Agent 暂停同步并准备备份
curl -sf -X POST "$HA_AGENT/cluster/backup/prepare" \
     -H "Authorization: Bearer $TOKEN" || exit 1

# 2. 停止备节点
docker compose -f /opt/neo4j-ha/docker/docker-compose.yml stop neo4j-standby

# 3. 拷贝数据
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
cp -r /var/lib/docker/volumes/neo4j-standby-data/_data \
      "$BACKUP_DIR/neo4j-$TIMESTAMP"

# 4. 重启备节点（HA Agent 自动追赶）
docker compose -f /opt/neo4j-ha/docker/docker-compose.yml start neo4j-standby

# 5. 通知 HA Agent 备份完成
curl -sf -X POST "$HA_AGENT/cluster/backup/complete" \
     -H "Authorization: Bearer $TOKEN"

# 6. 清理过期备份
find "$BACKUP_DIR" -maxdepth 1 -name "neo4j-*" -type d \
     -mtime +$RETENTION_DAYS -exec rm -rf {} \;

echo "[$(date)] Backup completed: neo4j-$TIMESTAMP"
```

#### 12.7.4 备份恢复

从备份恢复到新节点（例如替换永久下线的节点）：

```bash
# 1. 停止目标 Neo4j 容器
docker compose stop neo4j-standby

# 2. 清空现有数据卷并拷入备份
rm -rf /var/lib/docker/volumes/neo4j-standby-data/_data/*
cp -r /backup/neo4j-20260410-030000/* \
      /var/lib/docker/volumes/neo4j-standby-data/_data/

# 3. 启动容器
docker compose start neo4j-standby

# 4. HA Agent 自动检测到节点恢复
#    → 从备份时的 checkpoint 位点增量追赶
#    → 如果 Stream 已 trim 超过该位点 → 自动触发全量同步
```

#### 12.7.5 备份监控

| 指标 / 告警 | 说明 |
|------------|------|
| `neo4j_ha_backup_last_success_timestamp` | 上次成功备份的时间戳 |
| `neo4j_ha_backup_duration_ms` | 备份耗时 |
| `Neo4jHABackupOverdue` | `time() - neo4j_ha_backup_last_success_timestamp > 86400 * 2` → WARNING（超过 2 天未备份） |

---

## 13. 项目目录结构

```
neo4j-HA/
├── docs/                                  # 设计文档
│   ├── nuclear-fusion/
│   │   ├── requirements/                  # 需求文档
│   │   ├── design/                        # 架构设计
│   │   │   └── modules/                   # 模块详细设计
│   │   ├── analysis/                      # 分析报告
│   │   ├── reviews/                       # 评审报告
│   │   └── testing/                       # 测试策略
│   └── diagrams/                          # 架构图源文件
├── src/
│   ├── ha-agent/                          # HA Agent 集中式主进程（唯一可执行模块）
│   │   ├── src/main/java/
│   │   └── pom.xml
│   ├── cdc-collector/                     # CDC 变更捕获（作为 ha-agent 内部模块）
│   │   ├── src/main/java/
│   │   └── pom.xml
│   ├── sync-applier/                      # 变更回放（作为 ha-agent 内部模块）
│   │   ├── src/main/java/
│   │   └── pom.xml
│   └── common/                            # 共享库
│       ├── src/main/java/
│       └── pom.xml
├── config/                                # 配置模板
│   ├── haproxy/                           # HAProxy 配置
│   └── agent/                             # HA Agent 配置（含 Redis 连接信息）
├── docker/                                # Docker 相关
│   ├── neo4j/                             # Neo4j Dockerfile
│   ├── haproxy/                           # HAProxy Dockerfile
│   └── docker-compose.yml                 # 一键启动（Redis 复用外部实例）
├── scripts/                               # 运维脚本
│   ├── deploy/                            # 部署脚本
│   ├── failover/                          # 手动切换脚本
│   └── backup/                            # 备份脚本
├── test/                                  # 测试
│   ├── unit/                              # 单元测试
│   ├── integration/                       # 集成测试
│   └── failover-simulation/               # Failover 模拟测试
├── pom.xml                                # Maven 父 POM
└── README.md
```
