# Neo4j 社区版主备高可用系统 — 需求解析文档

> 日期: 2026-04-10
> 版本: v1.0
> 模式: Nuclear Fusion Full Pipeline — Phase 1

---

## 1. 项目背景

### 1.1 问题陈述

Neo4j Community Edition 是一款开源图数据库，但**社区版不支持集群、不支持因果集群（Causal Clustering）、不支持原生高可用**。企业版提供的 Causal Clustering 基于 Raft 协议实现多副本一致性，但其商业许可费用对中小团队而言成本过高。

当前业务场景需要：
- 图数据库的写入可用性保障（RPO < 1s, RTO < 30s）
- 读写分离以提升查询性能
- 主节点故障后备节点自动接管，业务无感知

### 1.2 方案选型理由

| 方案 | 优点 | 缺点 | 结论 |
|------|------|------|------|
| Neo4j Enterprise Causal Clustering | 原生支持、Raft一致性 | 商业许可昂贵 | 成本不可接受 |
| Neo4j Community + Kafka CDC | 成熟生态、持久化强 | 引入Kafka+ZK运维复杂、资源开销大 | 架构过重 |
| Neo4j Community + Redis Stream | 轻量、亚毫秒延迟、运维简单 | 需自研同步逻辑、Stream非持久化MQ | **本方案** |
| 冷备 + 定时快照 | 实现简单 | RPO分钟级、无法实时同步 | 不满足需求 |

**选择 Redis Stream 替代 Kafka 的核心理由：**
- 亚毫秒级延迟（p99 < 1ms vs Kafka 12.5ms）
- 无需额外部署 ZooKeeper/KRaft，运维成本低
- 已有 Redis 基础设施可复用（缓存 + 分布式锁 + Stream 三合一）
- 吞吐量满足图数据库场景（48万 msg/s 远超图变更频率）

---

## 2. 需求维度分析

### 2.1 功能模块

| 模块 | 功能描述 | 优先级 |
|------|---------|--------|
| **CDC Collector** | 从 Neo4j 主节点捕获数据变更事件 | P0 |
| **Stream Publisher** | 将变更事件序列化后写入 Redis Stream | P0 |
| **Stream Consumer / Sync Applier** | 从 Redis Stream 消费事件，在备节点回放 | P0 |
| **Failover Manager** | 故障检测 + 自动主备切换 | P0 |
| **Health Monitor** | 主备节点健康检查（心跳/探活） | P0 |
| **Client Router** | 客户端连接路由，感知主备切换 | P0 |
| **Full Sync Engine** | 首次同步/数据追赶的全量同步 | P1 |
| **Checkpoint Manager** | 同步位点管理，支持断点续传 | P1 |
| **Monitoring & Alerting** | 同步延迟、节点状态的监控告警 | P1 |
| **Admin API** | 手动切换、状态查询、运维接口 | P2 |

### 2.2 技术架构需求

| 需求项 | 详细描述 |
|--------|---------|
| 架构风格 | 主备（Master-Standby）单写多读 |
| 部署模式 | 容器化部署（Docker Compose / K8s） |
| 主节点 | 唯一写入点，运行 CDC Collector |
| 备节点 | 1~N 个只读副本，运行 Sync Applier |
| 消息通道 | Redis Stream（替代 Kafka） |
| 协调服务 | Redis Sentinel（故障检测 + 分布式锁） |
| 客户端路由 | HAProxy / 应用层 SDK |
| 编程语言 | Java（主体）/ Python（运维脚本） |

### 2.3 数据同步需求

| 需求项 | 指标 |
|--------|------|
| 同步延迟（正常） | < 100ms |
| 同步延迟（峰值） | < 1s |
| RPO（Recovery Point Objective） | < 1s |
| RTO（Recovery Time Objective） | < 30s |
| 数据一致性 | 最终一致性（异步复制） |
| 全量同步 | 首次加入 / 数据差距过大时触发 |
| 断点续传 | 基于 Redis Stream Message ID 的 Checkpoint |
| 冲突处理 | 不存在（单写架构，无写冲突） |

### 2.4 故障检测与Failover需求

| 需求项 | 详细描述 |
|--------|---------|
| 故障检测方式 | 多层探活：TCP连接 + Bolt协议 + Cypher查询 |
| 检测间隔 | 1s（心跳） |
| 故障判定 | 连续 N 次（默认3次）探活失败 |
| 自动切换 | 备节点提升为主节点，开始接受写入 |
| 脑裂防护 | Redis 分布式锁（Redlock / Sentinel） |
| 旧主恢复 | 降级为备节点，全量/增量追赶后重新加入 |
| 手动切换 | 支持运维 API 手动触发主备切换 |

### 2.5 安全与权限

| 需求项 | 详细描述 |
|--------|---------|
| Neo4j 认证 | Bolt 协议原生认证 |
| Redis 认证 | Redis AUTH + TLS（可选） |
| 管理API认证 | Token / Basic Auth |
| 网络隔离 | 同步通道走内网，不暴露公网 |

### 2.6 日志与审计

| 需求项 | 详细描述 |
|--------|---------|
| 同步日志 | 每条变更的发布/消费/回放状态 |
| Failover日志 | 切换事件、决策依据、时间线 |
| 性能指标 | 同步延迟、吞吐量、队列深度 |
| 日志格式 | 结构化 JSON |
| 日志输出 | 文件 + stdout（兼容容器日志采集） |

---

## 3. 约束条件

| 约束 | 说明 |
|------|------|
| Neo4j 版本 | Community Edition 2026.2.3 |
| APOC 依赖 | APOC Core（内置）+ APOC Extended（需额外安装，提供 `apoc.trigger.toNode` 等删除捕获能力） |
| Neo4j CDC 不可用 | 社区版无原生 CDC，通过 Cypher 轮询 + APOC Trigger 实现变更捕获 |
| 单写约束 | 任何时刻只有一个主节点可写入 |
| **Redis 版本** | **≥ 6.2（推荐 7.x）**。`StreamMaintenanceTask` 使用 `XTRIM MINID` 命令（Redis 6.2 引入）。6.0.x 不支持该命令，会导致 Stream consumer-aware 精细化清理失败 |
| Redis Stream 有限持久化 | Stream 受内存限制，需设置 MAXLEN / 定期 TRIM |
| 异步复制 | 非强一致，极端情况可能丢失最后几笔事务 |
| Neo4j 事务日志 | 社区版可读取事务日志但格式为内部二进制，版本间可能变化 |

---

## 4. 非功能性需求

| 维度 | 要求 |
|------|------|
| 可用性 | 99.9%（年停机 < 8.76h） |
| 可观测性 | Prometheus metrics + Grafana dashboard |
| 可扩展性 | 支持 1主N备（N ≤ 5） |
| 可运维性 | Docker Compose 一键部署、Ansible/脚本化运维 |
| 可测试性 | 故障注入测试、同步正确性验证 |

---

## 5. 关键技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Neo4j 社区版事务日志格式变更 | CDC Collector 失效 | 采用 Cypher 轮询方案作为主方案，事务日志解析作为高性能备选 |
| Redis 宕机导致同步中断 | 主备数据不一致 | Redis Sentinel 高可用 + 本地 WAL 缓冲 |
| 脑裂（双主） | 数据冲突 | Redis 分布式锁 + Fencing Token |
| 备节点全量同步耗时过长 | 同步期间备节点不可用 | 增量优先 + 后台全量 + 限速 |
| Redis Stream 内存溢出 | OOM 导致 Redis 崩溃 | MAXLEN 限制 + 监控告警 + 自动 TRIM |

---

## 6. 术语表

| 术语 | 定义 |
|------|------|
| CDC | Change Data Capture，变更数据捕获 |
| Failover | 故障转移，主节点故障后备节点自动接管 |
| Fencing Token | 防脑裂令牌，确保旧主不会在新主产生后继续写入 |
| RPO | Recovery Point Objective，数据恢复点目标（可接受的最大数据丢失量） |
| RTO | Recovery Time Objective，恢复时间目标（从故障到恢复服务的最大时间） |
| PEL | Pending Entries List，Redis Stream 中已读未确认的消息列表 |
| Consumer Group | Redis Stream 消费者组，支持多消费者协同消费 |
| Bolt | Neo4j 的二进制通信协议 |
