# KG 脏数据查询功能需求 — HA 平台

> 背景：oversea-chatbot 的图谱数据存于 Neo4j。历史上出现过多种类型的脏数据
> （孤儿节点、半完成的删除、字段漏写、跨边不一致），目前依赖运维同学手写
> Cypher / Python 脚本临时排查（参见 `scripts/_scan_dirty_kg.py`）。
> 期望 HA 平台提供标准化查询入口，让 RD/运维能自助定位脏数据，无需直接
> 触碰生产 Neo4j。

## 0. 数据模型简介

四类节点/边（标签前缀 `aiagent_`）：

| 类型 | Key | 重要属性 |
|---|---|---|
| `Entity` 节点 | (`bot_id`, `name`, `entity_type`) MERGE | `group_id`, `description`, `is_active` |
| `Segment` 节点 | (`bot_id`, `segment_id`) MERGE | `data_id`, `group_id`, `es_index`, `is_active`, `is_doc_active` |
| `MENTIONED_IN` 边 | (`segment_id`, `data_id`) MERGE on `Entity→Segment` | `bot_id`, `group_id`, `source` (auto/manual), `is_active`, `is_doc_active` |
| `RELATES_TO` 边 | (`triple_id`, `bot_id`) MERGE on `Entity→Entity` | `group_id`, `source_segment_id`, `source_data_id`, `source` |

隔离维度：所有查询必须以 `bot_id` 为强制过滤前提（bot 隔离）。

---

## 1. 数据规模与分布（基础体检）

### 1.1 单 bot 全局规模
- **输入**：`bot_id` (required)
- **输出**：`segment_count`, `entity_count`, `mention_count`, `relation_count`, `distinct_data_ids`, `distinct_group_ids`

### 1.2 RELATES_TO 边按 `group_id` 分布
- **输入**：`bot_id` (required)
- **输出**：`[ {group_id, edge_count} ]` 按 edge_count desc

### 1.3 Segment 按 `data_id` 分布
- **输入**：`bot_id` (required)，可选 `group_id` 过滤
- **输出**：`[ {data_id, segment_count} ]` 按 segment_count desc

### 1.4 Entity 按 `group_id` 分布
- **输入**：`bot_id` (required)
- **输出**：`[ {group_id, entity_count} ]`（`group_id` 可能为 `<null>`）

---

## 2. 孤儿节点检测（hard orphan）

### 2.1 完全无边的 Entity
- **条件**：`bot_id` 匹配 且 `NOT (e)--()`
- **输出**：`count` + 可选 sample 列表

### 2.2 无业务边的 Entity（只剩派生边如 BELONGS_TO/Community）
- **条件**：`NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:MENTIONED_IN]->()`
- **输出**：同上

### 2.3 无入边的 Segment
- **条件**：`NOT ()-[:MENTIONED_IN]->(seg)`
- **输出**：同上

### 2.4 Segmentless RELATES_TO（边引用的 `source_data_id` 在 bot 内已无对应 Segment）
- **条件**：`r.source_data_id NOT IN (bot 内 Segment.data_id 集合)`
- **输出**：`count` + sample `[ {triple_id, source_data_id, group_id} ]`

---

## 3. 字段完整性检查

### 3.1 MENTIONED_IN 字段缺失
- **输入**：`bot_id`
- **输出**：`{noBotId, noGroupId, noIsActive, noDataId}` 各字段的 NULL 计数

### 3.2 Segment 字段缺失
- **输出**：`{noGroupId, noIsActive, noDataId, noEsIndex}`

### 3.3 RELATES_TO 字段缺失
- **输出**：`{noBotId, noGroupId, noSourceDataId, noIsActive}`

### 3.4 Entity 字段缺失
- **输出**：`{noGroupId}`（重点关注）

> 用途：检测代码 bug 引入的字段漏写（如 2026-05-15 修复的 `bulk_create_mentioned_in`
> 漏写 group_id/is_active/bot_id 问题）

---

## 4. 跨边一致性（半完成的脏数据）

### 4.1 ★疑似孤儿 doc：MENTIONED_IN 残留但 RELATES_TO=0
- **条件**：某 `data_id` 在 bot 内有 ≥1 条 MENTIONED_IN 但 `RELATES_TO source_data_id=data_id` count=0
- **输出**：`[ {data_id, mention_count, segment_count} ]`
- **用途**：最常见的脏数据来源（历史半成功 delete_by_doc）

### 4.2 反向半完成：RELATES_TO 残留但 MENTIONED_IN=0
- **条件**：某 `data_id` 有 RELATES_TO 但无 MENTIONED_IN
- **用途**：罕见，反向半完成

### 4.3 Entity.group_id 卡死
- **条件**：`Entity.group_id` 非空 且 不在其所有边的 `group_id` 集合内
- **输出**：`[ {entity_name, entity_type, stuck_group_id, actual_group_ids} ]`
- **用途**：reconcile 漏过的实体（fix `21ff2e82`/`e0a4a074` 历史问题）

### 4.4 同名 Entity 多 entity_type
- **条件**：同一 `bot_id + name` 下出现 ≥2 个不同 `entity_type`
- **用途**：MERGE 索引异常预警（理论上 bot+name+type 唯一）

### 4.5 bot 隔离破洞
- **条件**：`MENTIONED_IN.bot_id ≠ Segment.bot_id`（或 ≠ Entity.bot_id）
- **用途**：跨 bot 数据污染预警

---

## 5. 详查 / 钻取

### 5.1 按 (`bot_id`, `data_id`)
- **输出**：
  - Segment 列表：`[ {segment_id, es_index, data_id, group_id, is_active, is_doc_active} ]`
  - MENTIONED_IN 列表（聚合）：`[ {entity_name, entity_type, segment_id, source, is_active, bot_id, group_id} ]`
  - RELATES_TO 计数：`{count, group_ids}`

### 5.2 按 (`bot_id`, `entity_name`[, `entity_type`])
- **输出**：
  - 该实体 MENTIONED_IN 指向的 `segment_id` 列表
  - 该实体所有 RELATES_TO 边的 `group_id` 集合 + count
  - Entity 节点本身的 `group_id` / `is_active`
- **用途**：判定一个 entity 是否可安全删除（无其他 doc 引用）

### 5.3 按 (`bot_id`, `segment_id`)
- **输出**：
  - 该切片上挂的 Entity 列表（含 mention.source）
  - 相关 RELATES_TO triple_id 列表

---

## 6. 跨系统对账（如平台支持 ES 查询）

> 这一类需要 Neo4j + ES 同时校验。如果平台不能跨系统，可作为 future scope。

### 6.1 Neo4j 有 Segment 但 ES 无 doc
- **逻辑**：取 Neo4j 内的 `(es_index, segment_id)` 集合 → ES mget → 列出 miss 的
- **用途**：确认"真孤儿 doc"，可触发自动清理

### 6.2 ES 有 doc 但 Neo4j 无 Segment（限 kgSwitch=true 的 KB）
- **逻辑**：取 ES 内某 KB 的 `data_id` 集合 → Neo4j 反查 → 列出 miss 的
- **用途**：KG 漏建预警

---

## 7. 通用参数与输出约定

### 输入参数
- `bot_id`：required，所有查询前提
- `group_id`：optional，进一步缩窄
- `data_id`：optional，详查
- `entity_name` / `entity_type` / `segment_id`：optional，详查
- `limit` / `offset`：分页
- `sample_size`：count 类查询附带样本时的样本上限（建议默认 20，上限 100）

### 输出格式
- 统一 JSON
- count 类查询返回 `{ count: N, samples: [...] }`，samples 是可选的示例条目
- 列表类查询支持游标分页或 offset/limit 分页

### 性能与安全
- 所有 Cypher 应带 `LIMIT`（推荐 5000 上限），避免单查询拉穷库
- 重型查询（如 4.1 / 6.1 全 bot 扫）支持异步任务模式：提交 → 拿 task_id → 轮询结果
- 只读账号，禁止 `DELETE / MERGE / SET / CREATE`
- 调用方需带 RBAC（看自己负责的 bot/project）

---

## 8. 交付优先级建议

| 优先级 | 范围 | 理由 |
|---|---|---|
| P0 | §1（规模）、§2（孤儿检测）、§4.1（疑似孤儿 doc）、§5.1（按 data_id 详查） | 覆盖 95% 实战场景，今天的清理流程刚好用到这几条 |
| P1 | §3（字段完整性）、§4.3-4.4（一致性预警）、§5.2-5.3（其他详查） | 防御性预警，cheap |
| P2 | §6（跨系统对账） | 价值高但需要打通 ES，工作量大 |

---

## 9. 参考实现

本仓库 `scripts/_scan_dirty_kg.py` 已用纯 Cypher + aiohttp 实现 §1、§2、§3、§4.1、§4.3、§6.1 全部能力，
可作为 HA 平台开发的 Cypher 参考。**所有 Cypher 都带 `bot_id` 过滤前提且不修改数据**。

> 实战案例（2026-05-15）：通过 §4.1 查询发现 bot `6a02d4967e7b712529eb2ade` 下
> 有 1 个疑似孤儿 doc，§6.1 ES 校验确认后清理 17 segment + 261 mention + 130 entity，
> 接口回归正常。
