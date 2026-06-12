# Neo4j HA 客户端使用约束（Client Contract）

> 适用范围：本文档面向**所有接入 `neo4j-HA` 集群的业务代码**（包括但不限于
> `oversea-chatbot-sdk` 等下游仓库），定义客户端在 HA 集群下必须遵守的约束、
> 推荐契约、保留命名空间。
>
> **本文档是 source of truth。** 业务仓库的 client contract（如
> `oversea-chatbot/docs/neo4j_ha_client_contract.md`）应当镜像本文档；如有
> 偏差，以本文档为准。
>
> 关联文档：
> - 设计：`docs/nuclear-fusion/design/modules/ha-agent-design.md`（简称 **design**）
> - 运维：`docs/nuclear-fusion/operations/ha-agent-cluster-operations.md`（简称 **ops**）
> - 历次审计：`docs/nuclear-fusion/reviews/*`

---

## 一、背景

`neo4j-HA` 提供 primary + N standby 集群，**不是** Neo4j Enterprise 内置的
Causal Cluster。同步链路为：

```
APOC Trigger → CDC Collector（keyset poll _updated_at）→ Redis Stream → SyncApplier
```

这条链路对客户端写法有几类约束：

| 等级 | 描述 | 违反后果 |
|------|------|---------|
| **R0 — 保留命名空间** | `_*` 前缀的 property / label / rel type 全归 HA agent 用 | 撞 BUG-083 UNIQUE 约束、被 reconciler 覆盖、cleanup 误删 |
| **强约束 S1/S2** | GDS 不能 `.write`；关系属性变更必须 DELETE+CREATE | standby 永久看不到变更 |
| **推荐契约 R1** | 关系 CREATE 时写 `r.createdAt = timestamp()` | NakedRelationshipHealer 降级到 slow path |
| **Driver D1/D2/D3** | 写路径需要重试；写读路由分流 | switchover 窗口写阻断暴露给业务 |

下面逐项展开。

---

## 二、约束总览

| 等级 | 编号 | 约束 | 违反后果 | 上游出处 |
|------|------|------|---------|---------|
| **保留命名空间** | **R0** | 不写 `_*` 前缀的 property / label / rel type | 撞 BUG-083 约束 / 被覆盖 / 被 cleanup 误删 | design §BUG-083、`ApocTriggerInstaller` |
| 强约束 | **S1** | GDS 算法回写禁用 `.write` / `.mutate→write`，必须 `.stream + MATCH + SET` | 算法结果**永远不进 standby**（CDC 永远捕获不到） | design §BUG-063、ops §"GDS write contract" |
| 强约束 | **S2** | 关系属性变更必须 DELETE+CREATE，不能 `SET r.x = ...` | SET 不刷 `_updated_at`，standby **永远看不到变更后的值** | design §BUG-059、`ApocTriggerInstaller.REL_TIMESTAMP_TRIGGER` |
| 推荐契约 | **R1** | 所有关系 CREATE / MERGE 时写 `r.createdAt = timestamp()` | `NakedRelationshipHealer` 降级到 slow path | design §BUG-062、ops §"Client relationship write contract" |
| Driver 层 | **D1** | 写路径推荐 **managed transaction** (`session.execute_write`) | 非托管写在 switchover 窗口必须应用层自行重试 | design §6.9.7 |
| Driver 层 | **D2** | 非托管写必须在 `ServiceUnavailable` / `SessionExpired` / `TransientError` / **`ConstraintError(_elementId)`** 上重试 | switchover / BUG-083 撞值的瞬时异常向业务暴露裸异常 | design §6.9.7、§BUG-083 |
| 路由分流 | **D3** | 写走 WRITE route，读走 READ route，DDL 走 WRITE route | 读占用写 HAProxy pool；switchover 写阻断时只读查询被连带误杀 | ops §HAProxy 双池 |
| 规模约束 | **L1** | 单 Cypher 影响行数 > 千时分批 | switchover Phase 3 drain（10×1s）窗口溢出 | design §6.9.2 |
| 原子性约束 | **L2** | 跨 `session.run` 不是原子的 | 中间状态被 CDC 捕获到 standby | Cypher 事务模型 |

---

## 三、保留命名空间（强约束 R0）

> 这是 HA Agent 内部使用的命名空间，**客户端代码任何路径都不能使用**。命中
> 任意一项 = 永久数据不一致 / 写入失败。

### 3.1 保留 property（节点 + 关系）

由 `cdc-timestamp-created` / `cdc-timestamp-assigned` / `cdc-timestamp-removed`
/ `cdc-rel-timestamp` 四个 APOC trigger 自动写入。客户端**不能 SET / REMOVE /
在 MERGE key 里使用**。

| Property | 类型 | 适用对象 | 写入时机 / 算法 | 客户端误写后果 |
|----------|------|---------|---------------|---------------|
| `_elementId` | `String` | 节点 + 关系 | `coalesce(prop, elementId(n))`<br>节点：`cdc-timestamp-created` phase=`before`<br>关系：`cdc-rel-timestamp` phase=`afterAsync` | ① BUG-083 UNIQUE 约束撞值时 `ConstraintValidationFailed` 立刻 abort 整条事务<br>② 跨切主时被 `PostSwitchoverReconciler` 覆盖回上游真身——业务读到的值跟自己写的不一样 |
| `_updated_at` | `long`（Neo4j `timestamp()`）| 节点 + 关系 | 节点：`cdc-timestamp-created` 设值 + `cdc-timestamp-assigned` 更新<br>关系：`cdc-rel-timestamp` 设值 | CDC keyset cursor 字段；客户端写值 → cursor 错位 → 该实体可能不再被 CDC 轮询捕获 |
| `_created_at` | `long` | 节点 + 关系 | 节点：`cdc-timestamp-created` `coalesce`<br>关系：`cdc-rel-timestamp` `coalesce` | `elementid-dedup.sh` 选 keep 节点的依据；写值会污染清理脚本的"上游真身"判定 |
| `_type` | `String` | **关系 only** | `cdc-rel-timestamp` `coalesce(r._type, type(r))` | rel 删除事件（`cdc-capture-rel-deletes`）必需读取此值；写值导致 standby 删除事件 routing 错乱 |
| `_labels` | `String`（JSON）| 仅出现在 `_CDCDeleteEvent` transit 节点 | `cdc-capture-node-deletes` 写出 | 不该出现在用户节点上 |

#### ❌ 禁用写法

```cypher
CREATE (n:Person {name: 'Alice', _elementId: 'my-uuid'})         -- 撞 BUG-083
SET n._updated_at = timestamp()                                   -- 污染 CDC cursor
MERGE (n:Person {_elementId: $foreign_key})                       -- 用作业务键
REMOVE n._elementId                                               -- 节点退出 CDC
SET n = {name: 'Bob'}                                             -- 整体覆盖会清掉 _elementId
```

#### ✅ 正确写法

```cypher
CREATE (n:Person {name: 'Alice', business_id: 'uuid-xxx'})       -- 用业务字段
SET n.name = 'Bob', n.age = 32                                    -- 增量更新
SET n += {name: 'Bob', age: 32}                                   -- 等价的 += 形态
MATCH (n:Person {business_id: $bid})                              -- 业务键查询
```

#### 审计 grep（在业务仓库跑）

```bash
# 直接 SET / REMOVE 系统属性
grep -rnE 'SET\s+[a-zA-Z_]+\._(elementId|updated_at|created_at|type|labels)\b' src/
grep -rnE 'REMOVE\s+[a-zA-Z_]+\._(elementId|updated_at|created_at|type|labels)\b' src/

# MERGE / CREATE 字面量里把系统属性当业务字段用
grep -rnE '_(elementId|updated_at|created_at|type|labels)\s*:' src/

# 整体覆盖（顺带把保留属性清空）
grep -rnE 'SET\s+[a-z]\s*=\s*\{' src/
```

### 3.2 保留 label

| Label | 用途 | 来源 |
|-------|------|------|
| `_CDCDeleteEvent` | DELETE 事件 transit 节点（NODE / REL 两类） | `NODE_DELETE_TRIGGER` / `REL_DELETE_TRIGGER` 写出，`DeleteEventCapture` 读出后由 cleaner DETACH DELETE。BUG-067 专门做过 orphan 清理 |
| `_TriggerReadinessProbe` | trigger 安装可达性探针 | `ApocTriggerInstaller` 启动时写入，自删除 |
| `_TestPing` | 历史 ping trigger 验证用，已废弃但 trigger 代码里仍有过滤 | 测试 fixture，生产不应出现 |

**铁律**：**任何 `_` 前缀的 label，业务都不能 CREATE / MATCH / MERGE / DETACH
DELETE**，也不能给业务节点附加这种 label。

#### 只读查询的兜底

业务侧 RAG / 统计类只读查询如果做 `MATCH (n) RETURN n` 这种**不带 label
限定**的写法，必须用 `WHERE NOT n:_CDCDeleteEvent` 兜底，否则可能瞬时混入
transit 节点（`_CDCDeleteEvent` 在 trigger 写出 → CDC 消费 → cleaner 删除
之间的窗口约 ~1s）。

```cypher
-- ❌ 危险
MATCH (n) WHERE n.business_id = $id RETURN n

-- ✅ 兜底
MATCH (n) WHERE n.business_id = $id AND NOT n:_CDCDeleteEvent RETURN n
```

更稳妥的做法是**永远带 label 限定**：`MATCH (n:Person) WHERE ...`。

#### 审计 grep

```bash
grep -rnE ':_[A-Z][A-Za-z]' src/                # label/reltype 名 :_Xxx
grep -rnE '\bMATCH\s*\(\s*[a-z]\s*\)' src/      # MATCH (n) 不带 label
```

### 3.3 保留 relationship type

| Type | 用途 |
|------|------|
| `_PROBE_REL` | rel-trigger 可达性探针，`cdc-rel-timestamp` 安装后自检用，瞬时存在 |

业务关系类型不能用 `_` 前缀。

### 3.4 保留 schema（索引 + 约束）

由 `IndexInstaller.ensureIndexes` 在每次 boot / 切主 / OldPrimaryRecovery 时
**幂等管理**。客户端 / 业务 DDL **不能 DROP 这些 schema**。

| 类别 | 命名模式 | 范围 | 用途 |
|------|---------|------|------|
| `RANGE INDEX` | 自动名 `index_xxxxxx` | `(<UserLabel>, _updated_at)` | CDC keyset 节点轮询 |
| `RANGE INDEX` | 自动名 | `()-[r:<RelType>]-() ON (r._updated_at)` | CDC keyset 关系轮询 |
| `RANGE INDEX` | 自动名 | `()-[r:<RelType>]-() ON (r._elementId)` | sync-applier MERGE/MATCH 关系 |
| `RANGE INDEX` | 自动名 | `()-[r:<RelType>]-() ON (r.createdAt)` | NakedRelationshipHealer fast path（**业务关系侧的 R1 契约字段**）|
| `RANGE INDEX` | 自动名 | `(:_CDCDeleteEvent, timestamp)` | DeleteEventCapture 轮询 |
| **`UNIQUE CONSTRAINT`** | `uniq_elementid_<UserLabel>` | `(<UserLabel>, _elementId)` | **BUG-083** 防 `_elementId` 撞值；节点 only |

> ⚠️ **业务侧 DDL 在 `IndexInstaller` 之后跑** —— 如果业务在 `ensure_constraints`
> 里也写 `CREATE INDEX ... ON (n._elementId)`，会和 `IndexInstaller` 的 UNIQUE
> 约束 backing 索引冲突（Neo4j 5.x 报 `An equivalent constraint already
> exists`）。结论：**业务 DDL 不要碰 `_elementId`**，让 HA agent 独占管理。

业务自己的索引必须用业务字段名：

```cypher
-- ✅
CREATE RANGE INDEX person_business_id IF NOT EXISTS FOR (n:Person) ON (n.business_id)

-- ❌
CREATE RANGE INDEX my_eid IF NOT EXISTS FOR (n:Person) ON (n._elementId)
```

### 3.5 保留 APOC trigger 名

HA Agent 占用以下 trigger 名，业务不能注册同名 trigger（否则会被 agent
覆盖）：

| Trigger 名 | phase | 用途 |
|-----------|-------|------|
| `cdc-timestamp-created` | before | 节点 CREATE 时 stamp `_elementId` / `_updated_at` / `_created_at` |
| `cdc-timestamp-assigned` | before | 节点属性变更时刷新 `_updated_at` |
| `cdc-timestamp-removed` | before | 节点属性 REMOVE 时刷新 `_updated_at` |
| `cdc-rel-timestamp` | afterAsync | 关系 CREATE 时 stamp 全套保留属性 |
| `cdc-capture-node-deletes` | before | 节点 DELETE 写 `_CDCDeleteEvent` transit 节点 |
| `cdc-capture-rel-deletes` | before | 关系 DELETE 写 `_CDCDeleteEvent` transit 节点 |

业务自定义 trigger 应使用其他前缀（推荐 `app-*` / 业务名前缀）。

---

## 四、强约束（S1 / S2）

### S1 — GDS 写回必须走 `.stream + MATCH + SET`

**上游出处**：design §BUG-063、ops §"GDS write contract"

**根因**：`.write` 变体（`gds.louvain.write` / `gds.pageRank.write` /
`gds.graph.writeNodeProperties` / `gds.graph.writeRelationship` 等）走 Neo4j
内部批量 property store API，**不经过 Cypher 事务**，也就**不会触发 APOC
trigger**。`_updated_at` 不被刷新，CDC keyset `WHERE n._updated_at > $lastTs`
永远不匹配这些节点，standby 上永远看不到算法结果。

#### ❌ 禁用

```cypher
CALL gds.leiden.write($g, {writeProperty: 'community_id'});
CALL gds.pageRank.write($g, {writeProperty: 'pagerank'});
CALL gds.graph.writeNodeProperties($g, ['community']);
CALL gds.graph.writeRelationship($g, 'SIMILAR');
```

#### ✅ 推荐

```cypher
-- 社区检测
CALL gds.leiden.stream($g, {...})
YIELD nodeId, communityId
MATCH (n) WHERE id(n) = nodeId
SET n.community_id = communityId;

-- PageRank 同理
CALL gds.pageRank.stream($g)
YIELD nodeId, score
MATCH (n) WHERE id(n) = nodeId
SET n.pagerank = score;
```

`MATCH (n) WHERE id(n) = nodeId` 比 `WITH gds.util.asNode(nodeId) AS n` 更稳
——后者语义绑定 GDS 版本，保守起见不推荐。

#### S1 拓扑约束：GDS 全流程必须 pin 到 primary

> ⚠️ **GDS catalog 是 per-node in-memory 状态，不通过 CDC / Bolt 协议同步。**

后果：

- `gds.graph.project($name, ...)` 在 primary 跑、`gds.<algo>.stream($name)` 落到 standby
  → standby 没这个 graph projection，立即 `Neo.ClientError.Procedure.ProcedureCallFailed:
  Graph with name '$name' does not exist`
- 哪怕 project 和 stream 都用同一个 driver session，driver 默认按"算法是只读 → 路由到
  standby"判断，**仍然会路由错**

**所有 GDS 调用（`gds.graph.project` / `gds.graph.drop` / `gds.<algo>.stream` /
`gds.<algo>.estimate` / `gds.graph.list`）必须强制走 WRITE route**，对齐到同一个
primary 节点。

#### ✅ Python 样板

```python
from neo4j import GraphDatabase, WRITE_ACCESS

# Driver 是 process 单例（见 §6.1）
driver = GraphDatabase.driver(WRITE_BOLT_URI, auth=AUTH)

def run_gds_workflow(graph_name: str):
    """整段 GDS 流程必须在同一个 WRITE session 中，pin 到 primary"""
    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        try:
            session.run(
                "CALL gds.graph.project($g, $node_label, $rel_type)",
                g=graph_name, node_label="Person", rel_type="KNOWS",
            )
            session.run("""
                CALL gds.leiden.stream($g, {})
                YIELD nodeId, communityId
                MATCH (n) WHERE id(n) = nodeId
                SET n.community_id = communityId
            """, g=graph_name)
        finally:
            session.run("CALL gds.graph.drop($g, false)", g=graph_name)
```

**关键点**：

- `default_access_mode=WRITE_ACCESS` 强制路由到 primary
- `try/finally` 确保 `gds.graph.drop` 一定执行，否则 in-memory graph 在 primary 内存里
  泄露，下次 `gds.graph.project` 会报 `Graph already exists`
- 整段在 **同一个 session** 里——不能 project 一个 session、stream 另一个 session（即使
  都是 WRITE access mode，driver 也可能 routing 到不同 primary，比如刚发生了
  switchover）

#### Switchover 期间的 GDS

GDS in-memory state 在 primary 重启 / failover 后**全部丢失**（catalog 不持久化）。
长 GDS 任务（小时级算法）**不要跨 switchover 重试**：retry 只会发现 graph projection
不存在。建议：

- 大任务在低峰期跑，避开运维 switchover 窗口
- 任务失败后**重新 project + 重跑**，不要假设 catalog 还在
- 在装饰器里**排除** `ProcedureCallFailed` ── 它通常是业务逻辑错误，不是 retry 能修

#### 审计 grep

```bash
grep -rnE 'gds\.[a-z.]+\.write\b' src/                                    # S1 主约束
grep -rnE 'gds\.graph\.(writeNodeProperties|writeRelationship)\b' src/
grep -rnE 'writeProperty:' src/
grep -rnE 'gds\.graph\.project' src/                                      # 必须人工核查 session 是 WRITE
grep -rnE 'session\(.*READ_ACCESS.*\).*gds\.' src/                        # 直接错配
```

---

### S2 — 关系属性变更必须 DELETE + CREATE

**上游出处**：design §BUG-059、`ApocTriggerInstaller.REL_TIMESTAMP_TRIGGER`

**根因**：`cdc-rel-timestamp` trigger 使用 `phase:'afterAsync'`，在 BUG-059
修复后**只响应 `$createdRelationships` 分支**（`$assignedRelationshipProperties`
分支已移除，避免 switchover 期间老 primary 的 async 任务迟到污染 standby）。
对**已有关系**执行 `SET r.x = ...` 不会 fire trigger，`r._updated_at` 不刷新，
CDC 永远捕获不到这次变更，standby 上 `r.x` 永远是创建时的值。

#### ❌ 禁用

```cypher
MATCH ()-[r:RelType {id: $id}]->()
SET r.status = 'inactive';        -- standby 永远看不到 'inactive'

MATCH ()-[r:RelType {id: $id}]->()
SET r.weight = r.weight + 0.1;    -- 同样永远同步不到 standby
```

#### ✅ 推荐（保留所有原属性 + 应用更新）

```cypher
MATCH (s)-[r:RelType {id: $id}]->(o)
WITH s, o, properties(r) AS props
MATCH (s)-[r2:RelType {id: $id}]->(o)
DELETE r2
WITH s, o, props
CREATE (s)-[r3:RelType]->(o)
SET r3 += props,
    r3 += $update_props,
    r3.createdAt = timestamp()          -- R1 契约，见下
RETURN count(*) AS cnt
```

**要点**：

- `WITH s, o, properties(r) AS props` 必须在 DELETE 前快照原属性
- 第二次 `MATCH r2` 是 `WITH` 投影之后重新绑定变量
- 新关系的 `r3.createdAt = timestamp()` **必须是当前时间**（不能继承旧关系的
  `createdAt`），否则 healer 的 `r.createdAt < $now - 5000` watermark 会误判
- 整条 Cypher 在单个隐式事务内原子提交

#### 豁免条件

只有一种情况可以保留 `MERGE ... ON MATCH SET r.x = ...`：**幂等重建路径上
caller 保证不会用同一 key 传递不同的属性值**。典型如批量插入用全新生成的
UUID 作 `triple_id`，ON MATCH 路径实际永不触发。

豁免必须**显式写在注释里**说明依赖的 caller 不变性。

---

## 五、推荐契约（R1）

### R1 — 关系 CREATE 时写 `r.createdAt = timestamp()`

**上游出处**：design §BUG-062、ops §"Client relationship write contract"

**根因**：APOC `cdc-rel-timestamp` trigger 在 `afterAsync` 异步执行器里跑，
**经验上有 ~0.5% 的丢任务率**——事务已经 commit，但 stamp 任务被静默丢弃。
这些关系的 `_elementId / _updated_at / _created_at` 永远为 NULL，CDC keyset
轮询永远捕获不到，成为 **naked rel**。

`NakedRelationshipHealer` 负责兜底修复，它有两条路径：

| 路径 | 触发条件 | 扫描复杂度 |
|------|---------|-----------|
| **Fast path** | 关系带 `r.createdAt` | `r.createdAt` 范围索引 seek，只扫上次 cursor 以后新建的关系 |
| **Slow path** | 关系缺 `r.createdAt` | `r._elementId IS NULL` 范围索引扫描，每次 agent 重启要把整个类型的索引空范围重扫 |

两条路径都不会退化为全图扫描，但千万级关系量时 fast path 让稳态扫描成本从
`O(total_rels_of_type)` 降到 `O(new_rels_since_last_scan)`。

#### ✅ CREATE 路径

```cypher
CREATE (a)-[r:RelType {createdAt: timestamp(), ...other_props}]->(b)
```

#### ✅ MERGE 路径（幂等）

```cypher
MERGE (a)-[r:RelType {natural_key: $key}]->(b)
ON CREATE SET r.createdAt = timestamp(),
              r.other_prop = $other
```

#### 重要约束

- **必须是 Cypher `timestamp()`**（Neo4j 侧落盘时刻），不能是 Python 侧构造的
  `int(time.time() * 1000)`——后者早于实际 commit 时刻，healer 的 5s watermark
  会判断错。
- **独立于业务 `created_at`**（snake_case）——两者**必须共存**，不能合并。
  `createdAt` 是 HA 契约字段（驼峰、不带下划线前缀），只给 healer 用；
  `created_at` 是业务查询/排序字段。两者是不同的 property key。
- **不要写节点的 `createdAt`**——healer 目前只处理关系，节点 trigger 走
  `before` phase + FOREACH 已修复（BUG-063），不存在 naked node 问题。

#### 日志判断是否合规

HA Agent 日志中若出现：

```
NakedRelationshipHealer (slow-path) healed N 'RelType' rel(s);
client is not writing r.createdAt — consider contract migration
```

说明对应关系类型未落实契约，走的是 slow path。

---

## 六、Driver / 路由层约束

### 6.1 Driver / Session 生命周期（基础）

> Switchover 期间的稳定性**强依赖于客户端正确管理 Driver / Session 生命周期**。
> 错误的生命周期模式会让 D2 的退避重试装饰器失效，甚至直接打爆 HAProxy 连接池。

#### 三层对象的生命周期约束

| 对象 | 推荐生命周期 | 线程安全 | 错误模式 |
|------|------------|---------|---------|
| `Driver` | **进程级单例**（`@lru_cache` / module-level / DI singleton） | ✅ 完全线程安全 | ❌ 每请求 `new Driver()`：连接池反复重建，switchover 期间制造连接风暴打爆 HAProxy `maxconn` |
| `Session` | **每业务操作一个**，with 块自动关闭 | ❌ 不是线程安全 | ❌ 长生命周期 session（跨请求复用）：transaction 可能跨过 switchover 边界，事务内部异常无法被 D2 重试覆盖 |
| `Transaction` | **由 `session.execute_write` / `execute_read` 托管** | N/A | ❌ 手动 `session.begin_transaction()` 不调 commit / 不在 with 块里：连接泄露，pool 耗尽 |

#### ✅ Python 推荐模式（Driver 单例）

```python
import os
from functools import lru_cache
from neo4j import GraphDatabase, WRITE_ACCESS, READ_ACCESS

# 进程级单例：driver 内部维护连接池，重复创建会撑爆 HAProxy
@lru_cache(maxsize=1)
def get_write_driver():
    return GraphDatabase.driver(
        os.environ["NEO4J_WRITE_URI"],            # 走 HAProxy WRITE 池（端口约定见部署文档）
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASS"]),
        max_connection_pool_size=50,              # 单进程上限，对齐 HAProxy 后端 maxconn 留余量
        connection_acquisition_timeout=60,        # 拿不到连接 60s 后 fail-fast，避免无界堆积
        max_transaction_retry_time=30,            # 对齐 §6.2 D1/D2 的 ~30s 总预算
        keep_alive=True,
    )

@lru_cache(maxsize=1)
def get_read_driver():
    return GraphDatabase.driver(
        os.environ["NEO4J_READ_URI"],             # 走 HAProxy READ 池
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASS"]),
        max_connection_pool_size=200,             # 读侧并发更高，pool 调大
        connection_acquisition_timeout=10,
        keep_alive=True,
    )
```

#### ✅ Python 推荐模式（Session 短生命周期 + 托管 tx）

```python
def write_person(name: str, age: int):
    with get_write_driver().session(default_access_mode=WRITE_ACCESS) as s:
        s.execute_write(
            lambda tx: tx.run(
                "MERGE (n:Person {business_id: $bid}) "
                "ON CREATE SET n.name = $name, n.age = $age "
                "ON MATCH SET n.name = $name, n.age = $age",
                bid=name, name=name, age=age,
            )
        )

def read_person(bid: str):
    with get_read_driver().session(default_access_mode=READ_ACCESS) as s:
        return s.execute_read(
            lambda tx: tx.run(
                "MATCH (n:Person {business_id: $bid}) RETURN n", bid=bid
            ).single()
        )
```

要点：

- **Driver 用 read / write 两个独立实例**：连接池不互相挤占；switchover 期间写池阻塞
  时读侧不被连带误杀
- **Session 用 with 块**：异常路径也会归还连接到 pool
- **`execute_write` / `execute_read` 是托管事务**：driver 自动重试
  `ServiceUnavailable` / `SessionExpired` / `TransientError`（但**不覆盖 BUG-083
  `ConstraintError`**，要走 D2 应用层装饰器，详见 §6.2）

#### ❌ 反模式（任意一条命中都会在 switchover 时崩）

```python
# ❌ 反模式 1：每次请求 new Driver
def write_person(name):
    drv = GraphDatabase.driver(URI, auth=AUTH)    # 每次都 new pool
    with drv.session() as s:
        s.run("CREATE (:Person {name: $n})", n=name)
    drv.close()
    # → switchover 时 100 QPS × 50 conn-per-driver = 5000 半开连接打爆 HAProxy

# ❌ 反模式 2：长生命周期 session
class PersonRepo:
    def __init__(self, driver):
        self.session = driver.session()           # 跨请求复用 session
    def write(self, name):
        self.session.run("CREATE (:Person {name: $n})", n=name)
    # → 一个 session 对应一个连接；switchover 期间这个连接被强制关闭，
    #   self.session 进入 broken 状态，所有后续请求直接抛 SessionExpired

# ❌ 反模式 3：fork 后复用 Driver
driver = GraphDatabase.driver(URI, auth=AUTH)     # 父进程创建
def worker():
    with driver.session() as s:                   # 子进程使用 → 连接状态错乱
        s.run(...)
multiprocessing.Process(target=worker).start()
# 修复：fork 后子进程内重新调 get_write_driver.cache_clear() 再用
```

#### 跨进程模型（uvicorn / gunicorn worker / celery）

- **gunicorn `--preload` + Driver 单例**：必须在 `post_fork` hook 里
  `get_write_driver.cache_clear()`，让每个 worker 自建 pool
- **uvicorn workers**：每个 worker 独立进程，模块级 lru_cache 自然隔离，无需特殊处理
- **celery prefork**：同 gunicorn，需要 `worker_process_init` 信号清缓存
- **asyncio**：使用 `neo4j.AsyncGraphDatabase`，单 event loop 内 driver 单例

#### Driver close 时机

进程退出时 driver 应当 close：

```python
import atexit
atexit.register(lambda: get_write_driver().close())
atexit.register(lambda: get_read_driver().close())
```

容器化部署下 `SIGTERM` → process exit → atexit fires，连接被优雅关闭，避免在
HAProxy 后端留下 `CLOSE_WAIT` 半开连接（这会让 HealthChecker 暂时误判节点降级）。

---

### 6.2 D1 / D2 — Switchover 窗口的写重试

**上游出处**：

- design §6.9.7 客户端合同
- design §"Failover 窗口写入损失的设计边界"
- design §BUG-083（新增 `ConstraintError(_elementId)` 重试场景）

#### 客户端类型表

| 客户端类型 | 切换期间行为 | 要求 |
|-----------|------------|------|
| Neo4j driver **managed transaction** (`session.execute_write(tx -> ...)`) | 自动捕获 `ServiceUnavailable` / `SessionExpired` 并重试，默认 30s 内 | ✅ 开箱即用，业务无感 |
| Neo4j driver **非托管** (`session.run` / 手动 tx) | 拿到显式 Exception | ⚠️ 应用层必须自行重试或接受失败 |

#### SLA

- Write 侧 p99 延迟在切换期间会有 **1-2 s 尖峰**
- 最坏 failover 场景 `T_failover_P99 ≈ 18 s`（v1.0.0-baseline 实测 17.4s）
- Read 侧完全不受影响
- **写成功 = CDC 必然捕获**，不存在"静默丢数据"场景

#### 退避序列（弱约束，产品期望值）

| 第 N 次 retry | 建议延迟 + jitter | 累计等待 | 这时候集群在做什么 |
|-------|------------------|---------|-------------------|
| 1 | 500 ms + jitter(0, 100 ms) | 0.5 s | 瞬态抖动的乐观试探 |
| 2 | 1 s + jitter(0, 200 ms) | 1.5 s | HAProxy tcp-check 过一两拍 |
| 3 | 2 s + jitter(0, 500 ms) | 3.5 s | HealthChecker 第一轮 SUSPECT 到位 |
| 4 | 4 s + jitter(0, 1 s) | 7.5 s | SUSPECT → DOWN 升级中 |
| 5 | 8 s + jitter(0, 2 s) | ~15 s | FailoverOrchestrator 大概执行完 |
| 6 | 16 s + jitter(0, 4 s) | ~31 s | 兜底，覆盖 P99 长尾；仍失败则 fast-fail |

**硬边界**：

- 总预算 ~30 s（对齐 `T_failover_P99` + 一倍安全边际）
- 单次 retry 上限 16 s
- **Jitter 必须**：无 jitter 的并发客户端会同步打到新 primary，形成
  thundering herd 耗尽 HAProxy `retries 3` 预算

#### 可重试异常清单

| 异常 | 来源 | 重试理由 |
|------|------|---------|
| `ServiceUnavailable` | Driver 与服务端断连 | switchover 写阻断 / TCP 中断 |
| `SessionExpired` | Bolt session 失效 | switchover HAProxy `shutdown sessions` 强制断连（BUG-042）|
| `TransientError` (`Neo.TransientError.*`) | Neo4j 标记的瞬时错误 | 死锁 / 临时资源不足 |
| **`ConstraintError`** WHERE message contains `_elementId`<br>OR code = `Neo.ClientError.Schema.ConstraintValidationFailed` | **BUG-083 撞值** | 上游 `_elementId` UNIQUE 约束（`uniq_elementid_<Label>`）在 reconciler stamping 与本地 CREATE 撞值瞬间触发。**这不是业务 bug**——新事务会让 Neo4j 重新分配 `elementId`，重试通常一次就过。频次极低（实测 7600+ 写入 0~1 次）|

#### 6.2.1 D2 的前置硬约束：被重试的写操作必须幂等

**重试是一把双刃剑**：D2 的退避循环可能在 retry 时重复执行业务写操作。如果业务写
**不是幂等的**，retry 会在数据库里制造重复数据——**且 standby 也会忠实地复制这些
重复数据**，最终下游业务读到 N 条本应只有 1 条的记录。

> 关键原则：**任何被 D2 装饰器包裹 / 任何用了 driver `execute_write` 的写操作，
> 必须自身幂等。**

#### ❌ 非幂等写法（retry 一次 → 2 个节点）

```cypher
CREATE (:Person {name: $name, age: $age})

CREATE (a)-[:KNOWS {weight: $w}]->(b)

UNWIND $batch AS row CREATE (:Event {ts: row.ts, payload: row.payload})
```

#### ✅ 幂等写法（基于业务自然键的 MERGE）

```cypher
-- ✅ 节点：用业务唯一键 MERGE
MERGE (n:Person {business_id: $bid})
ON CREATE SET n.name = $name, n.age = $age, n.created_at = timestamp()
ON MATCH  SET n.name = $name, n.age = $age

-- ✅ 关系：用 (start, end, 自然键) 三元组 MERGE，配合 R1 契约
MATCH (a:Person {business_id: $aid}), (b:Person {business_id: $bid})
MERGE (a)-[r:KNOWS {edge_id: $edge_id}]->(b)
ON CREATE SET r.weight = $w, r.createdAt = timestamp(), r.created_at = timestamp()
ON MATCH  SET r.weight = $w

-- ✅ 批量：每条带业务唯一键（event_id）
UNWIND $batch AS row
MERGE (e:Event {event_id: row.event_id})
ON CREATE SET e.ts = row.ts, e.payload = row.payload
```

#### 不需要业务唯一键的天然幂等场景

| 操作 | 是否天然幂等 | 说明 |
|------|------------|------|
| `MATCH ... SET n.x = $v` | ✅ 是 | 同一值赋两次 = 一次 |
| `MATCH ... DELETE n` | ✅ 是 | 第二次 retry MATCH 不到，no-op |
| `MATCH ... REMOVE n.tmp` | ✅ 是 | 同 DELETE 语义 |
| `OPTIONAL MATCH ... DELETE` | ✅ 是 | 同上 |
| 任何 `MERGE` | ✅ 是 | 这是 MERGE 的定义 |
| `CREATE` 无自然键 | ❌ 否 | retry 一次 = 2 条 |
| `CREATE` 关系 | ❌ 否 | retry 一次 = 平行边 |
| `UNWIND ... CREATE` 批量 | ❌ 否 | 整批被 2× |

#### BUG-083 `ConstraintError` 与幂等性的相互作用

BUG-083 触发 `ConstraintError(_elementId)` 时，事务**已经在 Neo4j 端 abort 整体回滚**
——所以下次重试就是干净的 retry，**不会出现"上次写了一半"**。这意味着：

- ✅ 客户端用 MERGE 写：幂等 + 自动 abort + retry → 完全安全
- ⚠️ 客户端用裸 CREATE 写：第一次 CREATE 撞值 abort，retry 时 Neo4j 重新分配
  `elementId` → 重试通常会成功，但**新节点的 elementId 与第一次 CREATE 的不同**。
  如果业务靠 driver 返回的 `elementId(n)` 做后续关联，retry 会让关联指向不存在的 id。
  → **再次确认：写路径必须 MERGE + 业务自然键**，不要把 Neo4j 的 `elementId(n)`
  当业务 ID 用。

#### 审计 grep（找潜在的非幂等写）

```bash
grep -rnE 'CREATE\s*\(\s*:[A-Z]' src/                   # CREATE 节点字面量
grep -rnE 'CREATE\s*\([^)]+\)-\[[^]]*:[A-Z]' src/       # CREATE 关系字面量
grep -rnE 'UNWIND.*CREATE\b' src/                        # 批量 CREATE
# 命中后人工核查：是否有 MERGE 替代？是否有业务唯一键？
```

---

#### 6.2.2 完整 `@ha_write_retry` 装饰器

```python
"""ha_retry.py — 业务侧统一写重试装饰器
====================================================================
契约出处：docs/nuclear-fusion/operations/ha-client-contract.md §6.2
"""
import logging
import random
import time
from functools import wraps

from neo4j.exceptions import (
    ServiceUnavailable,
    SessionExpired,
    TransientError,
    ConstraintError,
)

logger = logging.getLogger(__name__)

# §6.2 退避序列。第一列是基础延迟 ms，第二列是 jitter 上界 ms
_BACKOFF_SCHEDULE_MS = [
    (500,    100),
    (1000,   200),
    (2000,   500),
    (4000,  1000),
    (8000,  2000),
    (16000, 4000),
]

_RETRYABLE_BASE = (ServiceUnavailable, SessionExpired, TransientError)


def _is_retryable(exc: BaseException) -> bool:
    """决定一个异常是否应当被 D2 重试。

    覆盖：
      1. driver 标记的瞬时错误：ServiceUnavailable / SessionExpired / TransientError
      2. BUG-083 _elementId UNIQUE 撞值（HA infra 抖动，非业务 bug）
    """
    if isinstance(exc, _RETRYABLE_BASE):
        return True

    if isinstance(exc, ConstraintError):
        # 兼容两种识别方式：错误码 + 错误消息文本
        code = getattr(exc, "code", "") or ""
        msg = (str(exc) or "").lower()
        if code == "Neo.ClientError.Schema.ConstraintValidationFailed" and (
            "_elementid" in msg or "uniq_elementid_" in msg
        ):
            return True

    return False


def ha_write_retry(
    *,
    max_total_seconds: float = 30.0,
    on_retry: callable = None,
    metric_label: str = "default",
):
    """业务侧写重试装饰器。

    使用约束（与 §6.2 契约硬绑定）：
      1. 装饰的函数**必须幂等**（见 §6.2.1）。
      2. **不可与 driver 的 execute_write 叠加**（嵌套重试 ≈ 30s × 7 = 210s
         违反契约硬上限）。如果函数内部已经用了 execute_write，**不要再加这个装饰器**。
      3. 异步函数请使用同名的 async 版本（本文件未列出，按 asyncio.sleep 替换 time.sleep
         即可）。

    Args:
        max_total_seconds: 总预算硬上限，超过则放弃 fast-fail（默认 30s 对齐
            T_failover_P99）。
        on_retry: 可选回调 ``on_retry(attempt, exc, delay_s)``，业务用来打 metric / log。
        metric_label: 给 on_retry 的标识，方便区分不同业务路径的 retry 频次。
    """

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            last_exc = None

            for attempt, (base_ms, jitter_ms) in enumerate(_BACKOFF_SCHEDULE_MS, start=1):
                try:
                    return fn(*args, **kwargs)
                except BaseException as exc:
                    if not _is_retryable(exc):
                        raise

                    elapsed = time.monotonic() - start
                    if elapsed >= max_total_seconds:
                        logger.error(
                            "ha_write_retry [%s] giving up after %.2fs (budget=%.1fs); "
                            "last_exc=%s",
                            metric_label, elapsed, max_total_seconds, repr(exc),
                        )
                        raise

                    last_exc = exc
                    delay_ms = base_ms + random.randint(0, jitter_ms)
                    delay_s = delay_ms / 1000.0

                    # 别冲过总预算；剩余预算不够下一轮就 fast-fail
                    remaining = max_total_seconds - elapsed
                    if delay_s >= remaining:
                        logger.error(
                            "ha_write_retry [%s] next backoff %.2fs > remaining %.2fs; "
                            "fast-fail at attempt=%d, last_exc=%s",
                            metric_label, delay_s, remaining, attempt, repr(exc),
                        )
                        raise

                    logger.warning(
                        "ha_write_retry [%s] attempt=%d backoff=%.2fs exc=%s",
                        metric_label, attempt, delay_s, type(exc).__name__,
                    )
                    if on_retry is not None:
                        try:
                            on_retry(attempt, exc, delay_s)
                        except Exception:                       # noqa: BLE001
                            logger.exception("ha_write_retry on_retry callback failed")

                    time.sleep(delay_s)

            # 所有退避都用完了
            logger.error("ha_write_retry [%s] exhausted schedule; last_exc=%s",
                         metric_label, repr(last_exc))
            raise last_exc

        return wrapper

    return decorator
```

#### 用法

```python
@ha_write_retry(metric_label="person_upsert")
def upsert_person(driver, business_id: str, name: str):
    """注意：函数体里**不要再用** driver.session().execute_write()。
    要么用 execute_write 让 driver 自动重试（对 BUG-083 不生效），
    要么用本装饰器手动控制（必须自己写 with session().run）。
    """
    with driver.session(default_access_mode=WRITE_ACCESS) as s:
        s.run(
            "MERGE (n:Person {business_id: $bid}) "
            "ON CREATE SET n.name = $name, n.created_at = timestamp() "
            "ON MATCH  SET n.name = $name",
            bid=business_id, name=name,
        )
```

#### 何时用 driver `execute_write`、何时用 `@ha_write_retry`

| 场景 | 推荐方案 | 理由 |
|------|---------|------|
| 写路径不可能撞 BUG-083（罕见，几乎所有路径都可能） | driver `execute_write` | 官方实现，无需维护本地代码 |
| 写路径可能撞 BUG-083（默认假设） | `@ha_write_retry` 包裸 `session.run` | 覆盖 `ConstraintError(_elementId)` |
| 业务需要 metric 埋点（`ha_write_retry_total{label="..."}`） | `@ha_write_retry` + `on_retry` 回调 | driver 内部不暴露 retry 钩子 |
| 异步路径（FastAPI / asyncio worker） | `@ha_write_retry` 的 async 版本 | driver `execute_write` 的 async 版本同样不覆盖 BUG-083 |

> ⚠️ **二选一硬约束**：同一个写函数上**不要同时使用** `execute_write` 和
> `@ha_write_retry`。两者都是 retry 层，叠加后最坏总预算 ≈ 30s × 7 = 210s，
> 违反契约 ~30s 硬上限，且 driver 层的 backoff 和应用层 backoff 互相干扰会让
> 实际行为不可预测。

---

### 6.3 D3 — 读写分流

**上游出处**：ops §HAProxy 双池配置

| 场景 | 路由 | HAProxy 池 |
|------|------|-----------|
| 写入（CREATE / MERGE / SET / DELETE）| WRITE | `neo4j_primary` (7687) |
| DDL（CREATE CONSTRAINT / INDEX）| WRITE | `neo4j_primary` (7687) |
| 只读查询（RAG 注入 / 社区摘要读 / 统计）| READ | `neo4j_all` (7688) |
| 强一致读（极少数场景）| PRIMARY_READ | `neo4j_primary` (7687) |

#### 为什么读要走 READ

- 不占用写 pool 连接（避免影响写吞吐）
- Switchover 期间写被 HAProxy block 时，**只读查询不被连带误杀**（`neo4j_all`
  池照常服务）
- 读负载可以分散到 N standby（水平扩展读）

#### Standby 直连写的禁令

业务**不能绕过 HAProxy 直连 standby 节点写入**。standby 上没有 trigger，
直连写入既不会出现在其他节点也不会被 CDC 捕获。fencing token 会拒绝大部分场景，
但绕过 HAProxy 直连 7687 的 driver **不会经过 fencing 检查**。运维必须从
网络/账号策略上禁止业务直连 standby 写入。

#### DDL 路由

`CREATE CONSTRAINT` / `CREATE INDEX` 必须走 WRITE route。读副本执行 DDL 会在
Cypher 层失败，错误暴露较晚。建议在业务的 `ensure_constraints` 入口对
非 WRITE route 直接 `raise RuntimeError`，让错误**在入口就暴露**。

---

## 七、规模与事务形态约束（L1 / L2）

### L1 — 大事务

`design §6.9.2 Phase 3` 在 switchover 时做 CDC 最终排空 poll（最多 10 轮，每轮
1s）。如果有个 Cypher tx 刚 commit 了成千上万条关系的 DELETE+CREATE，CDC 必须
在 drain 窗口内把 2N 条事件全部推入 Redis Stream，否则这些事件会因 Phase 3
停 CDC 而延迟到新 primary 上重新捕获。虽然不会丢数据，但会**拉长 switchover
时长、在 standby 上产生瞬时不一致**。

**建议**：单个 Cypher 影响的关系数 > 千时，用 `WITH r LIMIT N` 做批量循环。

```cypher
// ✅ 分批模板
MATCH (n:Doc {id: $doc_id})-[r:HAS_TRIPLE]->(t)
WITH r LIMIT 5000
DELETE r
RETURN count(*) AS deleted
// 调用方循环直到返回 0
```

### L2 — 跨 `session.run` 的"原子操作"

`session.run(A)` + `session.run(B)` 是**两个独立事务**，不是原子的。如果 A
成功 B 失败，中间状态会被 CDC 捕获并同步到 standby，可能导致短暂的逻辑不一致。

**建议**：依赖原子性的操作合并到单条 Cypher（通过 `WITH` 链式）或用 managed
tx 的单 tx 内多 statement。

```python
# ❌ 非原子
session.run("MATCH (c:Community)-[r:BELONGS_TO]-() DELETE r")
session.run("UNWIND $members AS m MERGE (c)-[:BELONGS_TO]-(m)")

# ✅ 原子
session.run("""
    OPTIONAL MATCH (c:Community {id: $cid})-[old:BELONGS_TO]-()
    DELETE old
    WITH DISTINCT c
    UNWIND $members AS mid
    MATCH (m {business_id: mid})
    MERGE (c)-[r:BELONGS_TO]->(m)
    ON CREATE SET r.createdAt = timestamp()
""", cid=cid, members=members)
```

---

## 八、节点属性变更（无特殊约束 + 注意点）

与关系不同，**节点属性变更没有 BUG-059 等价的约束**——`cdc-timestamp-assigned`
trigger 在 BUG-063 修复后用 FOREACH + `before` phase，对 `$assignedNodeProperties`
分支正常 fire，`SET n.x = ...` 会刷新 `_updated_at` 并被 CDC 捕获。

所以对节点可以直接：

```cypher
MATCH (n:Person {business_id: $id}) SET n.name = 'Bob'              -- ✅
MATCH (n:Person {business_id: $id}) SET n += {name: 'Bob', age: 32} -- ✅
```

**节点属性 REMOVE**：Neo4j 5.x 语义上 `SET n.x = NULL` ≡ `REMOVE n.x`，
APOC 5.x `$removedNodeProperties` 对两种形态统一捕获，CDC 等价。

```cypher
MATCH (n:Person {business_id: $id}) SET n.tmp = NULL  -- ✅
MATCH (n:Person {business_id: $id}) REMOVE n.tmp     -- ✅ 等价
```

> ⚠️ 但**不能 REMOVE 任何 `_*` 系统保留属性**（见 §3.1）。

---

## 九、自检脚本与 grep 审计

### 9.1 客户端代码 grep checklist

```bash
# R0 — 系统命名空间
grep -rnE 'SET\s+[a-zA-Z_]+\._(elementId|updated_at|created_at|type|labels)\b' src/
grep -rnE 'REMOVE\s+[a-zA-Z_]+\._(elementId|updated_at|created_at|type|labels)\b' src/
grep -rnE '_(elementId|updated_at|created_at|type|labels)\s*:' src/
grep -rnE ':_[A-Z][A-Za-z]' src/                                          # 系统 label / reltype

# S1 — GDS 写
grep -rnE 'gds\.[a-z.]+\.write\b' src/
grep -rnE 'gds\.graph\.(writeNodeProperties|writeRelationship)\b' src/
grep -rnE 'writeProperty:' src/

# S2 — 关系属性 SET
grep -rnE 'SET\s+r[a-z0-9]?\.[a-z]' src/         # SET r.xxx = ... （需人工判断是否新建关系）

# R1 — 关系 createdAt 缺失
grep -rnE 'CREATE\s*\([^)]*\)-\[r?:[A-Z]' src/   # CREATE 关系字面量，需检查是否带 createdAt

# L1 — 大事务无分批
grep -rnE 'DELETE\s+[a-z]\b' src/                # 找 DELETE 后人工核查是否带 LIMIT
```

### 9.2 集群侧自检（可选）

在任一节点执行，验证历史数据是否违反约束：

```cypher
-- 业务节点中缺少 _elementId 的（应为 0）
MATCH (n) WHERE NOT n:_CDCDeleteEvent
  AND NOT n:_TriggerReadinessProbe
  AND NOT n:_TestPing
  AND n._elementId IS NULL
RETURN labels(n) AS lbl, count(*) AS cnt;

-- 业务节点中 _elementId 与 local elementId 不一致的
-- standby 上正常会有，primary 上应为 0（除非经历过 switchover）
MATCH (n) WHERE NOT n:_CDCDeleteEvent
  AND n._elementId IS NOT NULL
  AND n._elementId <> elementId(n)
RETURN labels(n) AS lbl, count(*) AS cnt;

-- BUG-083 dup _elementId 检查（应为 0）
MATCH (n) WHERE n._elementId IS NOT NULL
WITH n._elementId AS eid, count(*) AS c WHERE c > 1
RETURN count(eid) AS dup_groups, sum(c) AS poisoned_nodes;
```

prom 巡检：

```
neo4j_ha_dup_element_id_nodes == 0     # BUG-083 健康判据
```

---

## 十、新增 / 修改代码时的 checklist

写任何新的 Neo4j 操作前，过一遍以下 13 点：

### 写法层

- [ ] **是写入还是只读？** 写走 WRITE route，读走 READ route（D3）
- [ ] **属性 / label / reltype 名是否带 `_` 前缀？** 不能带（R0）
- [ ] **是否在写 `_elementId` / `_updated_at` / `_created_at` / `_type`？** 任何路径都不能（R0）
- [ ] **涉及 GDS 算法回写？** 必须 `.stream + MATCH + SET`，绝对禁止 `.write` / `.mutate→write`（S1）
- [ ] **GDS 全流程是否 pin 到同一个 WRITE session？** `gds.graph.project / drop / stream` 三件套必须在同一个 primary 上跑，且 `try/finally` 保证 drop（S1 拓扑约束）
- [ ] **修改已有关系的属性？** 必须 DELETE+CREATE，不能 `SET r.x = ...`（S2）
- [ ] **CREATE 新关系？** 带上 `r.createdAt = timestamp()`（R1）

### 重试 / 幂等层

- [ ] **写操作是否幂等？** 用 MERGE + 业务自然键（不是 `_elementId`），或确认操作天然幂等（pure SET / DELETE / REMOVE）。**不幂等的写不能上重试装饰器**（A1 / §6.2.1）
- [ ] **是否在 switchover 窗口可能被打断？** 写路径加 `@ha_write_retry`（或用 driver `execute_write`），二者**不能叠加**（D1 / D2 / §6.2.2）
- [ ] **`is_retryable` 是否覆盖 `ConstraintError(_elementId)`？** driver 默认不覆盖，BUG-083 撞值场景必须由应用层补（D2）

### 生命周期 / 路由层

- [ ] **Driver 是否进程单例？** 不要每请求 `new Driver()`；fork 后子进程要 `cache_clear()`（§6.1）
- [ ] **Session 是否短生命周期 + with 块？** 不要跨请求持有 session（§6.1）
- [ ] **DDL？** 必须走 WRITE route，不能碰 `_elementId` 索引（D3 + R0 §3.4）

### 规模 / 原子性层

- [ ] **事务规模？** 单 tx 影响行数 > 千时，用 `LIMIT` 分批（L1）
- [ ] **跨多条 `session.run` 的"原子"操作？** 合并到单条 Cypher 或单 tx 内多 statement（L2）

---

## 十一、变更与审计记录

| 日期 | 事件 | 关联 |
|------|------|------|
| 2026-04-21 | 下游 `oversea-chatbot` 首次发布 client contract | `oversea-chatbot/docs/neo4j_ha_client_contract.md` |
| 2026-04-22 | BUG-082 落地（`REL_DELETE_SCOPED` + endpoint stamping）| `ApocTriggerInstaller.REL_DELETE_TRIGGER` / `CypherTemplates.REL_DELETE_SCOPED` |
| 2026-04-22 | BUG-083 落地（`_elementId` UNIQUE 约束 + dup gauge）| `IndexInstaller` / `HaMetrics` / `scripts/deploy/elementid-dedup.sh` |
| 2026-04-23 | v1.0.0-baseline 三项回归全绿（load-switchover / failover-chaos / backup-pause）| 见 `docs/nuclear-fusion/reviews/baseline-2026-04-23/` |
| 2026-04-23 | 本文档创建：把分散在 `ha-agent-cluster-operations.md §2.1` / design 各 BUG 节里的客户端约束整合为单一 source of truth；新增 BUG-083 引入的 `ConstraintError(_elementId)` 重试场景、完整保留命名空间清单（5 系统属性 + 3 系统 label + 1 系统 reltype + 6 系统 trigger 名）| 本文件 |
| 2026-04-23 | 补齐"代码开发"维度盲区：① §6.1 Driver / Session 生命周期模式（含反模式 + uvicorn / gunicorn / celery / fork 适配）② §6.2.1 重试前置硬约束 — 写路径必须幂等（含审计 grep）③ §6.2.2 完整 `ha_write_retry` 装饰器（120 行，可直接 copy 进下游）④ §S1 GDS 拓扑约束 — `.project/.drop/.stream` 三件套必须 pin 到同一 WRITE session ⑤ §10 checklist 扩展为 13 点 | 本文件 |

---

## 十二、参考

### 上游（本仓库）

- **设计**：`docs/nuclear-fusion/design/modules/ha-agent-design.md`
  - §6.9.2 Switchover 编排
  - §6.9.7 客户端合同
  - §BUG-059（关系属性 SET 不 fire trigger）
  - §BUG-062（naked rel + `NakedRelationshipHealer` + `r.createdAt` 契约）
  - §BUG-063（GDS `.write` 禁用 + node trigger 拆分）
  - §BUG-067（`_CDCDeleteEvent` orphan 清理）
  - §BUG-080（`PostSwitchoverReconciler` 两阶段对账）
  - §BUG-081（PEL replay batch splitting on `_elementId` reuse）
  - §BUG-082（`REL_DELETE_SCOPED` + endpoint stamping）
  - §BUG-083（`_elementId` UNIQUE 约束 + dup gauge）
- **运维**：`docs/nuclear-fusion/operations/ha-agent-cluster-operations.md`
  - §2.1 业务方写 Cypher 的约束（CDC 同步前提）
  - §"Client relationship write contract"
  - §"GDS write contract"
- **核心代码**：
  - `src/ha-agent/src/main/java/com/neo4j/ha/agent/bootstrap/ApocTriggerInstaller.java`（保留属性 / trigger 名）
  - `src/ha-agent/src/main/java/com/neo4j/ha/agent/bootstrap/IndexInstaller.java`（保留 schema）
  - `src/sync-applier/src/main/java/com/neo4j/ha/sync/applier/CypherTemplates.java`（applier MERGE/DELETE 形态）

### 下游（业务仓库参考）

- `oversea-chatbot/docs/neo4j_ha_client_contract.md`（业务侧实施记录）
- `oversea-chatbot/src/core/knowledge_graph/_ha_retry.py`（D1/D2 实现样板）
- `oversea-chatbot/src/core/knowledge_graph/neo4j_route.py`（D3 实现样板）
