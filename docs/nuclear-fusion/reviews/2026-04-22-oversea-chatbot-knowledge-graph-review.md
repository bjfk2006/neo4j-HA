# Code Review Report — oversea-chatbot KG 接入 HA 契约对齐审计

**Date**: 2026-04-22
**Reviewer**: nuclear-fusion / reviewing-code skill
**Target**: `oversea-chatbot/src/core/knowledge_graph/**` (12 files, 4407 LOC)
**Reference contract**: `docs/nuclear-fusion/operations/ha-client-contract.md` (1085 行)
**Audit mode**: 静态对齐（不构建 / 不跑回归），按契约 13 项 checklist 逐条核验

---

## 1. Scope

| 维度 | 数值 |
|---|---|
| Files audited | 12（不含 `__init__.py`） |
| Total LOC | 4407 |
| 写路径文件 | `kg_neo4j_store.py` (1938) / `community_manager.py` (549) / `_ha_retry.py` (162) |
| 读路径文件 | `kg_vector_query_inject.py` (227) / `entity_recognizer.py` (103) / `graph_triple_format.py` (78) |
| 路由层 | `neo4j_route.py` (45) + `src/common/db_client/__init__.py`（间接） |
| 改动类型 | feature integration（已上线） |
| 已声明意图 | 在 oversea-chatbot 中接入 neo4j-HA 的读写分流 + switchover 重试 + GDS Leiden 社区检测 |

`kg_aligner.py` / `kg_tokenizer.py` / `triple_extractor.py` / `entity_recognizer.py` / `graph_triple_format.py` / `llm_response_util.py` 不直接持有 Neo4j driver / session，HA 维度判定为 **N/A**（仅做反例 grep 兜底）。

---

## 2. Summary

下游 KG 层**整体高度对齐**当前 HA 客户端契约：13 条 checklist 中 11 条完全符合或显式有意识跳过、1 条存在**已知未处置 gap**（M2，self-flagged）、1 条因上游契约新增而**未更新**（M1）。代码注释里多处直接引用契约章节（§S2/§R1/§D1/§D2/§D3），可读性远好于多数同类工程。

主要风险集中在**重试装饰器未覆盖 BUG-083 引入的新可重试异常**（M1），以及**两个单条三元组写接口被有意排除在重试之外**（M2，TODO 已存在）。两者都是**业务侧 5xx**层级的 UX 退化，不会造成数据不一致。

**Verdict**: APPROVE with suggestions（详见 §6）。

---

## 3. Dimension Scan

| Dimension | Status | Notes |
|---|---|---|
| Correctness（语义对齐） | ✅ | DELETE+CREATE 模式、createdAt 字段、`_drop_graph_if_exists` 兜底全部正确 |
| Security（系统命名空间） | ✅ | 全目录无 `_elementId / _updated_at / _created_at / _CDCDeleteEvent / _PROBE_REL` 字面量；触发 grep 命中均为业务字段（`entity_type` / `predicate_type` 等子串误判） |
| Performance（大事务 / 重试预算） | ✅ | `set_doc_triples_status` / `delete_by_doc` / `delete_by_group` 均 5000/批 + skip-no-op 过滤；retry 总预算 ~31.5s 与上游 T_failover_P99 ≈ 20s 对齐 |
| Maintainability | ✅ | 契约引用直接写在 docstring 里；`_ha_retry.py` 162 行注释说明设计取舍 |
| Consistency（路由 / 幂等） | ⚠️ | 写方法多数走 MERGE+@ha_write_retry；但 `create_triple` / `add_triple_to_segment` 显式裸 CREATE 且不重试（self-flagged，见 M2）|
| Completeness（异常覆盖） | ⚠️ | `_RETRYABLE_EXCEPTIONS` 缺 `ConstraintError(_elementId)`（BUG-083 后新增项），见 M1 |

---

## 4. Findings

### Critical（blocks merge）

*None*。

---

### Major（should fix before merge）

#### **M1** — `_ha_retry.py:79` — 重试集合未覆盖 BUG-083 引入的 `ConstraintError(_elementId)`

```python
_RETRYABLE_EXCEPTIONS = (ServiceUnavailable, SessionExpired, TransientError)
```

**Problem**：

上游 `IndexInstaller` 在 BUG-083 后强制安装 `uniq_elementid_<Label>` UNIQUE 约束（见 `ha-agent-design.md` BUG-083 章节、`ha-client-contract.md` §6.2.2 完整 `@ha_write_retry` 装饰器）。在以下场景会抛 `Neo.ClientError.Schema.ConstraintValidationFailed`：

1. Switchover Phase 2 写阻断刚解除、`PostSwitchoverReconciler` 正在 stamping `_elementId`；
2. 此刻业务侧 retry 把同一条 MERGE 重新打到新 primary；
3. APOC trigger 给新建节点写 `_elementId` 时撞上 reconciler 已经写入的同 ID。

当前装饰器**把这种异常当业务异常透传**，业务侧拿到 5xx，违背了上游契约 §6.2.2 / D2 的"完整重试集合"条款。

**Impact**：

- 影响范围：所有 `@ha_write_retry()` 包装的方法（`kg_neo4j_store.py` 13 处 + `community_manager.py` 5 处 = **18 个写入入口**）。
- 严重程度：低概率（仅 switchover 与 retry 同时撞上 reconciler 写入的窗口），但发生时**整批写永久失败**——retry 不会展开。
- 数据一致性：**无影响**（约束本身保护了数据），仅 UX 退化。

**Fix**：

替换 `_ha_retry.py:79-156` 的固定 tuple + `except _RETRYABLE_EXCEPTIONS` 为可重试谓词函数。最小 diff（保留现有签名 / 退避序列 / jitter）：

```python
from neo4j.exceptions import (
    ConstraintError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

_RETRYABLE_BASE = (ServiceUnavailable, SessionExpired, TransientError)

# BUG-083 后 _elementId UNIQUE 约束在 switchover + reconciler stamping
# 与业务 retry 撞上时会抛 ConstraintValidationFailed，
# 上游契约（ha-client-contract.md §6.2.2）将其列为可重试。
def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, _RETRYABLE_BASE):
        return True
    if isinstance(exc, ConstraintError):
        code = getattr(exc, "code", "") or ""
        msg = (str(exc) or "").lower()
        return (
            code == "Neo.ClientError.Schema.ConstraintValidationFailed"
            and ("_elementid" in msg or "uniq_elementid_" in msg)
        )
    return False

# wrapper 内部:
async def wrapper(*args, **kwargs):
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            last_exc = exc
            # ... 现有 backoff + jitter 逻辑保持不变
```

**注意**：`_is_retryable` 必须**严格匹配** `_elementId` 关键词，避免吞掉业务侧的 `aiagent_entity_unique`（自然键）冲突——后者反映业务真重复，不能 retry，会导致死循环。

---

#### **M2** — `kg_neo4j_store.py:211-258` (`create_triple`)、`kg_neo4j_store.py:308-364` (`add_triple_to_segment`) — 单条三元组写接口未保护 switchover

```python
# kg_neo4j_store.py:212
async def create_triple(self, triple: dict) -> str:
    # TODO(HA §D1/§D2): 本方法使用裸 CREATE，非幂等，暂不包装 ha_write_retry。
    # 后续若将 CREATE 改为 MERGE (triple_id) 后可加装饰器。参见 review 条目 M5：
    # triple_id 无唯一约束 + 三处仍用 CREATE 非 MERGE，switchover 窗口 driver
    # 重试会产生重复关系。
```

**Problem**：

两个方法**有意识地**跳过 `@ha_write_retry`（TODO 注释直接引用了上一轮 review 的 M5 条目），原因是 Cypher 用裸 `CREATE (s)-[:aiagent_RELATES_TO {...}]->(o)`，retry 会产生重复边。这个判断**正确**——非幂等就不能盲 retry。

但**未处置**：当前仍**对外暴露这两个 endpoint**（`/extract` 单条添加路径、`/segment` 单条三元组添加路径），switchover 窗口落到这两个调用上的请求会**无重试直接 5xx**。下游业务（chatbot 修订流 / 管理后台）拿不到任何"暂时不可用，请重试"的语义提示。

**Impact**：

- 影响入口：`/extract` 和 `/segment` 的单条添加 API（高频管理操作）。
- 数据一致性：无影响（不会重复写）。
- UX：switchover 20 秒窗口内每个落在这两个接口上的请求都 5xx。
- 上游契约 §6.2.1 D2 前置硬约束明确："**被重试的写操作必须幂等**"——当前实现合规，但缺补救。

**Fix**（任选其一，按代价从低到高）：

1. **推荐**：把 `CREATE` 改成 `MERGE (s)-[r:aiagent_RELATES_TO {triple_id: $triple_id, bot_id: $bot_id}]->(o) ON CREATE SET ...`，与 `bulk_create_triples`（line 151）对齐，然后加 `@ha_write_retry()`。Cypher 已经为 triple_id 准备好（`triple_id = triple.get("triple_id", str(uuid.uuid4()))`，line 217），改动 < 10 行。
2. **次选**：保留裸 CREATE，在 `try/except` 里把 `(ServiceUnavailable, SessionExpired)` 转成业务可识别的 `KGTransientWriteError`，让 caller / 网关层做用户级 prompt + 重试，而不是 5xx。
3. **最低代价**：在两个方法的 docstring 顶上加显式 WARN 段，把"switchover 窗口请求会失败、调用方需自行重试"明确成 SDK 契约，而不是埋在 TODO 注释里。

**理由 Major 而非 Minor**：

虽然代码 self-flagged，但 SDK 把"补偿/重试"责任甩给 caller、又没在 SDK 边界给出明确异常类型（caller 不知道哪些是可重试的瞬时错），实际工程里 caller 大概率裸捕获 `Exception` → 把可重试错当永久错处理。**SDK 契约不完整 = 上游隐患**。

---

### Minor

#### **m1** — `community_manager.py:206-308` — GDS Leiden 工作流跨 5 个独立 session

```python
async with self.driver.session(database=self.database) as session:  # L206 project
    ...
async with self.driver.session(database=self.database) as session:  # L244 stats + stream
    ...
async with self.driver.session(database=self.database) as session:  # L270 read back
    ...
async with self.driver.session(database=self.database) as session:  # L291 cleanup
    ...
async with self.driver.session(database=self.database) as session:  # L307 drop in finally
    ...
```

**Problem**：

GDS catalog **per-server-process** 而非 per-session，多 session 之间在稳态下指向同一 primary，没问题。**但**在 switchover 窗口：每次 reconnect 后 driver 可能拿到新 primary 的连接，project 出来的 graph 在新 primary 上不存在 → stream/read 阶段抛 `Neo.ClientError.Procedure.ProcedureCallFailed`。这种异常**当前不在 `_RETRYABLE_EXCEPTIONS` 里**（合理，因为重试整个工作流而不是单 session）。

由于 `_run_leiden` 整体被 `@ha_write_retry()` + 入口 `_drop_graph_if_exists` + 出口 finally `_drop_graph_if_exists` 双重保护，**最终一致**。但有以下小改进空间：

- 5 次 connection acquisition 在 HAProxy 失活窗口期都会触发 driver 内部 routing 表刷新，放大瞬态延迟。

**Fix**：

把 5 个 `session.run()` 合并到同一个 `async with self.driver.session()` 块（保留 try/finally drop 即可）。一次 session 内连续 5 个 cypher，性能更好且 switchover 期间要么整体成功要么整体抛错（比当前更易诊断）。

**降级理由**：现有写法在生产环境是 working 的，仅是 best practice 偏离，不影响正确性。

---

#### **m2** — `_ha_retry.py:130` — wrapper 缺 `on_retry` metric 钩子

**Problem**：

上游契约 §6.2.2 推荐 `on_retry: Optional[Callable[[int, Exception, float], None]] = None` 给业务做 prometheus counter / 链路追踪埋点。当前只 `logging.warning`，运维侧无法做"retry 率"告警。

**Fix**：

```python
def ha_write_retry(
    ...,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
):
    ...
    if on_retry:
        try:
            on_retry(attempt + 1, exc, delay)
        except Exception:
            logging.exception("[KG HA retry] on_retry callback failed")
```

并在业务侧注册一个全局 `oversea_chatbot_kg_retry_total{exc_type, method}` Counter。

---

#### **m3** — `_ha_retry.py:46-50` — "禁止内嵌 execute_write" 是注释约束，无 runtime 防护

```
**硬约束：禁止在被本装饰器包装的方法内调用 `session.execute_write(tx_fn)` ...**
```

**Status**：当前 grep `execute_write|execute_read` 全 KG 目录**零命中** ✅，违反风险已被工程纪律消化。

**Suggest**（可选）：在 CI 加一条 `rg "execute_(write|read)" src/core/knowledge_graph/` 失败用例，把约束从注释升级为 build-time check，防止后续 contributor 误加。

---

#### **m4** — `kg_neo4j_store.py:1738`/`1753` — `merge_entities` 给迁移后的关系生成新 `triple_id = randomUUID()`

```cypher
CREATE (canon)-[nr:aiagent_RELATES_TO]->(target)
SET nr = properties(r), nr.triple_id = randomUUID(), nr.createdAt = timestamp()
```

**Observation**：

把 dup 节点的关系迁移到 canonical 时，`triple_id` 被替换为新 UUID（不再等于原值）。

**Risk**：

如果上游有外部系统按 `triple_id` 建立反向索引（比如 ES / 业务库存了一份 triple_id → metadata 的映射），合并后映射失效。

**Suggest**：

要么保留原 `triple_id`：`SET nr = properties(r), nr.createdAt = timestamp()`（删掉 randomUUID），让 caller 通过 `(start, type, end)` 自然键重新关联；要么在 merge_entities 返回值里携带 `{old_triple_id: new_triple_id}` 映射让 caller 同步外部索引。

**降级理由**：当前下游业务**没有**依赖 triple_id 跨系统关联（grep 验证），仅是埋雷。

---

### Info / Observations

#### **i1** — `kg_neo4j_store.py:80-88`、`kg_vector_query_inject.py:27-32` — 入口路由断言

两处都在方法入口 fail-fast 校验 `route_mode`（`ensure_constraints` 拒绝 READ ctx；`_build_graph_triple_text` 拒绝 WRITE ctx），完全符合契约 §D3。**值得保留这一模式**，新增方法时建议沿用。

#### **i2** — 系统命名空间洁净

13 条契约 grep 中：
- `_elementId / _updated_at / _created_at / _type / _labels` 在写入语句里**零命中**；
- `_CDCDeleteEvent / _TriggerReadinessProbe / _TestPing / _PROBE_REL` 全目录**零命中**；
- 仅 `kg_neo4j_store.py:1642`/`1675` 在 read-only 查询里调用 Neo4j 内置 `elementId(a) < elementId(b)` 函数做去重排序——**正确用法**，不是写系统属性。

下游对系统命名空间的隔离非常彻底，**值得表扬**。

#### **i3** — Driver 单例 + lazy init

`src/common/db_client/__init__.py:99-127` 用 `_neo4j_driver_cache` + double-checked lock 实现 per-URI driver 单例 ✅，符合 §6.1 Driver 生命周期。

**Possible improvement**（非必须）：`AsyncGraphDatabase.driver(uri, auth=...)` 没显式设置 `max_transaction_retry_time` / `max_connection_pool_size`。Driver 默认 `max_transaction_retry_time=30s` 与 `@ha_write_retry` 总预算 ~31.5s 是巧合一致。建议显式：

```python
driver = AsyncGraphDatabase.driver(
    uri, auth=(user, password),
    max_transaction_retry_time=30,
    max_connection_pool_size=100,
    connection_acquisition_timeout=60,
    keep_alive=True,
)
```

防止未来 driver 版本默认值漂移导致总预算失配。

#### **i4** — `bolt://` 单跳 vs `neo4j://` 路由

env 默认 `NEO4J_URI=bolt://10.55.77.3:7687`（`db_client/__init__.py:133/143`）。`bolt://` 是单跳 scheme，driver 不做 server-side routing，路由完全由 HAProxy + READ_URIS / WRITE_URIS 切分决定。

这一架构选择**与契约 §D3 兼容**，但跟契约文档里"`default_access_mode=WRITE_ACCESS`"那段建议**语义上有出入**——契约里那段是按 `neo4j://` 模式写的。下游用 `bolt://` 时不设 `default_access_mode` 是**正确的**（设了反而是 no-op）。

**建议契约文档**：在 §D3 / §6.1 补一段说明"如果用 `bolt://` 双 URI 物理分流方案，driver 端 `default_access_mode` 是 no-op，路由由 HAProxy 负责，无需设置"。这是契约文档的小遗漏，不是下游代码问题。

---

## 5. Strengths（值得表扬）

- **`kg_neo4j_store.py:797-826` `update_triple`**：完整执行 §S2（DELETE+CREATE 单 Cypher）+ §R1（`createdAt = timestamp()`），且 docstring 注释直接引用 BUG-059 解释设计动机。这是把契约"内化"成代码的范本。
- **`kg_neo4j_store.py:911-935` `set_doc_triples_status`**：5000/批循环 + `WHERE r.is_doc_active IS NULL OR r.is_doc_active <> $is_active` 过滤式重写，避免对已处于目标态的关系做无意义 DELETE+CREATE 放大 CDC 事件。这是**主动优化** CDC 通量的好实践。
- **`community_manager.py:218-220`**：在 Leiden 调用前直接注释解释"为什么不用 `gds.leiden.write`"——把 §S1 GDS 契约的反模式说明写在了使用现场。新人看到代码立刻知道"不能改回 `.write`"。
- **`_ha_retry.py:1-54`**：装饰器 docstring 详细写明退避表、总预算、可重试异常集、与 `execute_write` 的边界。**超出契约要求**地清晰，应作为其他 SDK 模块的注释模板。
- **`kg_vector_query_inject.py:27-32`**：SDK 边界 fail-fast 校验 READ ctx，把契约 §D3 错误暴露在 SDK 边界而非 Neo4j 服务端，节约排错时间。
- **`kg_neo4j_store.py:25-37` `_is_schema_rule_already_exists`**：`ensure_constraints` 只吞 schema-equivalent-rule-exists 异常，其他全部 raise。在多实例并发启动时不会误吞真错——**比"裸 try/except: pass"高一个量级的稳健性**。

---

## 6. Verdict

**APPROVE with suggestions**

**Rationale**：

下游 13 项契约 checklist 中 11 项完全合规，对系统命名空间、DELETE+CREATE、批量循环、GDS `.stream + MATCH + SET` 等关键约束的执行非常严格，注释里直接引用契约和 BUG ID 的密度很高。两个 Major 都属于"上游契约更新或 self-flagged 的已知 gap，不影响数据一致性、仅造成 switchover 窗口的 5xx UX 退化"，可作为下一个 sprint 的小改动落地。

不存在 Critical 风险——也就是说**不会出现 standby 数据分叉、不会出现 PEL 重放失败、不会出现 _elementId 污染**这些上游本次 baseline（BUG-080/081/082/083）所修复问题的回归。

---

## 7. Follow-ups

- [ ] **M1**: 把 `_ha_retry.py:79` 的 `_RETRYABLE_EXCEPTIONS` 升级为 `_is_retryable(exc)` 谓词，纳入 `ConstraintError` 中针对 `_elementId / uniq_elementid_` 的子串匹配（参考 §4 M1 给出的 diff）。
- [ ] **M2**: 选择 1 / 2 / 3 之一处置 `create_triple` & `add_triple_to_segment`：推荐方案 1（改 MERGE + 加 retry 装饰器），改动 < 10 行。
- [ ] **m1**（可选）：合并 `community_manager._run_leiden` 5 个 session 为单 session，提升 switchover 诊断性。
- [ ] **m2**（可选）：`_ha_retry.py` 增 `on_retry` 钩子供业务侧打 metric。
- [ ] **m3**（可选）：CI 加 `rg "execute_(write|read)" src/core/knowledge_graph/` 的 negative-test，把"禁止双层 retry"约束变成 build-time check。
- [ ] **m4**（可选）：评估 `merge_entities` 是否需要保留原 `triple_id`，与上游业务侧确认 `triple_id` 是否跨系统索引。
- [ ] **i3**（可选）：在 `db_client/__init__.py:120` 显式传入 driver tuning 参数（`max_transaction_retry_time` 等），防止 driver 升级时默认值漂移。
- [ ] **i4**: 反向更新 `ha-client-contract.md` §D3 / §6.1，补"双 URI bolt:// 物理分流方案下 `default_access_mode` 为 no-op"的说明，让下游接入者不被误导。

---

**Reviewer note**：本报告基于静态对齐审计，未触发实际 switchover / chaos / load 回归。Major / Minor 修复后建议跑一次 `bash scripts/deploy/ha-failover-chaos-test.sh` 同时在 oversea-chatbot 侧打开 KG 写入压力，验证 M1 的 `ConstraintError` 重试路径在真实 reconciler 撞车时是否生效。
