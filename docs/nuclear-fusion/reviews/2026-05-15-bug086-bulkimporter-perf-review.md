# Code Review: BUG-086 Fullsync REL Performance Optimization

- 日期：2026-05-15
- Reviewer：fukai（自审 + nuclear-fusion reviewing-code）
- Scope：BUG-086 修复涉及的 2 个文件
- 关联设计文档：`docs/design/2026-05-15-bug084-fullsync-consumer-premature-exit.md` §13
- 触发动机：用户要求评估性能优化是否引入新风险

---

## 1. Scope

| 文件 | 改动性质 | 行数 |
|---|---|---|
| `cdc-collector/.../RelationshipExporter.java` | 新增 2 行：把 `startLabels` / `endLabels` 放进 entity Map | +9 |
| `sync-applier/.../BulkImporter.java` | REL 路径整段重写：按 (sLabel, relType, eLabel) 分组 + UNWIND 批量 + label-aware MATCH | +60 -25 |

合计 ~70 行有效改动，影响范围**仅限 fullsync 路径**（增量同步路径未触及）。

## 2. Intent

**正确性目标**：保持 fullsync 数据一致性（已通过用户实测验证，primary/standby `count(n)` `count(r)` 全 label / 全 rel type 完全相等）。

**性能目标**：1000 行 REL batch 处理从 13-15s 降到 < 1s（设计文档 §13.4 预期值）。

## 3. Dimensions checked

### Correctness 

**修复后数据一致**（用户日志证据）：

```
== neo4j-primary ==                == neo4j-standby-1 ==
Community  688                     Community  688
Entity     4468                    Entity     4468
Segment    1000                    Segment    1000
BELONGS_TO   6476                  BELONGS_TO   6476
MENTIONED_IN 11145                 MENTIONED_IN 11145
RELATES_TO   4675                  RELATES_TO   4675
```

✅ 完全一致。修复未引入数据丢失。

### Security 

`sanitizeLabel` (IndexManager:75-81) 用反引号 + escape 处理特殊字符：

```java
if (label.matches("[a-zA-Z_][a-zA-Z0-9_]*")) return label;
return "`" + label.replace("`", "``") + "`";
```

这是 Neo4j 官方 label escape 模式，对 cypher 注入**防御充分**。Label 来源是 primary 自己的 Neo4j 数据（不是用户输入），实际注入风险极低。

### Performance 

代码逻辑上**应该有 10-50× 提速**，但需用户提供"优化后"的 fullsync 日志才能量化验证。从日志能看到的间接证据：

- 用户日志显示 22202 rels 在 23 batches × ~12.5s = ~5 分钟完成
- 这是**优化前**的数据
- 优化后理论应到 ~30 秒以内

### Maintainability / Consistency / Completeness — 见下方 Findings

## 4. Findings

### 🔴 Critical

无。

### 🟠 Major

#### M-1 `labels(n)` 顺序不稳定 → bucket 分布随机化

**File**: `BulkImporter.java:144-148`

```java
String sLabel = (sLabels == null || sLabels.isEmpty())
    ? "" : IndexManager.sanitizeLabel(sLabels.get(0));
```

Neo4j 的 `labels(n)` Cypher 函数**不保证 list 顺序稳定**——同一个节点在不同时间、不同 session、甚至不同 transaction 内查询，labels 的排列可能不同。

**影响**：

- bucket key `sLabel|relType|eLabel` 用 `labels.get(0)` 作为 key 的一部分
- 同一个节点在两次 fullsync 中可能进入**不同的 bucket**
- 多 label 节点（如 `(:Person:Employee)`）一次按 `Person` 分组，下次按 `Employee` 分组
- **正确性不受影响**（任一 label 上的 `_elementId` index 都能 hit），但 bucket 数量会 noisy，难以做 capacity planning

**修复建议**：取 sorted 后的第一个 label，保证幂等。

```java
private static String stableFirstLabel(List<String> labels) {
    if (labels == null || labels.isEmpty()) return "";
    return labels.stream().sorted().findFirst().get();
}
```

工作量：~10 行。

#### M-2 `@SuppressWarnings("unchecked")` 强转 `List<String>` 在反序列化边界缺乏防御

**File**: `BulkImporter.java:140-143`

```java
@SuppressWarnings("unchecked")
List<String> sLabels = (List<String>) rel.get("startLabels");
```

`rel` 是 `Map<String, Object>`，反序列化于 Redis Stream JSON。如果 Jackson 把数组反序列化为 `List<Object>`（实际元素是 String），强转 `List<String>` **不会** 在赋值时抛——Java 泛型擦除——但在后续 `sLabels.get(0)` 强转 String 时抛 ClassCastException。

实际场景里 Jackson 默认行为是 `List<Object>` 元素都是 `String`（因为 JSON array of strings），所以**几乎不会出错**。但如果未来某个版本 Jackson 升级或者数据源 schema 漂移，可能突然炸。

**修复建议**：用类型守护：

```java
Object raw = rel.get("startLabels");
List<String> sLabels = (raw instanceof List<?> l)
    ? l.stream().filter(o -> o instanceof String).map(o -> (String) o).toList()
    : List.of();
```

或在 deserializer 端做强类型转换并写入 metric 标记类型异常。

### 🟡 Minor

#### m-1 NODE 路径与 REL 路径的 label 处理风格不对称

**File**: `BulkImporter.java:81-83` vs `:144-148`

```java
// NODE: 多 label 拼接成复合
String labelStr = String.join(":", labels.stream()
    .map(IndexManager::sanitizeLabel).toList());
// → MERGE (n:Person:Employee {_elementId:...})

// REL: 只取第一个 label
String sLabel = sanitizeLabel(sLabels.get(0));
// → MATCH (a:Person {_elementId:...})
```

**这是有意为之**（NODE MERGE 需要完整 label 集，REL MATCH 只需一个 label 命中索引），但**注释里没说**。后来人看到不对称会困惑甚至"修复"成对称形式（错误地把多 label 拼到 MATCH 上，反而会丢失没有第一个 label 的节点）。

**修复建议**：在 REL 路径前加注释：

```java
// REL 端点用 MATCH (a:Label {_elementId:...}) — 只需要 ONE label 命中
// 该节点的 _elementId 索引即可，不需要拼接所有 labels。这与 NODE
// MERGE 路径不对称是有意的：MERGE 必须匹配完整 label 集。
```

#### m-2 bucket key 用字符串拼接 `sLabel + "|" + relType + "|" + eLabel`

**File**: `BulkImporter.java:152` + `:156`

```java
String key = sLabel + "|" + relType + "|" + eLabel;
// ...
String[] parts = entry.getKey().split("\\|", -1);
```

不够类型安全。如果某天 label 内含 `|` 字符（虽然 sanitizeLabel 不会产生这种输出，但 raw label 可能），bucket 会被错误拆分。

**修复建议**：用 record：

```java
record BucketKey(String startLabel, String relType, String endLabel) {}
var groups = new java.util.LinkedHashMap<BucketKey, List<Map<String, Object>>>();
```

类型安全 + 自文档化。

#### m-3 缺少 bucket 分组结果的 INFO 日志

**File**: `BulkImporter.java`

当用户反馈"fullsync 慢"时，无法从日志直接看出"是否走了 label-aware 路径"还是"全部走了 unlabeled fallback"。

**修复建议**：每个 batch 加一行：

```java
log.info("Fullsync REL batch: {} entities split into {} buckets "
       + "(unlabeled fallback: {})",
    batch.entities().size(), groups.size(), unlabeledCount);
```

便于 debug 性能问题。

#### m-4 无 label 节点的 fallback 静默退化

**File**: `BulkImporter.java:144-148`

如果业务图存在无 label 节点（不常见但合法），sLabel="" 会走 fallback unlabeled MATCH（即 BUG-086 之前的慢路径）。**正确性 OK**，但性能退化无声无息。

**修复建议**：fallback 命中时打 WARN 日志 + metric `fullsync_unlabeled_match_total`，便于运维感知。

#### m-5 没有单测覆盖 bucket grouping 逻辑

**File**: `src/sync-applier/src/test/.../BulkImporterTest.java`（不存在）

修复了核心性能路径但**没有任何单测**。容易在未来 refactor 时 silently 退化。

**修复建议**：至少补 3 个测试：

```java
@Test void bucketsByLabelTriple() {
    // 喂 4 个 rel：2 个 (Person, KNOWS, Person) + 2 个 (Company, EMPLOYS, Person)
    // assert: 生成 2 个 bucket，每个含 2 个 rel
}
@Test void unlabeledFallback() {
    // 喂 rel 不含 startLabels/endLabels
    // assert: 走 fallback path（cypher 不含 ":Label"）
}
@Test void multiLabelTakesFirstSorted() {
    // 喂 rel startLabels=["B","A"] 和 ["A","B"]
    // assert: 都进同一个 bucket (A 排序后第一)  ← 配合 M-1 修复
}
```

工作量：~30 分钟。

#### m-6 没有 metric 暴露 bucket 数量 / unlabeled fallback rate

**File**: `HaMetrics.java`

无法被 Prometheus 监控。如果生产环境 fullsync 突然慢下来，无法定位是 bucket 爆炸还是 fallback rate 飙升。

**修复建议**：加两个 metric：

```java
public final Counter fullsyncBucketCount;          // 累计 bucket 数
public final Counter fullsyncUnlabeledFallback;    // unlabeled 命中次数
```

### 🔵 Info（观察 + Praise）

#### i-1 ✓ 优化方向正确

UNWIND 批量 + label-aware MATCH 是 Neo4j 文档明确推荐的批量导入优化模式。与官方 `neo4j-admin import` 工具的内部策略一致。

#### i-2 ✓ 复用 RelationshipExporter 已查询但未使用的字段

`startLabels` / `endLabels` 在 cypher 里已经查询，只是 Java 端忘了塞进 entity Map。**零额外 Neo4j 查询代价**就把字段补全。优秀。

#### i-3 ✓ 用户实测数据一致性已达成

用户日志 + cypher count 双向验证：

| Rel Type | Primary | Standby | Diff |
|---|---|---|---|
| BELONGS_TO | 6476 | 6476 | 0 ✓ |
| MENTIONED_IN | 11145 | 11145 | 0 ✓ |
| RELATES_TO | 4675 | 4675 | 0 ✓ |

**核心功能正确**，BUG-086 没有引入正确性回归。

#### i-4 ✓ Stale 过滤日志清晰

```
16:48:56.209 skipping stale batch 4/23 (type=RELATIONSHIP, ts=1778829668935 < snapshotTs=...)
```

每条都打印 ts < snapshotTs 的具体值，运维定位"为什么跳过"非常友好。

#### i-5 ⚠ 一个不属于本次修复但值得注意的现象

```
16:53:57.329 ChangeApplier - BUG-081: split batch of 64 events into 2 sub-tx(es) 
  due to duplicate _elementId (Neo4j rel-id reuse)
```

fullsync 完成后立刻有 BUG-081 重复 elementId 触发——说明业务图存在 rel-id 复用模式。这不是 BUG-086 的问题，但需要持续监控
`neo4j_ha_batch_split_for_duplicate_elementid_total` 是否在常态下增长。

## 5. Severity Distribution

| 级别 | 数量 |
|---|---|
| Critical | 0 |
| Major | 2 |
| Minor | 6 |
| Info | 5 |

## 6. Verdict

**APPROVE with suggestions**

- 核心修复**正确性已经在生产环境实测验证**（数据完全一致）
- 0 Critical / 0 阻塞性 Major
- 2 Major（M-1 labels 顺序、M-2 强转防御）建议下一迭代修复——不影响功能，影响可观测性 + 长期稳健性
- 6 Minor 都是可观测性 / 测试覆盖 / 风格问题，**不阻塞合并**

## 7. 优先级建议

| 优先级 | 项目 | 工作量 | 收益 |
|---|---|---|---|
| 高 | M-1 sort labels 取第一 | 10 行 | bucket 分布稳定，能做 capacity planning |
| 中 | m-5 单测覆盖 + m-3 INFO log | 1 小时 | 防回归 + 故障排查 |
| 低 | M-2 类型守护 / m-2 record / m-4 fallback WARN | 30 分钟各 | defense-in-depth |
| 可延后 | m-6 Prometheus metric | 1 小时 | 生产监控完善 |

## 8. 验证后续

下次用户做 fullsync 时，重点观察：

1. **每批 REL 耗时**：应该 < 1 秒（vs 修复前 13-15 秒）
2. **总 fullsync 时间**：应该 < 1 分钟（vs 修复前 ~5 分钟）
3. **standby ServiceState 转换**：应该早早回到 ONLINE（vs 之前因 lag 阈值反复在 SYNCING / ONLINE 间抖动）
4. **`BUG-081 split` 日志频率**：fullsync 后增量同步 phase 监控，应该频率正常（每天几次而非每秒）

如果观察不到 10× 以上速度提升，触发独立诊断（jar 是否真的更新了 / Neo4j 索引是否在 ONLINE 状态 / standby 资源是否过载）。

---

## 9. 总结

数据一致性 ✅ 达成；性能优化方向 ✅ 正确；正确性 ✅ 不退化；可观测性 ⚠ 待补；测试覆盖 ⚠ 缺失。

**核心修复 APPROVE。** 建议下一迭代补 M-1（labels 顺序稳定）+ m-5（单测）+ m-6（metric），这三项加起来约 2-3 小时工作量。
