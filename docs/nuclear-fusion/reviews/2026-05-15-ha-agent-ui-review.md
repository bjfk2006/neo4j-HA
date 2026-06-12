# Code Review: HA Agent Web UI v1.2

- 日期：2026-05-15
- Reviewer：fukai（自审 + nuclear-fusion reviewing-code）
- Scope：UI 启用（§10）+ 布局重设计 + 数据一致性 v1.2（§13）
- 设计文档：`docs/design/2026-05-14-ha-agent-ui-solution.md`

---

## 1. Scope

| 类别 | 文件数 | 新增 LOC | 修改 LOC |
|---|---|---|---|
| 后端 Java（新增） | 8 | ~1400 | — |
| 后端 Java（修改） | 4 | — | ~260 |
| 前端 Vue / JS（新增） | 9 | ~1100 | — |
| 前端 Vue / JS（修改） | 5 | — | ~250 |
| 配置 / 文档 | 6 | ~700 | — |

**变更覆盖**：

- 新增 `agent/http/auth/**`（鉴权全套）
- 新增 `agent/http/{Auth,Audit,MetricsSummary,DataStats,DataDiff}Controller`
- 新增 `agent/consistency/{EntityCounter,DiffEngine,PropertyHasher}`
- 改造 `AdminHttpServer`（路由重组 + 静态资源 + SPA fallback）
- 改造 `HaAgent.main`（装配 9 个新组件）
- 改造 `HaConfig` `ConfigValidator` `HaMetrics`（新增 `admin.ui` schema + 指标）
- 前端 Vue 3 SPA 全新建（13 个文件）
- 设计文档 §10（UI 增量）+ §13（数据一致性）

## 2. Intent（已验证）

参考设计文档 §13.1。三个目标：

1. ✅ 提供 Web UI 替代 curl 操作 — 实现完整
2. ✅ 布局参考截图重做（三栏 + 暗色侧栏）— 实现完整
3. ✅ 数据一致性 Phase 1+2（数量统计 + 差异明细，只读）— 实现完整，Phase 3 显式留空

---

## 3. Findings

### 🔴 Critical

无。

### 🟠 Major

#### M-1 `DiffEngine` 主键扫描路径在大图上会触发全表扫描

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/consistency/DiffEngine.java:217-228`

```cypher
MATCH (n) WHERE n._elementId IS NOT NULL
RETURN n._elementId AS eid, labels(n) AS ls, properties(n) AS p
ORDER BY coalesce(n._updated_at, 0) DESC LIMIT $limit
```

**问题**：项目里 `_updated_at` / `_elementId` 索引是 **per-label** 创建的（`IndexInstaller.java:37-38, 90-91`：`CREATE RANGE INDEX ... FOR (n:%s) ON (n._updated_at)`）。本 query **没有 label 限定**，Neo4j 优化器无法选中任何 per-label 索引，会**全节点表扫描**。在百万节点级图上，单次 RECENT 扫描可能耗时数十秒至分钟，触发上层 `DataDiffController` 的 15s `future.get` 超时（虽然有 catch 但用户体验差），且占用 primary 大量 IO。

**同样的问题**还出现在三处：

- `DiffEngine.java:227` 关系扫描 `MATCH ()-[r]->() WHERE r._elementId IS NOT NULL ...`
- `DiffEngine.java:165-170` standby "extra" 节点扫描（无 label + `NOT IN $known` 双重无索引谓词，更慢）
- `DiffEngine.java:178-184` standby "extra" 关系扫描（同上）

**修复建议**（两选一）：

- **A（推荐）**：限制 `RECENT` scope 的 `limit` 上限 ≤ 200，并在 UI 上提示"大图请用 `scope=label` 限定标签"。同时把 `RANDOM` scope 也按 label 分批跑（用户能写多 label 就分别跑）。
- **B（重一些）**：在 IndexInstaller 里再加一个 **全局** `RANGE INDEX FOR (n) ON (n._updated_at)`（无 label）。需要 Neo4j 5.x 支持 token-lookup 索引，需验证。

无论选哪个，先在 `DataDiffController` 加预检：`primary nodeCount > 100_000` 时强制要求 `scope != recent` 或 `limit ≤ 200`。

---

#### M-2 `recordToMap` 对 Neo4j 空间 / 时长类型可能直接抛 `InvalidDefinitionException`

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/consistency/DiffEngine.java:313-316`

```java
private static Map<String, Object> recordToMap(Value v) {
    if (v == null || v.isNull()) return Map.of();
    return v.asMap();   // ← 返回的 Map 里可能含 Neo4j 自定义类型
}
```

下游 `PropertyHasher.java:53` 用 Jackson 序列化这个 map：

```java
String json = MAPPER.writeValueAsString(canonical);
```

Jackson **默认不带** `JavaTimeModule` 和 Neo4j 空间类型 serializer。属性中只要含有 `point({x:1, y:2})`、`duration({days:1})` 等类型，`writeValueAsString` 会抛 `JsonMappingException`。当前虽然外层有 try-catch 兜底成 `toString()`，但 fallback 走 Java 默认 toString，会得到 `org.neo4j.driver.internal.value.PointValue@a3f5...`——**对象内存地址**，每个 JVM 进程不同，hash 不稳定 → 主备永远 hash 不等 → 误报"属性异"。

**影响**：图里若有 spatial / temporal 属性，所有这类节点都会进 `propDiff` 列表（false positive）。即便没有这类类型，未来一旦引入就立刻爆。

**修复建议**：

1. `PropertyHasher` 注册 `JavaTimeModule`：
   ```java
   MAPPER.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
   ```
   父 pom 已有 `jackson-databind 2.17.0`，再加 `jackson-datatype-jsr310` 即可。
2. 在 `DiffEngine.recordToMap` 里把 Neo4j 自定义类型转成 canonical 字符串：
   ```java
   Map<String, Object> out = new HashMap<>();
   for (var e : v.asMap().entrySet()) {
       Object val = e.getValue();
       if (val instanceof org.neo4j.driver.types.Point p) val = p.toString();
       else if (val instanceof java.time.temporal.TemporalAccessor t) val = t.toString();
       out.put(e.getKey(), val);
   }
   return out;
   ```
3. 加个单测覆盖（用 `MockSession` 喂入 PointValue）。

---

#### M-3 SPA fallback API `config.spaRoot.addFile(...)` 在 Javalin 6.1.3 不存在

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/http/AdminHttpServer.java:108`

```java
config.spaRoot.addFile("/", "/static/index.html", Location.CLASSPATH);
```

**问题**：Javalin 6.x 的 SPA fallback API 是 `JavalinConfig.spaRoot.addFile(String hostedPath, String filePath, Location)`，**但 `spaRoot` 字段是 6.4+ 才稳定**。父 pom 的 `javalin.version` 是 `6.1.3`（见 `pom.xml:35`）。6.1.3 实际暴露的是 `staticFiles.add(staticFiles -> { staticFiles.precompress = ...; })` 且**没有** `spaRoot` 字段（5.x 用 `enableWebjars()`，6.0~6.3 用 `staticFiles.precompress(true)` + 自定义 404 handler）。

如果编译失败的话用户会立刻知道；但如果通过了（spaRoot 是新版引入但 6.1.3 已有 stub？需要验证），运行时也可能行为不一致。

**修复建议**：

- **首选**：把 javalin 版本升到 `6.4.0+`（一行 pom 改动），然后这段代码就是正确的。检查 `pom.xml:35` 修改后重新打包。
- **备用**（不升 javalin 时）：手写 SPA fallback：
  ```java
  app.error(404, ctx -> {
      String path = ctx.path();
      if (path.startsWith("/api/") || path.startsWith("/cluster/")
              || path.equals("/health") || path.equals("/metrics")) return;
      // SPA route — serve index.html and let Vue Router resolve client-side
      try (var in = AdminHttpServer.class.getResourceAsStream("/static/index.html")) {
          if (in != null) ctx.contentType("text/html").result(in.readAllBytes());
      }
  });
  ```

我个人推荐升 javalin（最干净，且 6.4+ 已发布 1 年+，稳定）。

---

#### M-4 `EntityCounter.runWithTimeout` 每次调用创建并销毁一个 ExecutorService

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/consistency/EntityCounter.java:98-117`

```java
private <T> T runWithTimeout(Callable<T> task) throws Exception {
    var ex = Executors.newSingleThreadExecutor(...);  // ← 每次 new
    try {
        Future<T> f = ex.submit(task);
        return f.get(timeoutMs, TimeUnit.MILLISECONDS);
    } finally {
        ex.shutdownNow();
    }
}
```

每次 `count()` 调用会触发 3 次 `runWithTimeout`（nodeCount / relCount / byLabel），每次都 `new ExecutorService` + `shutdownNow`。3 个节点 × 3 次 = **9 个线程被反复创建销毁** 每次 `/api/cluster/data-stats` 请求。前端默认 2 秒不轮询这个端点（只在用户点"刷新"或进 Consistency 页时调），所以**实际影响小**，但属于明确的资源浪费。

更严重的隐含问题：`shutdownNow()` 不会等待 task 真正结束。如果 Bolt 查询已经发出去但还没返回，`shutdownNow` 不会中断 Bolt 网络读，task 还会在后台跑直到 Bolt 自己 timeout——意味着 timeout 后**资源没立即释放**。

**修复建议**：

```java
public class EntityCounter {
    // Shared single executor reused across calls.
    private static final ExecutorService EX = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "entity-counter");
        t.setDaemon(true);
        return t;
    });
    // ...
    private <T> T runWithTimeout(Callable<T> task) throws Exception {
        Future<T> f = EX.submit(task);
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            f.cancel(true);  // best-effort interrupt
            throw te;
        }
    }
}
```

或者用 Neo4j Driver 自己的 `SessionConfig.builder().withDefaultAccessMode(...).build()` + transaction timeout。

---

### 🟡 Minor

#### m-1 `AuditController` 的 Jedis API 使用方式版本敏感

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/http/AuditController.java:91-103`

之前已在 §10.7 `java-build-packaging-guide.md` caveat 提示过（`xrange/xrevrange(key, start, end, count)` 位置参数版本）。Jedis 5.1.0 应支持，但版本敏感。**修复路径已经在 caveat 表里**，不再重复。建议升级 jedis 到 5.2.0+ 时改用 `XRangeParams.xRangeParams(...).count(N)` 链式 API。

#### m-2 `DataDiffController` 的 60s cooldown 只对 `nodeId` 指定时生效

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/http/DataDiffController.java:62-79`

```java
if (nodeId != null && !nodeId.isBlank()) {
    AtomicLong last = lastScanByNode.computeIfAbsent(nodeId, k -> new AtomicLong(0));
    ...
}
```

不指定 `nodeId`（默认对所有 standby 扫描，更重）的请求**反而绕过 cooldown**。攻击/误操作可以连续发请求，每次都执行完整对所有 standby 的扫描。

**修复建议**：cooldown 改为 per-(primary, standby) 二元组，全 fan-out 时检查每个 standby 的 cooldown，任何一个在 cooldown 都拒绝整个请求。或简单点：再加一个全局 `lastFullScan` 的 30s cooldown。

#### m-3 `DataDiffController` 没有写审计日志

**File**: `src/ha-agent/src/main/java/com/neo4j/ha/agent/http/DataDiffController.java:全文`

虽然 `data-diff` 是只读端点，但**它是高成本操作**（可能触发全表扫），且**会暴露具体的属性内容**。设计文档 §10.4 审计表里 `op.*` 类型显式声明了"操作落 ui-audit"。一致性扫描应该归类为 `op.consistency-scan` 并入审计：

```java
// Inside DataDiffController.getDiff(), before doing work:
Principal p = AuthFilter.principal(ctx);
uiAuditLog.logOperation("consistency-scan",
    p.displayActor(), ctx.ip(),
    String.format("scope=%s,limit=%d,type=%s,nodeId=%s", scope, limit, kind, nodeId),
    UUID.randomUUID().toString());
```

`UiAuditLog` 已注入 `AdminHttpServer`，但 `DataDiffController` 构造器没拿到——需要补一个参数。

#### m-4 前端 fetch 没有 timeout

**File**: `ui/src/api/http.js:9-31`

```js
const res = await fetch(finalUrl, { credentials: 'same-origin', ...opts, headers })
```

无 `AbortController`、无 timeout。在 M-1 的全表扫描 + 浏览器请求叠加场景下，浏览器可能挂 30s+ 看不到任何反馈。

**修复建议**：

```js
const controller = new AbortController()
const timer = setTimeout(() => controller.abort(), opts.timeoutMs || 30_000)
try {
    const res = await fetch(finalUrl, { signal: controller.signal, ... })
    // ...
} finally { clearTimeout(timer) }
```

#### m-5 `ConsistencyDiffTable.vue` 详情面板字段命名风格不一致

**File**: `ui/src/components/ConsistencyDiffTable.vue:73`

```js
detail.value = { ...entry, _category: kindLabel }
```

下划线开头的 `_category` 字段混入了对象其他属性（驼峰命名如 `elementId` / `primaryProps`）。前端没有强制命名风格但风格不一致**是 review reviewer 的 warning 信号**。

**修复建议**：改名 `category`，或者把 category 和 entry 分开：

```js
detail.value = { entry, category: kindLabel }
// 模板里 detail.entry.elementId / detail.category
```

#### m-6 `StatusOverview.vue` 每 2 秒拉 audit 流，对 Redis 是持续小压力

**File**: `ui/src/components/StatusOverview.vue:16, POLL_MS=2000`

```js
const [c, s, a] = await Promise.all([
    api.clusterStatus(), api.metricsSummary(), api.audit(null, 5)
])
```

`api.audit(null, 5)` 内部走 Redis `XREVRANGE`，每个 UI 客户端每 2 秒一次。多个浏览器同时打开会放大。**不是 bug**，但生产部署有 N 个 NOC 屏幕时会出现。

**修复建议**：audit 拉取改为 30 秒（事件本来就不是高频）：

```js
const [c, s] = await Promise.all([ api.clusterStatus(), api.metricsSummary() ])
// audit 独立轮询：
if (now - lastAuditFetch > 30_000) { events.value = (await api.audit(null, 5)).entries }
```

#### m-7 新增的 Java 代码没有单元测试

**File**: `src/ha-agent/src/test/java/com/neo4j/ha/agent/consistency/`（不存在）

`EntityCounter` / `DiffEngine` / `PropertyHasher` 三个文件没有对应单测。`DiffEngine.computeDelta` / `PropertyHasher.hash` 都是纯函数，**很容易测**。

**修复建议**：至少补三个：

```java
@Test void hash_excludesUpdatedAt() { ... }
@Test void hash_orderIndependent() { ... }
@Test void computeDelta_findsAddedRemovedChanged() { ... }
```

工作量约 1 小时。

#### m-8 设计文档 §13.9 列了 Prometheus 指标但未实际接入

**File**: 设计文档 `2026-05-14-ha-agent-ui-solution.md:894-905` 提到 4 个 `ha_consistency_*` 指标，但 `HaMetrics.java` 里没有对应字段，`DataStatsController` / `DataDiffController` 里也没调 `metrics.recordXxx()`。

**修复建议**：要么后端补上（约 30 分钟），要么把设计文档对应章节标"v1.3 候选"。设计稿与实现不一致是技术债来源。

---

### 🔵 Info（观察 + 建议）

#### i-1 SPA fallback 的修复思路精确

`AuthFilter` 重写为"白名单 API"模式（只拦 `/api/*` 和 `/cluster/*`）是正确的 SPA 后端思路；非 API 路径全部放给 staticFiles 处理。这一处的 commit 注释（`AuthFilter.java:30-39`）清楚说明了为什么。

#### i-2 鉴权双轨设计的"Filter 优先级 + Principal 抽象"很清晰

`Principal` 同时承载 `AuthKind.{SESSION,TOKEN}` 和 `Role`，让 RBAC 检查只看 `Principal`，无需关心来自 Cookie 还是 Header。这层抽象到位。

#### i-3 `PropertyHasher` 排除 HA 内部字段的设计正确

`_updated_at` 在标准 sync 路径上会被 APOC trigger 自动 stamp，主备值经常不同（事务时间不同）；如果参与 hash 就**永远 hash 不等**。`PropertyHasher.java:35-36` 显式排除 `_updated_at` 等是正确的，注释说明了 why。

#### i-4 设计文档 §13.6 "风险边界决策表"非常实用

提前明确"差异多少条该用哪种修复手段"的决策规则。这种"知道何时不要用这个 feature"的文档比 feature 本身更值钱。

#### i-5 数据一致性视图明确"v1.2 不修复"+ 详情对话框引导走 Full Sync — 设计纪律好

`ConsistencyDiffTable.vue:286-289` 详情对话框底部的 `el-alert` 显式提示 v1.2 不修复，引导用户走 Full Sync。避免了用户基于"看到差异就想动手"的本能造成的不可逆错误。

---

## 4. Severity Distribution

| 级别 | 数量 |
|---|---|
| Critical | 0 |
| Major | 4 |
| Minor | 8 |
| Info | 5 |

## 5. Verdict

**REQUEST CHANGES**（4 个 Major 需先处理）

- **M-1 与 M-3 必须修复后才能合并**：M-1 在大图上直接出可用性问题；M-3 影响整个 UI 是否可访问（编译可能直接失败）。
- **M-2 在生产图含 spatial/temporal 属性时必修**；如果业务图确认不含这两类，可降级为 Minor。
- **M-4 是效率问题不阻塞合并**，但建议同期处理（共用线程池 + 真正取消任务）。
- **Minor 中 m-1 / m-3 / m-4 / m-7 建议同窗口处理**（合计 ≤ 4 小时）。
- **m-5 / m-6 / m-8 可放下一迭代**。

## 6. 后续动作建议

按优先级排：

1. **立即**：M-3（javalin 升级到 6.4.0+）—— 验证 SPA fallback 真的能工作；如果编译报错就修。一行 pom 改动。
2. **立即**：M-1（DataDiffController 加预检 + UI 限制大图 limit）—— 防大图把 primary 拖慢。
3. **本迭代内**：M-2（Jackson JavaTimeModule + Neo4j Point/Duration 序列化）+ 单测覆盖。
4. **本迭代内**：M-4（共用 ExecutorService）+ m-3（DataDiffController 写审计）+ m-7（三个工具类单测）。
5. **下一迭代**：m-2 / m-5 / m-6 / m-8 + Phase 3 评估。

---

## 7. 总体评价

数据一致性 v1.2 的**架构判断和功能边界**做得很好：
- 明确分三个 Phase，每个 Phase 都是可发布单元
- 显式拒绝在 v1.2 做修复（避免不可逆错误）
- 详情对话框引导用户走 Full Sync（既有功能）
- 配套设计文档完整、决策可追溯

主要问题集中在**实现细节**而非设计：
- 大图查询路径没用上索引（M-1）
- 第三方库 API 兼容性（M-2 Jackson、M-3 Javalin）
- 资源管理（M-4）

这些都是可以在 1-2 个工作日内修完的 Major，不会推翻设计。Phase 3 单点修复的"先观察 1-2 周再做"决定是务实的。
