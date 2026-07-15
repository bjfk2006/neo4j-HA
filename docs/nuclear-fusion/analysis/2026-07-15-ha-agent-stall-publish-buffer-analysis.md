# HA Agent「卡死」与消息同步异常 — 根因分析（BUG-088 候选）

> 日期: 2026-07-15
> 环境: 10.55.77.3 `/data2/neo4j-cluster`（deploy-test.yml，node-01 primary + node-02 standby，agent 端口 18888/19999）
> 镜像: `neo4j-ha-agent:1.1.0-SNAPSHOT`
> 状态: 已定位根因，未改代码（分析交付）

---

## 1. 现象

- ha-agent 日志完全静默，CDC 停止发布，看似进程卡死。当天两次复现：
  - 第一段运行：20:36:05 后静默（仅 20:36:42 一条 StreamMaintenance），20:38:58 被人工重启；
  - 第二段运行：20:39:19 后静默 23+ 分钟（诊断时仍在持续）。
- `GET /health` 正常返回 `{"status":"UP"}`，容器无重启（RestartCount=0，无 OOMKilled）。
- 备节点数据漂移（诊断时实测）：**standby 比 primary 少 26 个节点、1797 条关系**（primary 17017/65009，standby 16991/63212）。
- sync-applier 曾报大量乱序：`OrderValidator -- Out-of-order events in batch ... +21800 also suppressed`（旧 ts=1782188xxx≈6月23日 排在新 ts=1784118xxx≈7月15日 之后）。

## 2. 排除项（证据）

| 假设 | 证据 | 结论 |
|---|---|---|
| JVM 死锁 | `kill -3` 线程转储无 `Found deadlock`，无 BLOCKED 业务线程 | ❌ |
| OOM / GC | Heap 192MB/512MB，Metaspace 正常 | ❌ |
| cdc-collector 线程阻塞在 IO | 线程状态 TIMED_WAITING parked 在 `DelayedWorkQueue.take`（等下一次调度） | ❌ |
| poll 任务被未捕获异常杀死 | 两次转储间隔 113s，线程 CPU 从 6086ms 增至 6484ms（~3.5ms/s），且 Redis `neo4j:ha:cdc-checkpoint:node-01` 心跳仍在更新 | ❌ **任务活着，每 100ms 空转** |
| Redis / Neo4j 不可达 | XLEN=57002 正常读写；cypher-shell 正常 | ❌ |

**结论：不是进程级卡死，而是 CDC 管道功能性停摆 + 本地缓冲永不消化，外观上表现为"卡死"。**

## 3. 根因链

### 3.1 雷是 6 月 23 日埋下的：发布失败时缓冲重复膨胀

`/data2/neo4j-cluster/ha-agent/buffer/` 现存 **53 个 jsonl、共 482MB**，文件时间戳集中在 2026-06-23 12:15:53 ~ 12:18:59（约 3 分钟）。

机制（代码路径）：

1. Redis 发布失败 → `StreamPublishService.publishBatch` catch 后 `publishBuffer.add(events)`（`StreamPublishService.java:67-71`）；
2. `CdcCollector.pollLoop` 的 C1 保护：发布失败**游标不推进**（`CdcCollector.java:349-353`）；
3. 100ms 后下一轮 poll **重读同一批变更** → 再次失败 → **同一批事件再次 append 进缓冲**；
4. 故障持续 3 分钟 ≈ 同一批数据被重复缓冲上百次 → 482MB。

且 `ChangeEventBuilder` 每次 build 都重新生成 eventId（`ChangeEventBuilder.java:68`，`IdGenerator.uuidV7()`），这些重复副本对 `DuplicateDetector` 而言是"全新事件"，**去重失效**。

> C1 语义下游标不推进、数据必然被重读，此时再往 PublishBuffer 写入本身就是多余的——buffer 的合理场景只剩 fencing-reject（BUG-047 路径）。

### 3.2 「卡死」的直接原因：缓冲回放被"有新变更"门控

- `retryBuffered()`（每次只 drain 500 条）**唯一调用点**在 `publishBatch()` 内（`StreamPublishService.java:55`）；
- `pollLoop` 轮询结果为空时走 BUG-072 心跳分支**直接 return**（`CdcCollector.java:324-334`），不会触及 publishBatch；
- 因此：一旦轮询追平、集群无新写入 → **53 个文件 / 482MB 永远不会被 flush**，日志无输出、缓冲无进度 → 用户看到的"卡死"。
- `bufferForRetry` 的 javadoc 声称 "start() 会 on every tick 调 retryBuffered()"（`StreamPublishService.java:40-41`）——**该逻辑并不存在**，注释与实现脱节。

两次"卡死"时间点与此完全吻合：每次重启后，旧 checkpoint 落后 → 轮询有增量 → 每轮顺带 flush 500 条旧缓冲（日志密集）；轮询追平的瞬间（第一段 20:36:05、第二段 20:39:08）→ 空批 return → 一切静默。

### 3.3 「消息同步异常」的直接原因：陈旧事件晚于新事件重放

每当有新变更触发 publishBatch，会**先** flush 500 条 6 月 23 日的旧事件到 stream 尾部：

- 备节点在 7 月的新值之后应用 6 月 23 日的旧值，`SET n = $properties` 全量覆盖 → **新数据被旧快照回滚**；
- `OrderValidator` 单段运行即报 21800+ 乱序事件（仅告警不拦截，按设计 passthrough）；
- 与实测漂移吻合（standby -26 节点 / -1797 关系）。诊断时主库 `_CDCDeleteEvent` 残留为 0，排除删除中转路径的现行贡献。

### 3.4 放大因素：观测性双盲

- `monitoring.prometheus.port: 9090` 配置存在，但容器内 9090 **无监听**（宿主 19999 映射连接被拒），metrics 实际由 Admin server 在 8080/18888 `/metrics` 提供 → 按文档配 Prometheus 抓 19999 的话完全失明；
- `neo4j_ha_buffer_size` 实测为 **-500（负数）**：add/drain 计数逻辑有 bug，482MB 积压在指标上完全不可见，也无对应告警。

### 3.5 次要贡献：启动 sweep 阻塞主线程 67 秒

第一段运行 `[main]` 在 20:34:35 → 20:35:42 之间被 BUG-067 startup sweep 阻塞（单批清理 10000 个 `_CDCDeleteEvent` 花 67s），启动期同样呈现"卡死"观感。残留量大时该步骤会显著拉长启动时间。

## 4. 时间线（7 月 15 日）

| 时刻 | 事件 |
|---|---|
| 20:34:27 | 第一段启动，恢复 6/19 老 checkpoint |
| 20:34:35→20:35:42 | startup sweep 清 10000 transit 节点，main 阻塞 67s |
| 20:35:44→20:36:05 | 每轮 poll 顺带回放旧缓冲（~1.5 文件/秒），sync-applier 开始报乱序 |
| 20:36:05 | 轮询追平 → 空批 → **静默（第一次"卡死"）** |
| 20:38:58 | 人工重启（SIGTERM，优雅停机正常完成） |
| 20:39:08 | 第二段追平前 flush 了 1 个缓冲文件 |
| 20:39:19 | node-02 ONLINE → 此后**再次静默（第二次"卡死"，诊断期间持续 23+ 分钟）** |

## 5. 修复方向建议（未实施，hand-off）

1. **紧急止血（运维）**：停 agent → 将 `buffer/` 整目录移出归档（内容是重复的陈旧事件，重放只会继续污染 standby）→ 启动 agent → `POST /cluster/fullsync?nodeId=node-02` 重建备节点，消除既有漂移。
2. **代码修复（建议立项 BUG-088，`quick-coding`/`building-production-feature`）**：
   - a. 空批分支同样调用 `retryBuffered()`（或独立定时器 flush buffer），解除"新变更"门控；
   - b. C1 语义下发布失败**不再重复入缓冲**（游标未动、数据必然重读；PublishBuffer 收窄为 fencing-reject 专用），杜绝重复膨胀；
   - c. 回放侧加陈旧防护：丢弃 `timestamp` 早于当前已发布位点安全窗口的缓冲事件（或 applier 按 `_updated_at` 拒绝回退写）；
   - d. 修复 `neo4j_ha_buffer_size` 负数 bug，补"缓冲文件数/字节数"指标与积压告警；
   - e. Prometheus 9090 独立端口未生效：实现或改文档/compose（现 metrics 在 admin 8080）；
   - f. `bufferForRetry` javadoc 与实现对齐。
3. **回归验证**：模拟 Redis 断连 3 分钟 → 恢复 → 确认 buffer 不膨胀、恢复后无乱序回放、standby 与 primary 逐字段一致。

## 6. 证据存档

- 线程转储：10.55.77.3 `/tmp/ha-td.txt`（两次采样 CPU 对比见 §2）
- 缓冲目录：`/data2/neo4j-cluster/ha-agent/buffer/`（53 文件 / 482MB，勿直接删除，建议归档）
- 关键日志锚点：`docker logs ha-agent` 内 20:35:42（sweep 67s）、20:36:05/20:39:08（最后一次 flush）、OrderValidator 乱序告警
