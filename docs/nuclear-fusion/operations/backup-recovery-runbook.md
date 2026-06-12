# Neo4j HA 备份与恢复操作手册

> 版本：v2（2026-05-15）
> 适用范围：当前仓库的集中式 `ha-agent` 架构（**Community / Enterprise 通用**）
> 相关脚本：`scripts/backup/backup-standby.sh`、`scripts/backup/restore-standby.sh`
> 关联设计文档：`docs/design/2026-05-15-bug084-fullsync-consumer-premature-exit.md` 介绍 fullsync 追赶机制（备份恢复后的兜底路径）

---

## 1. 目标与原则

- **备节点离线备份**：所有备份操作针对 standby 节点，避免影响 primary 写入流量
- **数据一致性优先**：备份期间 standby 容器**完全停机**，确保拷贝得到事务一致的数据快照
- **HA Agent 统一协调**：四个内部子系统（SyncApplier / HealthChecker / HaProxyStateSyncer / HAProxy 路由）由 `BackupCoordinator` 一并暂停 / 恢复，运维脚本只需调用 `prepare` / `complete` 两个 API
- **Community 兼容**：不依赖 `neo4j-admin database backup`（Enterprise Only），改用文件系统层 cp/tar
- **恢复后自动追赶**：standby 容器启动后由 HA Agent 基于 prepare 时记录的 checkpoint 自动判断增量追赶或全量同步

---

## 2. 方案选型与权衡（v2 新增）

| 方案 | 数据一致性 | Community 可用 | standby 停机时长 | 复杂度 | 当前选择 |
|---|---|---|---|---|---|
| A. `neo4j-admin database backup` 在线热备 | ✅ | ❌ Enterprise Only | 不停机 | 低 | — |
| B. 暂停 SyncApplier + 直接 cp 运行中目录 | ❌ **数据可能损坏**<br>(page cache / WAL torn / store.lock) | ✅ | 不停机 | 低 | — |
| **C. 协调暂停 + docker stop + cp + docker start** | ✅ | ✅ | 5–15 分钟（100GB） | 中 | ✅ **本手册采用** |
| D. LVM/btrfs/ZFS 文件系统快照 | ✅ | ✅ | < 5 秒 | 高（需文件系统支持） | 进阶可选，未实施 |

**为什么是 C**：在 Community 部署下，方案 C 是**唯一既保证数据一致性又工程化简单**的选择。standby 停机 5–15 分钟在多 standby 集群里完全可接受（其他 standby + primary 顶住读 + 写流量）；如果只有 1 个 standby 且业务对读副本敏感，建议加 standby 或评估方案 D。

---

## 3. 备份原理与时序

### 3.1 端到端时序图

```
 备份脚本                  HA Agent                        Standby 容器           HAProxy
─────────                ────────────                    ─────────────         ────────
    │                         │                                │                   │
    │── POST /backup/prepare ─→│                                │                   │
    │                          │── pause SyncApplier(node-02) ──→│                  │
    │                          │── suppress HealthChecker ──────→│                  │
    │                          │── pause HaProxyStateSyncer ────→│                  │
    │                          │── set state=maint  ─────────────────────────────→│
    │←── 200 OK ───────────────│                                │                   │
    │                          │                                │                   │
    │── docker exec checkpoint ──────────────────────────────→ flush dirty pages    │
    │── docker stop ──────────────────────────────────────────→ graceful shutdown   │
    │                          │                                │ (no longer holds  │
    │                          │                                │  store.lock)      │
    │                                                            │
    │── cp -a /data → /backup ──┘  ← 此时 data 目录稳定，可安全拷贝
    │── tar/pigz + sha256sum
    │                                                            │
    │── docker start ────────────────────────────────────────→ Neo4j 启动           │
    │── wait for bolt port  ←─────────────────────────────────── 就绪                │
    │                          │                                │                   │
    │── POST /backup/complete ─→│                                │                   │
    │                          │── set state=ready ───────────────────────────────→│
    │                          │── resume HaProxyStateSyncer ───→│                  │
    │                          │── resume HealthChecker ────────→│                  │
    │                          │── resume SyncApplier ──────────→│                  │
    │←── 200 OK ───────────────│                                │ → 从 prepare      │
    │                                                            │   checkpoint 追赶 │
```

### 3.2 BackupCoordinator 内部协调（4 件事一起做）

`POST /cluster/backup/prepare?nodeId=<target>` 内部执行（顺序很重要）：

| 步骤 | 子系统 | 目的 |
|---|---|---|
| 1 | `SyncApplier.pause(nodeId)` | 停止应用层写入；记录 prepare 时刻的 stream checkpoint 用于 complete 后追赶 |
| 2 | `HealthChecker.suppressFor(nodeId, maxDuration)` | 备份期间 Bolt 不可达不再触发 `onNodeDown`，避免误报警 / failover 误判 |
| 3 | `HaProxyStateSyncer.pause()` | 暂停周期 reconciler，防止它根据 ServiceState 把摘除的 standby 又加回读后端 |
| 4 | `HaProxyUpdater.setReadState(nodeId, maint)` | 显式从 HAProxy 读后端摘除，客户端不再读到这个 standby |

`POST /cluster/backup/complete` 反向恢复：set ready → resume StateSyncer → resume HealthChecker → resume SyncApplier。

**Trap 兜底**：脚本任何异常路径都通过 `trap` 调用 `complete`，避免集群留在半暂停状态。

### 3.3 关键不变式

| # | 不变式 | 失败影响 |
|---|---|---|
| 1 | 备份期间 standby 容器**已停止**（无 page cache 脏页 / WAL 未刷盘） | 直接 cp 运行中目录 → 文件损坏 |
| 2 | 备份期间 HealthChecker 不报警该 standby DOWN | 误报警 / NOC 困扰 |
| 3 | 备份期间 HaProxyStateSyncer 不"修复"读后端状态 | 摘除路由被 reconciler 强制改回，客户端读到停机容器 |
| 4 | SyncApplier 暂停期间，stream 上的事件继续累积（不消费、不 ACK） | complete 后从 checkpoint 重新消费追赶 |
| 5 | 暂停时间超过 `backup.maxDuration`（默认 2h）自动恢复 | 防脚本异常退出后 standby 永久滞后 |

---

## 4. 前置条件

- HA Agent 管理接口可用：`http://<host>:<port>`（v1.1+ 推荐 18888；v1.0 是 8080）
- 已配置管理员 Token（`ADMIN_TOKEN`，从 `docker/.env` 加载或显式 export）
- 备节点容器名已知（默认 `neo4j-standby-1`）
- 备节点 bind mount 数据目录可访问（默认 `/opt/neo4j-2/data` 或类似）
- 备份目录可写（默认 `/backup/neo4j`）
- 宿主机有充足磁盘空间：原始数据量 × 2（同时存在 data 目录 + 备份归档的瞬时占用）

---

## 5. 备份脚本

### 5.1 文件路径

- `scripts/backup/backup-standby.sh`

### 5.2 执行流程（9 步）

1. **预检**
   - `GET /health` → `UP`
   - `GET /cluster/status` → 目标节点 `role == STANDBY`（**拒绝 PRIMARY**）
   - `GET /cluster/backup/status` → 当前 `state == IDLE`（**拒绝并发备份**）
   - （可选）检查目标节点不是**唯一** standby（防影响可用性）
2. **`POST /cluster/backup/prepare?nodeId=<target>`** → 触发 §3.2 的 4 件事
3. **预 checkpoint**（缩短 stop 时间，可选）
   ```bash
   docker exec <target> cypher-shell -u neo4j -p $NEO4J_PASSWORD \
     "CALL db.checkpoint()"
   ```
4. **`docker stop <target>`** → graceful shutdown，确保所有脏页落盘
5. **拷贝数据目录**
   ```bash
   cp -a /opt/neo4j-2/data /tmp/staging-$TS
   tar -C /tmp -cf - staging-$TS | pigz > $BACKUP_ROOT/<target>-$TS.tar.gz
   sha256sum $BACKUP_ROOT/<target>-$TS.tar.gz > .sha256
   rm -rf /tmp/staging-$TS
   ```
   `pigz` 不可用时降级 `gzip`。
6. **`docker start <target>`** → Neo4j 重新启动
7. **等待容器就绪**
   ```bash
   until docker exec <target> cypher-shell ... "RETURN 1" >/dev/null 2>&1; do sleep 2; done
   ```
8. **`POST /cluster/backup/complete`** → 反向恢复 §3.2 的 4 件事；SyncApplier 从 prepare checkpoint 追赶
9. **保留清理**
   ```bash
   find $BACKUP_ROOT -maxdepth 1 -name "*.tar.gz" -mtime +$RETENTION_DAYS -delete
   ```

### 5.3 环境变量

| 变量 | 默认值 | 必填 | 说明 |
|---|---|---|---|
| `HA_AGENT_URL` | `http://localhost:18888` | 否 | HA Agent 端口（v1.1+ deploy-test.yml 用 18888） |
| `ADMIN_TOKEN` | 从 `docker/.env` 自动加载 | **是** | Bearer token |
| `STANDBY_NODE_ID` | — | **是** | HA Agent cluster node id（如 `node-02`，与 `ha-agent.yml: cluster.nodes[].id` 对齐） |
| `STANDBY_CONTAINER` | `neo4j-standby-1` | 否 | Docker container name |
| `DATA_SOURCE_DIR` | `/opt/neo4j-2/data` | 否 | 宿主机数据目录的 bind mount 路径 |
| `BACKUP_ROOT` | `/backup/neo4j` | 否 | 备份归档存放目录 |
| `RETENTION_DAYS` | `7` | 否 | 自动清理天数 |
| `COMPRESS_CMD` | `pigz`（不可用降级 `gzip`） | 否 | 压缩工具 |

### 5.4 执行示例

```bash
cd /home/ubuntu/neo4j-ha
chmod +x scripts/backup/backup-standby.sh

# 必填变量
export STANDBY_NODE_ID='node-02'
export STANDBY_CONTAINER='neo4j-standby-1'
export DATA_SOURCE_DIR='/opt/neo4j-2/data'
export BACKUP_ROOT='/backup/neo4j'

# 可选：覆盖 Agent URL
export HA_AGENT_URL='http://127.0.0.1:18888'

# ADMIN_TOKEN 会自动从 docker/.env 加载，无需手动 export
./scripts/backup/backup-standby.sh
```

### 5.5 成功判定

- 脚本输出 `[OK] Backup finished: /backup/neo4j/node-02-<TS>.tar.gz size=<X>GB duration=<Y>s`
- `.sha256` 文件存在
- `docker ps | grep <target>` → `Up`
- `GET /cluster/backup/status` → `state=IDLE`
- `GET /cluster/status` → 目标节点最终回到 `health=HEALTHY` 和 `serviceState=ONLINE`（追赶完成）

### 5.6 整体耗时拆解（100GB 数据，普通 SSD，参考值）

| 阶段 | 耗时 |
|---|---|
| 1–2 预检 + prepare | ~1s |
| 3 checkpoint | 3–10s |
| 4 docker stop | 5–30s |
| 5 cp + 压缩 + sha256 | 10–25 分钟 |
| 6 docker start + 就绪等待 | 30–60s |
| 7 complete + 增量追赶 | 1–10 分钟 |

**standby 离线总时长 = 4 + 5 + 6 ≈ 10–27 分钟**。

---

## 6. 恢复脚本

### 6.1 文件路径

- `scripts/backup/restore-standby.sh`

### 6.2 执行流程（5 步）

1. **`docker stop <target>`**
2. **清空数据目录**
   ```bash
   rm -rf $DATA_SOURCE_DIR/*
   ```
3. **解压备份归档**
   ```bash
   sha256sum -c $ARCHIVE.sha256           # 校验完整性
   tar -C /tmp -xzf $ARCHIVE
   mv /tmp/staging-$TS/* $DATA_SOURCE_DIR/
   ```
4. **`docker start <target>`** + 等待就绪
5. **HA Agent 自动追赶**：HealthChecker 检测到容器恢复，触发 `onNodeRecovered` → SyncApplier 从 checkpoint 重连 stream 追赶

### 6.3 环境变量

| 变量 | 默认值 |
|---|---|
| `STANDBY_CONTAINER` | `neo4j-standby-1` |
| `DATA_SOURCE_DIR` | `/opt/neo4j-2/data` |

### 6.4 执行示例

```bash
cd /home/ubuntu/neo4j-ha
chmod +x scripts/backup/restore-standby.sh

export STANDBY_CONTAINER='neo4j-standby-1'
export DATA_SOURCE_DIR='/opt/neo4j-2/data'

./scripts/backup/restore-standby.sh /backup/neo4j/node-02-20260515-160000.tar.gz
```

### 6.5 恢复后校验

- `GET /cluster/status`：目标节点 `health=HEALTHY`
- `GET /cluster/nodes/{id}`：`serviceState` 从 `SYNCING` 最终变为 `ONLINE`
- `GET /metrics`：`neo4j_ha_sync_lag_ms` 下降并稳定 < 2000
- HAProxy 读后端已将该节点置为 `ready`

如果恢复点距离当前时间太远（超过 Redis stream 保留窗口），HA Agent 会**自动触发 fullsync 重建**，不需要人工介入；但耗时会从分钟级变成 fullsync 级（参考 §9.3）。

---

## 7. 常见故障与处理

| 现象 | 可能原因 | 处理 |
|---|---|---|
| `prepare` 失败 401/403 | `ADMIN_TOKEN` 错误或缺失 | 检查 `docker/.env` 中的 `ADMIN_TOKEN`；确认 Authorization Header 格式 |
| `prepare` 失败 409 | 当前已有备份进行中 | 等当前备份完成；或确认上次备份是否异常退出（看 Agent 日志 `BackupCoordinator state`） |
| `prepare` 失败 400 `role mismatch` | 目标节点不是 STANDBY | 切换目标节点；**禁止对 PRIMARY 备份** |
| 备份期间 `state` 卡在 `IN_PROGRESS` 超过 `backup.maxDuration` | 脚本异常退出未调 complete | Agent 内部超时自动 resume；或手动 `POST /cluster/backup/complete` |
| `docker stop` 超时（30s 默认） | Neo4j shutdown 慢、有大事务 | 提前跑 `db.checkpoint()`；或调大 `docker stop -t 120` |
| 拷贝期间宿主机 OOM | tar/pigz 占内存 + Neo4j 容器虽 stop 但其他容器吃内存 | 降低压缩等级（gzip 比 pigz 省内存）；或加 swap |
| 恢复后节点长期 `SYNCING` | Stream 已 trim 触发 fullsync；或网络 / Bolt 异常 | 观察 `BUG-084/085/086` fullsync 日志；必要时手动 `POST /cluster/fullsync?nodeId=...` |
| 容器拉起失败 | 数据目录权限错乱（cp 后 owner 不对）、Neo4j 配置丢失 | `chown -R 7474:7474 $DATA_SOURCE_DIR`（Neo4j 镜像默认 UID）；确认 `conf/` 目录也被拷过 |

---

## 8. 运维建议

- **定期演练恢复流程**：每季度至少一次 restore 演练，验证 RTO 数字真实
- **每日低峰备份**：crontab 配置在业务 QPS 最低时段，留出 30 分钟 buffer
  ```cron
  30 3 * * *  /home/ubuntu/neo4j-ha/scripts/backup/backup-standby.sh >> /var/log/neo4j-backup.log 2>&1
  ```
- **`BACKUP_ROOT` 异地复制**：每天 rsync / s3 sync 到独立存储；不要把唯一备份和主机放在同一物理节点
- **保留策略按合规要求调整**：金融业务可能要求 90+ 天保留；普通业务 7-14 天即可
- **监控备份新鲜度**：Prometheus 报警 `time() - file_mtime > 86400 × N`（最新备份超过 N 天）
- **3-2-1 原则**：3 份副本，2 种介质，1 份异地
- **多 standby 集群**：备份谁就摘谁，其他 standby 顶住读流量，可用性影响最小化
- **2 节点（1 primary + 1 standby）部署**：备份期间评估业务影响；考虑在备份窗口前给 primary 额外读流量预算

---

## 9. 容量与时间评估（经验值）

实际值请以首次压测结果为准。以下为方案 C 在普通 SSD 上的参考。

### 9.1 全量备份大小与原始数据比例

- 原始 data 目录大小（未压缩）：≈ 原始数据量 × `1.0–1.2`（含事务日志 + 索引）
- `tar.gz`（gzip 单线程）：≈ 原始数据 × `0.4–0.9`
- `tar | pigz`（多线程）：体积同 gzip，**速度快 4–8 倍**
- `tar | zstd -3`（推荐进阶）：体积比 gzip 小 10–20%，速度快 5–10 倍

影响压缩比的因素：

- 属性值可压缩性（文本 / JSON 多 → 高）
- 数据熵（随机二进制多 → 低）
- 事务日志保留量、索引规模

容量规划建议：

- 单备份体积按原始数据 × `0.6–0.8` 估算
- `BACKUP_ROOT` 容量 ≥ `RETENTION_DAYS × 0.8x × 原始数据量 × 1.5`（含同名 cp 中转）

### 9.2 100GB 数据备份时间评估

总耗时 = `prepare + stop + cp/压缩 + start + complete + 追赶`。

| 阶段 | NVMe + 8 核 | 普通 SSD + 4 核 | 机械盘 / 高负载 |
|---|---|---|---|
| stop + start 容器 | 30–60s | 1–2 分钟 | 2–5 分钟 |
| cp + pigz | 4–8 分钟 | 10–20 分钟 | 25–60 分钟 |
| 追赶（增量小） | 30s–2 分钟 | 1–5 分钟 | 5–15 分钟 |
| **总计** | **5–10 分钟** | **15–25 分钟** | **30–80 分钟** |

维护窗口建议：按实测结果的 `1.5x – 2x` 预留缓冲。

### 9.3 100GB 数据恢复时间评估

恢复总耗时 = `停容器 + 清空 + 解压 + 启动 + 追赶`。

| 阶段 | NVMe | 普通 SSD | 机械盘 |
|---|---|---|---|
| stop + clean + 解压 | 4–10 分钟 | 10–20 分钟 | 25–50 分钟 |
| start | 30s–1 分钟 | 1–2 分钟 | 2–5 分钟 |
| 追赶（增量） | 1–5 分钟 | 5–15 分钟 | 15–30 分钟 |
| 追赶（触发 fullsync） | 5–30 分钟 | 15–60 分钟 | 60+ 分钟 |

**RTO 口径建议（100GB）**：

- 乐观（增量追赶）：`10–30 分钟`
- 常见：`30–60 分钟`
- 最差（触发 fullsync）：`60 分钟–数小时`

---

## 10. v2 与 v1 的差异速查

| 维度 | v1（runbook 旧版） | v2（本版） |
|---|---|---|
| 备份方法 | "停容器 + tar 数据目录" | **同样**——但增加 BackupCoordinator 协调 4 件事 |
| Coordinator 暂停项 | 仅 SyncApplier | **SyncApplier + HealthChecker + HaProxyStateSyncer + HAProxy maint** |
| 环境变量名 | `STANDBY_SERVICE_NAME` / `BACKUP_SOURCE_DIR` / `COMPOSE_FILE` | **`STANDBY_CONTAINER` / `DATA_SOURCE_DIR`**；移除未使用的变量 |
| 压缩工具 | `gzip` 单线程 | **`pigz` 多线程优先，gzip 降级** |
| 容器停机判定 | runbook 未说明何时拷贝 | 明确：**docker stop 之后才 cp** |
| 唯一标识 | 容器名 vs nodeId 含糊 | **明确 nodeId 用于 HA Agent，容器名用于 docker** |
| Trap 兜底 | 未明确 | **明确 4 件事的反向恢复 + maxDuration 兜底** |

## 11. 后续工作 (TODO)

- HA Agent 端增强：`/cluster/backup/prepare` 内部一并 pause HealthChecker + HaProxyStateSyncer（当前 v1 实现只 pause SyncApplier）—— **必须实施，否则 v2 脚本无法真实工作**
- `backup-standby.sh` 重写按 v2 流程对齐脚本和 runbook
- 加 Prometheus 指标：`ha_backup_duration_seconds` / `ha_backup_last_success_timestamp` / `ha_backup_archive_bytes`
- 备份完整性校验：周期性 sha256 重新验证已落盘备份是否未被损坏
- 自动化恢复演练：CI 定期跑一次 backup → restore → 数据校验
