# Failover Manager 模块详细设计

> **⚠️ 已废弃（v2.0）**: 本模块的所有职责已合并到 [ha-agent-design.md](./ha-agent-design.md) 的集中式 HA Agent 中。
> 保留此文件仅供历史参考。健康检查、Failover 编排、Fencing Token 管理等内容请参见 ha-agent-design.md §5-§6。

> 模块: failover-manager
> 运行位置: ~~独立进程，部署在仲裁节点~~ → 已合并到 ha-agent
> 状态: **DEPRECATED since v2.0**

---

## 1. 职责

~~健康检查、故障检测、Leader 选举、Failover 编排、Fencing Token 管理。是整个 HA 系统的"大脑"。~~

**v2.0 变更：** 上述职责全部由集中式 HA Agent 承担。详见 [ha-agent-design.md](./ha-agent-design.md)。

## 2. 包结构

```
com.neo4j.ha.failover/
├── FailoverManager.java                # 主入口
├── FailoverManagerConfig.java          # 配置
├── health/
│   ├── HealthChecker.java              # 多层健康检查调度
│   ├── TcpHealthCheck.java             # L1: TCP端口探活
│   ├── BoltHealthCheck.java            # L2: Bolt协议握手
│   ├── CypherHealthCheck.java          # L3: Cypher查询
│   ├── WriteHealthCheck.java           # L4: 写入测试
│   ├── HealthState.java                # 健康状态机
│   └── HealthCheckResult.java          # 探活结果
├── election/
│   ├── LeaderElection.java             # Leader选举（基于Redis分布式锁）
│   ├── FencingTokenManager.java        # Fencing Token 递增+分发
│   └── ElectionState.java              # 选举状态
├── orchestration/
│   ├── FailoverOrchestrator.java       # Failover流程编排
│   ├── StandbySelector.java            # 最佳备节点选择
│   ├── RoleSwitch.java                 # 角色切换执行
│   └── DrainWaiter.java                # 等待PEL排空
├── registry/
│   ├── NodeRegistry.java               # 节点注册表管理
│   └── NodeHeartbeat.java              # 节点心跳上报
├── routing/
│   ├── HaProxyUpdater.java             # HAProxy配置更新
│   └── RoutingNotifier.java            # 路由变更通知
└── audit/
    ├── FailoverAuditLog.java           # Failover审计日志
    └── FailoverHistory.java            # 历史记录
```

## 3. 健康检查状态机

```
                 ┌──────────────────────────────────────────┐
                 │                                          │
                 ▼                                          │
  ┌──────────┐  L1/L2失败×3   ┌──────────┐  L3失败×2   ┌────────┐
  │ HEALTHY  │───────────────▶│ SUSPECT  │────────────▶│UNHEALTHY│
  │          │◀───────────────│          │◀────────────│        │
  └──────────┘  任一层级恢复   └──────────┘  L1/L2恢复  └────┬───┘
       ▲                                                    │
       │                                             L4失败×2
       │        ┌──────────┐                                │
       └────────│  DOWN    │◀───────────────────────────────┘
         全量恢复│          │
                └──────────┘
```

**各状态的含义与动作：**

| 状态 | 含义 | 触发动作 |
|------|------|---------|
| HEALTHY | 节点正常 | 无 |
| SUSPECT | 疑似故障 | 增加检查频率，记录日志 |
| UNHEALTHY | 确认故障 | 告警，准备Failover |
| DOWN | 节点宕机 | 触发 Failover（如果是主节点） |

## 4. Failover 编排详细流程

```java
public class FailoverOrchestrator {

    void executeFailover(String failedNodeId) {
        FailoverContext ctx = new FailoverContext(failedNodeId);
        audit.start(ctx);

        try {
            // Phase 1: 确认 — 二次确认主节点确实不可用
            confirmationWait(ctx);
            if (healthChecker.isHealthy(failedNodeId)) {
                audit.cancel(ctx, "Node recovered during confirmation");
                return;
            }

            // Phase 2: Fence — 递增 Fencing Token，使旧主的所有操作失效
            long newToken = fencingTokenManager.increment();
            ctx.setFencingToken(newToken);

            // Phase 3: 选择新主 — 选 checkpoint 最新的备节点
            String newPrimary = standbySelector.selectBest();
            ctx.setNewPrimary(newPrimary);

            // Phase 4: 通知旧主 step-down（尽力而为）
            controlStream.publish(ControlCommand.stepDown(failedNodeId, newToken));

            // Phase 5: 等待新主排空 PEL
            drainWaiter.waitForDrain(newPrimary, drainTimeout);

            // Phase 6: 角色切换
            roleSwitch.promote(newPrimary, newToken);
            // → 通知 HA Agent 启动 CDC Collector、停止 Sync Applier
            // → 获取 Leader Lock

            // Phase 7: 更新路由
            haProxyUpdater.switchPrimary(newPrimary);

            // Phase 8: 更新注册表
            nodeRegistry.updateRole(newPrimary, NodeRole.PRIMARY);
            nodeRegistry.updateRole(failedNodeId, NodeRole.DOWN);

            audit.complete(ctx);
            metrics.recordFailover(ctx);

        } catch (Exception e) {
            audit.fail(ctx, e);
            alerting.critical("Failover failed", e);
        }
    }
}
```

## 5. 备节点选择策略

```java
public class StandbySelector {

    String selectBest() {
        List<NodeInfo> standbys = nodeRegistry.getByRole(NodeRole.STANDBY);

        // 过滤健康节点
        standbys = standbys.stream()
            .filter(n -> healthChecker.getState(n.getId()) == HEALTHY)
            .toList();

        if (standbys.isEmpty()) {
            throw new NoHealthyStandbyException();
        }

        // 按 checkpoint 新旧排序（最新优先 = 数据最完整）
        return standbys.stream()
            .sorted(Comparator.comparing(
                n -> checkpointManager.load(n.getId())
                    .map(Checkpoint::lastEventTs)
                    .orElse(0L),
                Comparator.reverseOrder()
            ))
            .findFirst()
            .map(NodeInfo::getId)
            .orElseThrow();
    }
}
```

## 6. 节点注册表

```
Redis Hash: neo4j:ha:node-registry

Fields:
  node-01 → {"role":"PRIMARY","host":"10.0.1.1","boltPort":7687,"healthPort":8080,"lastHeartbeat":1712736000000}
  node-02 → {"role":"STANDBY","host":"10.0.1.2","boltPort":7687,"healthPort":8080,"lastHeartbeat":1712736000500}
```

每个 HA Agent 每 2 秒向 `node-registry` 上报心跳（HSET 更新 lastHeartbeat）。
Failover Manager 通过心跳超时（默认 10s 未更新）辅助判断节点存活。

## 7. HAProxy 配置更新

Failover 后需要更新 HAProxy 使写流量指向新主：

**方案 A: HAProxy Runtime API**
```bash
# 通过 HAProxy stats socket 动态切换
echo "set server neo4j_primary/neo4j-01 state drain" | socat stdio /var/run/haproxy/admin.sock
echo "set server neo4j_primary/neo4j-02 state ready" | socat stdio /var/run/haproxy/admin.sock
```

**方案 B: 健康检查端点自动切换**
- 不需要直接操作 HAProxy
- HA Agent 更新健康端点 `/db/neo4j/cluster/writable` 的返回值
- 新主返回 `true`，旧主返回 `false`
- HAProxy 通过 tcp-check 自动感知，无需 reload

推荐方案 B（更简单、无需额外权限）。

## 8. Failover Manager 自身高可用

Failover Manager 作为单点存在单点故障风险。解决方案：

**多实例 + Redis 选举**
- 每个节点上部署一个 Failover Manager 实例
- 通过 Redis 分布式锁选举 Active Failover Manager
- Active 实例持有 `neo4j:ha:failover-manager-lock`，TTL 15s，每 5s 续期
- 其他实例 Standby，监控锁释放后竞争接管

## 9. 防误切机制

| 机制 | 说明 |
|------|------|
| 二次确认 | 检测到 DOWN 后等待 confirmationWait (5s) 再次检查 |
| 最小Failover间隔 | 两次 Failover 之间至少间隔 minFailoverInterval (60s) |
| 最大自动切换次数 | 1小时内最多自动切换 maxAutoFailovers (3次)，超出需人工介入 |
| Fencing Token 单调递增 | 防止旧主复活后脑裂 |
| 心跳 + 多层探活 | 避免单一检查方式的误判 |
