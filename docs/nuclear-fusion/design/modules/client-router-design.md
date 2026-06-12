# Client Router 模块详细设计

> **⚠️ 已废弃（v2.0）**: 本模块的 HAProxy 管理职责已合并到 [ha-agent-design.md](./ha-agent-design.md) 的集中式 HA Agent 中。
> v2.0 使用 HAProxy Runtime API（admin socket）直接控制路由，不再需要独立的 Client Router 进程。
> 保留此文件仅供历史参考。HAProxy 路由管理请参见 ha-agent-design.md §7。

> 模块: client-router
> 运行位置: ~~HAProxy 节点 / 独立部署~~ → 已合并到 ha-agent
> 状态: **DEPRECATED since v2.0**

---

## 1. 职责

~~管理客户端到 Neo4j 的连接路由。通过 HAProxy 实现读写分离和自动故障切换。~~

**v2.0 变更：** HAProxy 路由管理由 HA Agent 内的 `HaProxyUpdater` 组件通过 admin socket 完成。详见 [ha-agent-design.md](./ha-agent-design.md)。

## 2. 包结构

```
com.neo4j.ha.router/
├── ClientRouter.java                   # 主入口
├── haproxy/
│   ├── HaProxyConfigGenerator.java     # 动态生成 HAProxy 配置
│   ├── HaProxyReloader.java            # 热加载 HAProxy（Runtime API / reload）
│   └── HaProxyTemplate.java            # 配置模板
├── discovery/
│   ├── NodeDiscovery.java              # 从 node-registry 发现节点
│   └── TopologyWatcher.java            # 监听拓扑变化
└── sdk/
    └── Neo4jHaDriver.java              # (可选) 应用层SDK封装
```

## 3. 路由策略

```
客户端连接:
  ├── 写操作 (bolt://vip:7687)
  │   └── HAProxy frontend:neo4j_write
  │       └── backend:neo4j_primary (仅主节点)
  │           └── tcp-check → /db/neo4j/cluster/writable == true
  │
  └── 读操作 (bolt://vip:7688)
      └── HAProxy frontend:neo4j_read
          └── backend:neo4j_all (所有健康节点 roundrobin)
              └── tcp-check → /db/neo4j/cluster/available == true
```

## 4. 动态配置生成

TopologyWatcher 监听 `neo4j:ha:node-registry` 变化，当拓扑发生变更时：
1. 读取最新节点列表
2. HaProxyConfigGenerator 生成新配置
3. HaProxyReloader 执行热加载

## 5. 应用层 SDK（可选）

对于不使用 HAProxy 的场景，提供 Java SDK 封装：

```java
// 应用层使用
Neo4jHaDriver driver = Neo4jHaDriver.builder()
    .redisUri("redis-sentinel://...")
    .build();

// 自动路由到主节点
try (Session session = driver.writeSession()) {
    session.run("CREATE (n:Person {name: $name})", Map.of("name", "Alice"));
}

// 自动路由到任一健康节点
try (Session session = driver.readSession()) {
    session.run("MATCH (n:Person) RETURN n");
}
```

SDK 内部逻辑：
- 订阅 `neo4j:ha:node-registry` 变化
- 维护本地路由表
- write → 连接 role=PRIMARY 的节点
- read → 轮询 role=PRIMARY|STANDBY 的健康节点
- 连接失败自动重试下一个节点
