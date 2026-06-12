# Common 模块详细设计

> 模块: common
> 类型: 共享库（被所有其他模块依赖）

---

## 1. 职责

提供所有模块共用的数据模型、客户端封装、工具类和配置加载能力。

## 2. 包结构

```
com.neo4j.ha.common/
├── model/
│   ├── ChangeEvent.java              # CDC变更事件模型
│   ├── ChangeEventType.java          # 事件类型枚举
│   ├── EntityType.java               # 实体类型枚举(NODE/RELATIONSHIP)
│   ├── EntityData.java               # 实体数据（属性、标签等）
│   ├── NodeRole.java                 # 节点角色枚举(PRIMARY/STANDBY)
│   ├── NodeHealth.java               # 健康状态枚举
│   ├── NodeServiceState.java         # 服务状态枚举(OFFLINE/SYNCING/ONLINE)
│   ├── NodeInfo.java                 # 节点注册信息
│   ├── SyncMode.java                 # 同步模式枚举
│   ├── FullSyncBatch.java            # 全量同步批次
│   ├── ControlCommand.java           # 控制指令模型
│   └── FailoverEvent.java            # Failover事件记录
├── redis/
│   ├── RedisClientFactory.java       # Redis客户端工厂（支持Standalone/Sentinel/Cluster）
│   ├── StreamPublisher.java          # Stream发布封装
│   ├── StreamConsumer.java           # Stream消费封装
│   ├── DistributedLock.java          # 分布式锁封装
│   └── CheckpointManager.java        # Checkpoint读写封装
├── neo4j/
│   ├── Neo4jClientFactory.java       # Neo4j Driver 工厂
│   ├── Neo4jHealthChecker.java       # 多层健康检查
│   └── CypherExecutor.java           # Cypher执行封装（含重试）
├── config/
│   ├── HaConfig.java                 # 配置根模型（映射YAML）
│   ├── ConfigLoader.java             # YAML配置加载 + 环境变量覆盖
│   └── ConfigValidator.java          # 配置校验
├── serialization/
│   ├── EventSerializer.java          # ChangeEvent JSON序列化
│   └── EventDeserializer.java        # ChangeEvent JSON反序列化
├── metrics/
│   ├── MetricsRegistry.java          # Prometheus指标注册
│   └── HaMetrics.java               # 所有HA相关Metric定义
└── util/
    ├── FencingTokenValidator.java    # Fencing Token校验
    ├── RetryUtil.java                # 重试工具
    └── IdGenerator.java              # UUID v7 生成
```

## 3. 核心数据模型

### 3.1 ChangeEvent

```java
public record ChangeEvent(
    String eventId,                    // UUID v7
    ChangeEventType eventType,         // NODE_CREATED, NODE_UPDATED, ...
    String database,                   // "neo4j"
    long timestamp,                    // epoch millis
    long fencingToken,                 // 防脑裂令牌
    String txId,                       // 事务序列号
    EntityData entity,                 // 实体数据
    EventMetadata metadata             // 批次元数据
) {}
```

### 3.2 ChangeEventType

```java
public enum ChangeEventType {
    NODE_CREATED,
    NODE_UPDATED,
    NODE_DELETED,
    RELATIONSHIP_CREATED,
    RELATIONSHIP_UPDATED,
    RELATIONSHIP_DELETED,
    // 控制事件
    FULL_SYNC_START,
    FULL_SYNC_BATCH,
    FULL_SYNC_END,
    STEP_DOWN,
    HEARTBEAT
}
```

### 3.3 NodeServiceState

```java
/**
 * 节点服务状态 — 判断节点数据是否就绪、能否对外提供服务。
 * 与 NodeHealth（进程存活性）正交，两者共同决定节点是否接收流量。
 *
 * 状态转换:
 *   OFFLINE → SYNCING: Neo4j 进程启动，HA Agent 连接成功
 *   SYNCING → ONLINE:  同步延迟 < syncLagThreshold 且持续 stableDuration
 *   ONLINE  → SYNCING: 触发全量同步（数据清空重建）
 *   ONLINE  → OFFLINE: 节点宕机
 *   SYNCING → OFFLINE: 节点宕机
 */
public enum NodeServiceState {
    OFFLINE,    // 节点不可达
    SYNCING,    // 数据同步中，不接收客户端流量，不可作为 Failover 目标
    ONLINE      // 数据就绪，接收读流量，可作为 Failover 目标
}
```

### 3.4 EntityData

```java
public record EntityData(
    EntityType type,                    // NODE | RELATIONSHIP
    String elementId,                   // Neo4j element ID
    List<String> labels,                // 节点标签
    Map<String, Object> properties,     // 当前属性
    Map<String, Object> beforeState,    // 变更前属性（仅UPDATE）
    String startNodeElementId,          // 关系起点（仅RELATIONSHIP）
    String endNodeElementId,            // 关系终点（仅RELATIONSHIP）
    String relationshipType             // 关系类型（仅RELATIONSHIP）
) {}
```

## 4. 关键类设计

### 4.1 StreamPublisher

```java
public class StreamPublisher {
    // 原子性检查 Fencing Token + XADD（Lua脚本）
    void publish(String streamKey, ChangeEvent event, long fencingToken);

    // 批量发布（Pipeline）
    void publishBatch(String streamKey, List<ChangeEvent> events, long fencingToken);

    // 发布全量同步批次
    void publishFullSyncBatch(String streamKey, FullSyncBatch batch);
}
```

### 4.2 StreamConsumer

```java
public class StreamConsumer {
    // 创建 Consumer Group（如不存在）
    void ensureGroup(String streamKey, String groupName);

    // 阻塞消费
    List<StreamEntry> consume(String streamKey, String groupName,
                              String consumerName, int count, long blockMs);

    // 批量ACK
    void ack(String streamKey, String groupName, String... messageIds);

    // 读取PEL（未确认消息）
    List<StreamEntry> readPending(String streamKey, String groupName,
                                  String consumerName, int count);

    // XCLAIM — 接管其他消费者的超时消息
    List<StreamEntry> claim(String streamKey, String groupName,
                            String consumerName, long minIdleMs, String... messageIds);
}
```

### 4.3 DistributedLock

```java
public class DistributedLock {
    // 尝试获取锁（非阻塞）
    Optional<LockHandle> tryAcquire(String lockKey, String ownerId, Duration ttl);

    // 续期（Lua脚本保证原子性）
    boolean renew(LockHandle handle, Duration ttl);

    // 释放（Lua脚本保证只释放自己的锁）
    boolean release(LockHandle handle);

    // 查看当前持有者
    Optional<String> currentOwner(String lockKey);
}
```

### 4.4 CheckpointManager

```java
public class CheckpointManager {
    // === CDC Collector (主节点) — keyset pagination 复合游标 ===

    // 保存 CDC 轮询位点（复合游标：timestamp + elementId）
    void saveCdcCheckpoint(String nodeId, long lastTs, String lastElementId,
                           long lastDeleteTs, String lastDeleteEid,
                           String lastStreamId);

    // 加载 CDC 轮询位点
    Optional<CdcCheckpoint> loadCdcCheckpoint(String nodeId);

    // === Sync Applier (备节点) — Stream 消费位点 ===

    // 保存消费位点
    void saveSyncCheckpoint(String nodeId, String lastStreamId, long lastEventTs, SyncMode mode);

    // 加载消费位点
    Optional<SyncCheckpoint> loadSyncCheckpoint(String nodeId);

    // 检查位点是否仍在Stream中
    boolean isCheckpointValid(String streamKey, String lastStreamId);
}

// CDC 轮询位点（主节点使用）
public record CdcCheckpoint(
    long lastTs,               // 最后发布事件的 _updated_at
    String lastElementId,      // 最后发布事件的 _elementId（keyset 第二游标）
    long lastDeleteTs,         // 最后处理的删除事件时间戳
    String lastDeleteEid,      // 最后处理的删除事件 elementId
    String lastStreamId        // 最后写入的 Stream Message ID
) {}

// Sync 消费位点（备节点使用）
public record SyncCheckpoint(
    String lastStreamId,       // 最后成功消费的 Stream Message ID
    long lastEventTs,          // 最后事件的时间戳
    SyncMode syncMode,         // INCREMENTAL | FULL_SYNC | CATCHING_UP
    long lastFullSyncAt,       // 上次全量同步完成时间
    long pendingCount          // PEL 中待确认消息数
) {}
```

## 5. 配置模型

HaConfig 作为根配置，映射 `ha-agent.yml` 全部字段，使用 Jackson YAML 反序列化。支持 `${ENV_VAR}` 语法的环境变量替换。

## 6. 依赖

```xml
<dependencies>
    <!-- Neo4j -->
    <dependency>neo4j-java-driver 5.x</dependency>
    <!-- Redis -->
    <dependency>jedis 5.x 或 lettuce 6.x</dependency>
    <!-- Serialization -->
    <dependency>jackson-databind + jackson-dataformat-yaml</dependency>
    <!-- Metrics -->
    <dependency>micrometer-registry-prometheus</dependency>
    <!-- Logging -->
    <dependency>slf4j-api + logback (JSON encoder)</dependency>
    <!-- Utility -->
    <dependency>lombok (optional)</dependency>
</dependencies>
```
