package com.neo4j.ha.agent.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.common.model.NodeInfo;
import com.neo4j.ha.common.model.NodeRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JedisPool jedisPool;
    private final String registryKey;
    private final long updateIntervalMs;
    private final ClusterStateManager clusterState;
    private final Map<String, NodeInfo> localState = new HashMap<>();
    private ScheduledExecutorService scheduler;

    public NodeRegistry(JedisPool jedisPool, String registryKey, long updateIntervalMs,
                        ClusterStateManager clusterState) {
        this.jedisPool = jedisPool;
        this.registryKey = registryKey;
        this.updateIntervalMs = updateIntervalMs;
        this.clusterState = clusterState;
    }

    public void register(NodeInfo nodeInfo) {
        localState.put(nodeInfo.id(), nodeInfo);
        writeToRedis(nodeInfo);
    }

    public void updateRole(String nodeId, NodeRole role) {
        NodeInfo info = localState.get(nodeId);
        if (info != null) {
            NodeInfo updated = info.withRole(role);
            localState.put(nodeId, updated);
            writeToRedis(updated);
        }
    }

    public void markPendingCleanup(String nodeId, boolean pending) {
        NodeInfo info = localState.get(nodeId);
        if (info != null) {
            NodeInfo updated = info.withPendingCleanup(pending);
            localState.put(nodeId, updated);
            writeToRedis(updated);
        }
        clusterState.setPendingCleanup(nodeId, pending);
    }

    public boolean isPendingCleanup(String nodeId) {
        NodeInfo liveInfo = clusterState.getNodeInfo(nodeId);
        if (liveInfo != null) {
            return liveInfo.pendingCleanup();
        }
        NodeInfo info = localState.get(nodeId);
        return info != null && info.pendingCleanup();
    }

    public void startPeriodicUpdate() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-registry");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::updateAll, updateIntervalMs, updateIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduler != null) scheduler.shutdown();
    }

    private void updateAll() {
        for (NodeInfo info : clusterState.getAllNodes()) {
            localState.put(info.id(), info);
            writeToRedis(info);
        }
    }

    private void writeToRedis(NodeInfo info) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(registryKey, info.id(), MAPPER.writeValueAsString(info));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize NodeInfo for {}", info.id(), e);
        }
    }

    public Map<String, NodeInfo> loadAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(registryKey);
            Map<String, NodeInfo> result = new HashMap<>();
            for (var entry : data.entrySet()) {
                try {
                    result.put(entry.getKey(), MAPPER.readValue(entry.getValue(), NodeInfo.class));
                } catch (JsonProcessingException e) {
                    log.warn("Failed to parse NodeInfo for {}", entry.getKey(), e);
                }
            }
            return result;
        }
    }
}
