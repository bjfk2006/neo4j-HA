package com.neo4j.ha.agent.audit;

import com.neo4j.ha.common.model.FailoverEvent;
import com.neo4j.ha.common.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

public class FailoverAuditLog {

    private static final Logger log = LoggerFactory.getLogger(FailoverAuditLog.class);
    private static final String HISTORY_KEY = "neo4j:ha:failover-history";
    private static final int MAX_HISTORY = 100;

    private final JedisPool jedisPool;

    public FailoverAuditLog(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void logStart(String failedNodeId) {
        log.info("[AUDIT] Failover started for node: {}", failedNodeId);
    }

    public void logCancel(String failedNodeId, String reason) {
        log.info("[AUDIT] Failover cancelled for node {}: {}", failedNodeId, reason);
    }

    public void logComplete(String failedNodeId, String newPrimaryId, long durationMs) {
        log.info("[AUDIT] Failover complete: {} -> {} ({}ms)", failedNodeId, newPrimaryId, durationMs);
        saveToRedis(new FailoverEvent(
            IdGenerator.uuidV7(), failedNodeId, newPrimaryId, 0,
            System.currentTimeMillis() - durationMs, System.currentTimeMillis(),
            "SUCCESS", "Automatic failover"
        ));
    }

    public void logFailed(String failedNodeId, Exception e) {
        log.error("[AUDIT] Failover FAILED for node {}: {}", failedNodeId, e.getMessage());
        saveToRedis(new FailoverEvent(
            IdGenerator.uuidV7(), failedNodeId, null, 0,
            System.currentTimeMillis(), System.currentTimeMillis(),
            "FAILED", e.getMessage()
        ));
    }

    public void logRecoveryStart(String nodeId) {
        log.info("[AUDIT] Old primary recovery started: {}", nodeId);
    }

    public void logRecoveryComplete(String nodeId) {
        log.info("[AUDIT] Old primary recovery complete: {}", nodeId);
    }

    public void logRecoveryFailed(String nodeId, Exception e) {
        log.error("[AUDIT] Old primary recovery failed: {}: {}", nodeId, e.getMessage());
    }

    private void saveToRedis(FailoverEvent event) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(HISTORY_KEY, event.toString());
            jedis.ltrim(HISTORY_KEY, 0, MAX_HISTORY - 1);
        } catch (Exception e) {
            log.warn("Failed to save failover event to Redis", e);
        }
    }

    public List<String> getHistory(int count) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(HISTORY_KEY, 0, count - 1);
        }
    }
}
