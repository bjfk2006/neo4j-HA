package com.neo4j.ha.agent.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JedisPool jedisPool;
    private final String fencingTokenKey;

    public FailoverAuditLog(JedisPool jedisPool) {
        this(jedisPool, null);
    }

    public FailoverAuditLog(JedisPool jedisPool, String fencingTokenKey) {
        this.jedisPool = jedisPool;
        this.fencingTokenKey = fencingTokenKey;
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
            IdGenerator.uuidV7(), failedNodeId, newPrimaryId, currentFencingToken(),
            System.currentTimeMillis() - durationMs, System.currentTimeMillis(),
            "SUCCESS", "Automatic failover"
        ));
    }

    public void logFailed(String failedNodeId, Exception e) {
        log.error("[AUDIT] Failover FAILED for node {}: {}", failedNodeId, e.getMessage());
        saveToRedis(new FailoverEvent(
            IdGenerator.uuidV7(), failedNodeId, null, currentFencingToken(),
            System.currentTimeMillis(), System.currentTimeMillis(),
            "FAILED", e.getMessage()
        ));
    }

    /**
     * REVIEW-C7: the fencing token was hardcoded to 0 in every persisted event,
     * so the audit trail could never answer "which epoch was this?" — the single
     * most useful field when reconstructing a split-brain incident.
     */
    private long currentFencingToken() {
        if (fencingTokenKey == null) return 0L;
        try (Jedis jedis = jedisPool.getResource()) {
            String v = jedis.get(fencingTokenKey);
            return v == null ? 0L : Long.parseLong(v);
        } catch (Exception e) {
            return 0L;
        }
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

    /**
     * REVIEW-C7: persist as JSON, not {@code record.toString()}.
     *
     * <p>The old format was Java's record rendering
     * ({@code FailoverEvent[eventId=..., startTime=...]}), which
     * {@code AuditController} could not parse. It surfaced the whole line as an
     * opaque {@code details.raw} string and, because it had no timestamp to sort
     * on, SYNTHESISED one as {@code System.currentTimeMillis() - index}. Every
     * historical failover therefore displayed as "just now" and always sorted
     * above the real UI-audit entries — the audit view was actively misleading
     * exactly when someone was using it to reconstruct an incident.</p>
     */
    private void saveToRedis(FailoverEvent event) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.lpush(HISTORY_KEY, MAPPER.writeValueAsString(event));
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
