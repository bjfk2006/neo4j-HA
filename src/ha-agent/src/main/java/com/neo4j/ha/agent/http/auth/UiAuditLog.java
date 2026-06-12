package com.neo4j.ha.agent.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes UI audit events to Redis stream {@code neo4j:ha:ui-audit}. Redis
 * failures degrade gracefully to SLF4J ERROR — admin UI must keep working
 * when Redis is unreachable (the cluster's own audit stream
 * `neo4j:ha:failover-audit` faces the same degradation in
 * {@code FailoverAuditLog}).
 */
public class UiAuditLog {

    private static final Logger log = LoggerFactory.getLogger(UiAuditLog.class);

    private static final String STREAM_KEY = "neo4j:ha:ui-audit";
    private static final long MAX_LEN = 100_000L;

    private final JedisPool jedisPool;

    public UiAuditLog(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void logLoginSuccess(String username, String ip, String userAgent) {
        publish("login.success", Map.of(
            "username", nullToEmpty(username),
            "ip", nullToEmpty(ip),
            "userAgent", nullToEmpty(userAgent)
        ));
    }

    public void logLoginFailure(String claimedUsername, String ip, String reason) {
        publish("login.failure", Map.of(
            "username", nullToEmpty(claimedUsername),
            "ip", nullToEmpty(ip),
            "reason", nullToEmpty(reason)
        ));
    }

    public void logLoginLocked(String ip, long lockedForMs) {
        publish("login.locked", Map.of(
            "ip", nullToEmpty(ip),
            "lockedForMs", String.valueOf(lockedForMs)
        ));
    }

    public void logLogout(String username) {
        publish("logout", Map.of("username", nullToEmpty(username)));
    }

    public void logOperation(String op, String actor, String ip, String params, String requestId) {
        publish("op." + op, Map.of(
            "actor", nullToEmpty(actor),
            "ip", nullToEmpty(ip),
            "params", nullToEmpty(params),
            "requestId", nullToEmpty(requestId)
        ));
    }

    private void publish(String type, Map<String, String> fields) {
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("ts", String.valueOf(System.currentTimeMillis()));
        payload.put("type", type);
        payload.putAll(fields);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.xadd(STREAM_KEY,
                XAddParams.xAddParams().maxLen(MAX_LEN).approximateTrimming(),
                payload);
        } catch (Exception e) {
            // Redis down — do not block UI/admin operations. Log + drop.
            log.error("ui-audit publish failed (type={}): {}", type, e.toString());
        }
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}
