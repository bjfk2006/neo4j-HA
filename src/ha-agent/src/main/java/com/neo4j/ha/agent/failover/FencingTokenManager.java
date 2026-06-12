package com.neo4j.ha.agent.failover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class FencingTokenManager {

    private static final Logger log = LoggerFactory.getLogger(FencingTokenManager.class);

    private final JedisPool jedisPool;
    private final String tokenKey;

    public FencingTokenManager(JedisPool jedisPool, String tokenKey) {
        this.jedisPool = jedisPool;
        this.tokenKey = tokenKey;
    }

    public long increment() {
        try (Jedis jedis = jedisPool.getResource()) {
            long newToken = jedis.incr(tokenKey);
            log.info("Fencing token incremented to: {}", newToken);
            return newToken;
        }
    }

    public long getCurrentToken() {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(tokenKey);
            return value != null ? Long.parseLong(value) : 0;
        }
    }

    public void setToken(long token) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(tokenKey, String.valueOf(token));
        }
    }
}
