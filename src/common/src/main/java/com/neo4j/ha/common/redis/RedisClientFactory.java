package com.neo4j.ha.common.redis;

import com.neo4j.ha.common.config.HaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.JedisPooled;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class RedisClientFactory {

    private static final Logger log = LoggerFactory.getLogger(RedisClientFactory.class);

    private final HaConfig.RedisConfig config;
    private volatile JedisPool pool;
    private volatile JedisPool blockingPool;

    public RedisClientFactory(HaConfig.RedisConfig config) {
        this.config = config;
    }

    public JedisPool createPool() {
        if (pool != null) {
            return pool;
        }
        synchronized (this) {
            if (pool != null) {
                return pool;
            }

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(config.pool().maxTotal());
            poolConfig.setMaxIdle(config.pool().maxIdle());
            poolConfig.setMinIdle(2);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(false);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
            poolConfig.setMinEvictableIdleTime(Duration.ofMinutes(5));

            switch (config.mode()) {
                case "standalone" -> {
                    pool = new JedisPool(poolConfig,
                        config.standalone().host(),
                        config.standalone().port(),
                        (int) config.timeoutMs(),
                        nullIfEmpty(config.password()),
                        config.database());
                    log.info("Redis standalone pool created: {}:{}",
                        config.standalone().host(), config.standalone().port());
                }
                default -> throw new IllegalArgumentException(
                    "Unsupported Redis mode: " + config.mode());
            }
            return pool;
        }
    }

    public JedisPool createBlockingPool(long maxBlockMs) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.pool().maxTotal());
        poolConfig.setMaxIdle(config.pool().maxIdle());
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));

        int socketTimeout = (int) (config.timeoutMs() + maxBlockMs + 2000);

        switch (config.mode()) {
            case "standalone" -> {
                    this.blockingPool = new JedisPool(poolConfig,
                    config.standalone().host(),
                    config.standalone().port(),
                    (int) config.timeoutMs(),
                    socketTimeout,
                    nullIfEmpty(config.password()),
                    config.database(),
                    null);
                log.info("Redis blocking pool created (socketTimeout={}ms)", socketTimeout);
            }
            default -> throw new IllegalArgumentException(
                "Unsupported Redis mode: " + config.mode());
        }
        return this.blockingPool;
    }

    private static String nullIfEmpty(String value) {
        return (value == null || value.isEmpty()) ? null : value;
    }

    public void close() {
        if (blockingPool != null) {
            blockingPool.close();
        }
        if (pool != null) {
            pool.close();
        }
        log.info("Redis pools closed");
    }
}
