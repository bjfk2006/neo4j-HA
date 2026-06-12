package com.neo4j.ha.common.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DistributedLock {

    private static final Logger log = LoggerFactory.getLogger(DistributedLock.class);

    private static final String RENEW_LUA = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        else
            return 0
        end
        """;

    private static final String RELEASE_LUA = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """;

    private final JedisPool jedisPool;

    public DistributedLock(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public Optional<LockHandle> tryAcquire(String lockKey, String ownerId, Duration ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(lockKey, ownerId,
                SetParams.setParams().nx().px(ttl.toMillis()));
            if ("OK".equals(result)) {
                log.debug("Lock acquired: key={}, owner={}", lockKey, ownerId);
                return Optional.of(new LockHandle(lockKey, ownerId));
            }
            return Optional.empty();
        }
    }

    public boolean renew(LockHandle handle, Duration ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(RENEW_LUA,
                List.of(handle.lockKey()),
                List.of(handle.ownerId(), String.valueOf(ttl.toMillis())));
            return Long.valueOf(1).equals(result);
        }
    }

    public boolean release(LockHandle handle) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(RELEASE_LUA,
                List.of(handle.lockKey()),
                List.of(handle.ownerId()));
            boolean released = Long.valueOf(1).equals(result);
            if (released) {
                log.debug("Lock released: key={}, owner={}", handle.lockKey(), handle.ownerId());
            }
            return released;
        }
    }

    public Optional<String> currentOwner(String lockKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            String owner = jedis.get(lockKey);
            return Optional.ofNullable(owner);
        }
    }

    public record LockHandle(String lockKey, String ownerId) {}
}
