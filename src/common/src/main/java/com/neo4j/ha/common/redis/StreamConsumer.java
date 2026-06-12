package com.neo4j.ha.common.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

    private final JedisPool jedisPool;
    private final JedisPool blockingPool;

    public StreamConsumer(JedisPool jedisPool) {
        this(jedisPool, jedisPool);
    }

    public StreamConsumer(JedisPool jedisPool, JedisPool blockingPool) {
        this.jedisPool = jedisPool;
        this.blockingPool = blockingPool;
    }

    public void ensureGroup(String streamKey, String groupName) {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                jedis.xgroupCreate(streamKey, groupName, new StreamEntryID(), true);
                log.info("Consumer group '{}' created on stream '{}'", groupName, streamKey);
            } catch (Exception e) {
                if (!e.getMessage().contains("BUSYGROUP")) {
                    throw e;
                }
                log.debug("Consumer group '{}' already exists on '{}'", groupName, streamKey);
            }
        }
    }

    public List<Map.Entry<String, List<StreamEntry>>> consume(String streamKey, String groupName,
                                                               String consumerName, int count,
                                                               long blockMs) {
        try (Jedis jedis = blockingPool.getResource()) {
            var result = jedis.xreadGroup(
                groupName, consumerName,
                XReadGroupParams.xReadGroupParams().count(count).block((int) blockMs),
                Map.of(streamKey, StreamEntryID.UNRECEIVED_ENTRY)
            );
            if (result == null) {
                return Collections.emptyList();
            }
            return result;
        }
    }

    public void ack(String streamKey, String groupName, String... messageIds) {
        try (Jedis jedis = jedisPool.getResource()) {
            StreamEntryID[] ids = new StreamEntryID[messageIds.length];
            for (int i = 0; i < messageIds.length; i++) {
                ids[i] = new StreamEntryID(messageIds[i]);
            }
            jedis.xack(streamKey, groupName, ids);
        }
    }

    public List<StreamEntry> readPending(String streamKey, String groupName,
                                          String consumerName, int count) {
        try (Jedis jedis = jedisPool.getResource()) {
            var result = jedis.xreadGroup(
                groupName, consumerName,
                XReadGroupParams.xReadGroupParams().count(count),
                Map.of(streamKey, new StreamEntryID("0-0"))
            );
            if (result == null || result.isEmpty()) {
                return Collections.emptyList();
            }
            return result.get(0).getValue();
        }
    }

    public List<StreamEntry> claim(String streamKey, String groupName,
                                    String consumerName, long minIdleMs,
                                    String... messageIds) {
        try (Jedis jedis = jedisPool.getResource()) {
            StreamEntryID[] ids = new StreamEntryID[messageIds.length];
            for (int i = 0; i < messageIds.length; i++) {
                ids[i] = new StreamEntryID(messageIds[i]);
            }
            return jedis.xclaim(streamKey, groupName, consumerName, minIdleMs,
                new redis.clients.jedis.params.XClaimParams(), ids);
        }
    }
}
