package com.neo4j.ha.common.redis;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.FullSyncBatch;
import com.neo4j.ha.common.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.XAddParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamPublisher {

    private static final Logger log = LoggerFactory.getLogger(StreamPublisher.class);

    private static final String FENCING_CHECK_AND_PUBLISH_LUA = """
        local currentToken = redis.call('GET', KEYS[2])
        if currentToken and tonumber(currentToken) > tonumber(ARGV[1]) then
            return 0
        end
        redis.call('XADD', KEYS[1], ARGV[2], unpack(ARGV, 3))
        return 1
        """;

    private final JedisPool jedisPool;
    private final EventSerializer serializer;
    private final long maxLen;
    private final String fencingTokenKey;

    public StreamPublisher(JedisPool jedisPool, EventSerializer serializer,
                           long maxLen, String fencingTokenKey) {
        this.jedisPool = jedisPool;
        this.serializer = serializer;
        this.maxLen = maxLen;
        this.fencingTokenKey = fencingTokenKey;
    }

    public void publish(String streamKey, ChangeEvent event, long fencingToken) {
        Map<String, String> fields = serializer.toMap(event);
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(
                FENCING_CHECK_AND_PUBLISH_LUA,
                List.of(streamKey, fencingTokenKey),
                buildLuaArgs(fencingToken, "*", fields)
            );
            if (Long.valueOf(0).equals(result)) {
                log.warn("Fencing token rejected: current token is newer than {}", fencingToken);
                throw new FencingTokenRejectedException(fencingToken);
            }
        }
    }

    /**
     * Publishes a batch of events using Pipeline for throughput.
     * Returns the last Stream message ID from the batch, or null if events is empty.
     */
    public String publishBatch(String streamKey, List<ChangeEvent> events, long fencingToken) {
        if (events.isEmpty()) return null;

        try (Jedis jedis = jedisPool.getResource()) {
            String currentToken = jedis.get(fencingTokenKey);
            if (currentToken != null && Long.parseLong(currentToken) > fencingToken) {
                throw new FencingTokenRejectedException(fencingToken);
            }

            Pipeline pipeline = jedis.pipelined();
            redis.clients.jedis.Response<redis.clients.jedis.StreamEntryID> lastResponse = null;
            for (ChangeEvent event : events) {
                Map<String, String> fields = serializer.toMap(event);
                lastResponse = pipeline.xadd(streamKey,
                    XAddParams.xAddParams().maxLen(maxLen).approximateTrimming(),
                    fields);
            }
            pipeline.sync();
            log.debug("Published {} events to {}", events.size(), streamKey);
            if (lastResponse != null && lastResponse.get() != null) {
                return lastResponse.get().toString();
            }
            return null;
        }
    }

    public void publishFullSyncBatch(String streamKey, FullSyncBatch batch) {
        Map<String, String> fields = serializer.fullSyncBatchToMap(batch);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.xadd(streamKey,
                XAddParams.xAddParams().maxLen(maxLen).approximateTrimming(),
                fields);
        }
    }

    private List<String> buildLuaArgs(long fencingToken, String streamId,
                                       Map<String, String> fields) {
        List<String> args = new java.util.ArrayList<>();
        args.add(String.valueOf(fencingToken));
        args.add(streamId);
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            args.add(entry.getKey());
            args.add(entry.getValue());
        }
        return args;
    }

    public static class FencingTokenRejectedException extends RuntimeException {
        private final long rejectedToken;

        public FencingTokenRejectedException(long rejectedToken) {
            super("Fencing token " + rejectedToken + " was rejected (stale)");
            this.rejectedToken = rejectedToken;
        }

        public long getRejectedToken() {
            return rejectedToken;
        }
    }
}
