package com.neo4j.ha.cdc;

import com.neo4j.ha.common.config.HaConfig;

public record CdcCollectorConfig(
    long pollIntervalMs,
    int batchSize,
    long pollTimeoutMs,
    String timestampField,
    String createdAtField,
    String elementIdField,
    int cacheMaxSize,
    String changesStreamKey,
    String fullsyncStreamKey,
    long streamMaxLen,
    String fencingTokenKey,
    int fullsyncBatchSize,
    int fullsyncThrottleMs,
    String bufferDir,
    int bufferMaxFiles
) {
    public static CdcCollectorConfig from(HaConfig config) {
        return new CdcCollectorConfig(
            config.cdc().poll().intervalMs(),
            config.cdc().poll().batchSize(),
            config.cdc().poll().timeoutMs(),
            config.cdc().timestampField(),
            config.cdc().createdAtField(),
            config.cdc().elementIdField(),
            config.cdc().cache().maxSize(),
            config.stream().changes(),
            config.stream().fullsync(),
            config.stream().maxLen(),
            config.failover().fencingToken().key(),
            config.fullsync().batchSize(),
            config.fullsync().throttleMs(),
            config.buffer().dir(),
            config.buffer().maxFiles()
        );
    }
}
