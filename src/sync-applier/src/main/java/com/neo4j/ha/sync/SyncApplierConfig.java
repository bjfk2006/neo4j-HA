package com.neo4j.ha.sync;

import com.neo4j.ha.common.config.HaConfig;

public record SyncApplierConfig(
    String consumerGroup,
    int consumerBatchSize,
    long blockTimeoutMs,
    String applyMode,
    boolean batchCommit,
    int maxRetries,
    long retryDelayMs,
    int duplicateDetectorMaxSize,
    String changesStreamKey,
    String fullsyncStreamKey,
    long syncLagThreshold,
    long stableDurationMs,
    long checkIntervalMs
) {
    public static SyncApplierConfig from(HaConfig config) {
        return new SyncApplierConfig(
            config.sync().consumer().group(),
            config.sync().consumer().batchSize(),
            config.sync().consumer().blockTimeoutMs(),
            config.sync().apply().mode(),
            config.sync().apply().batchCommit(),
            config.sync().apply().maxRetries(),
            config.sync().apply().retryDelayMs(),
            config.sync().duplicateDetector().maxSize(),
            config.stream().changes(),
            config.stream().fullsync(),
            config.serviceState().syncLagThreshold(),
            config.serviceState().stableDurationMs(),
            config.serviceState().checkIntervalMs()
        );
    }
}
