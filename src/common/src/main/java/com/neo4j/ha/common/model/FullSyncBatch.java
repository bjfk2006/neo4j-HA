package com.neo4j.ha.common.model;

import java.util.List;
import java.util.Map;

public record FullSyncBatch(
    String batchId,
    int batchIndex,
    int totalBatches,
    EntityType entityType,
    List<Map<String, Object>> entities,
    long timestamp
) {}
