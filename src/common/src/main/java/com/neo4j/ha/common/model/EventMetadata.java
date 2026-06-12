package com.neo4j.ha.common.model;

public record EventMetadata(
    int batchIndex,
    int batchTotal,
    String correlationId
) {}
