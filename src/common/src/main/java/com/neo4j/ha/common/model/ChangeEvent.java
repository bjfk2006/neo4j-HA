package com.neo4j.ha.common.model;

public record ChangeEvent(
    String eventId,
    ChangeEventType eventType,
    String database,
    long timestamp,
    long fencingToken,
    String txId,
    EntityData entity,
    EventMetadata metadata
) {}
