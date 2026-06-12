package com.neo4j.ha.common.model;

public record FailoverEvent(
    String eventId,
    String failedNodeId,
    String newPrimaryId,
    long fencingToken,
    long startTime,
    long endTime,
    String result,
    String reason
) {}
