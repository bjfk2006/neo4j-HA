package com.neo4j.ha.common.model;

import java.util.Map;

public record ControlCommand(
    String commandId,
    ChangeEventType commandType,
    String targetNodeId,
    long fencingToken,
    long timestamp,
    Map<String, Object> params
) {}
