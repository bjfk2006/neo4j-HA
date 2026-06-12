package com.neo4j.ha.common.model;

import java.util.List;
import java.util.Map;

public record EntityData(
    EntityType type,
    String elementId,
    List<String> labels,
    Map<String, Object> properties,
    Map<String, Object> beforeState,
    String startNodeElementId,
    String endNodeElementId,
    String relationshipType,
    List<String> startNodeLabels,
    List<String> endNodeLabels
) {
    public EntityData(EntityType type, String elementId, List<String> labels,
                      Map<String, Object> properties, Map<String, Object> beforeState,
                      String startNodeElementId, String endNodeElementId,
                      String relationshipType) {
        this(type, elementId, labels, properties, beforeState,
             startNodeElementId, endNodeElementId, relationshipType, null, null);
    }
}
