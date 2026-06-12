package com.neo4j.ha.cdc.polling;

import com.neo4j.ha.common.model.ChangeEventType;

import java.util.List;
import java.util.Map;

public interface ChangeDetector {

    List<RawChange> detectChanges(PollingState state, int batchSize);

    record RawChange(
        ChangeEventType type,
        String elementId,
        List<String> labels,
        Map<String, Object> properties,
        Map<String, Object> beforeState,
        String relType,
        String startNodeEid,
        String endNodeEid,
        long timestamp,
        List<String> startNodeLabels,
        List<String> endNodeLabels
    ) {
        public RawChange(ChangeEventType type, String elementId, List<String> labels,
                         Map<String, Object> properties, Map<String, Object> beforeState,
                         String relType, String startNodeEid, String endNodeEid, long timestamp) {
            this(type, elementId, labels, properties, beforeState,
                 relType, startNodeEid, endNodeEid, timestamp, null, null);
        }
    }
}
