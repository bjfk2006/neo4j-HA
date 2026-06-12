package com.neo4j.ha.cdc.transform;

import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.common.model.*;
import com.neo4j.ha.common.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChangeEventBuilder {

    private final DiffCalculator diffCalculator;
    private final String database;
    private final long fencingToken;

    public ChangeEventBuilder(DiffCalculator diffCalculator, String database, long fencingToken) {
        this.diffCalculator = diffCalculator;
        this.database = database;
        this.fencingToken = fencingToken;
    }

    public void setFencingToken(long newToken) {
        // Fencing token is set at construction; create new builder for new token
    }

    public List<ChangeEvent> build(List<RawChange> rawChanges) {
        List<ChangeEvent> events = new ArrayList<>(rawChanges.size());
        for (RawChange raw : rawChanges) {
            events.add(toChangeEvent(raw));
        }
        return events;
    }

    private ChangeEvent toChangeEvent(RawChange raw) {
        Map<String, Object> beforeState = null;
        if (raw.type() == ChangeEventType.NODE_UPDATED
            || raw.type() == ChangeEventType.RELATIONSHIP_UPDATED) {
            beforeState = diffCalculator.computeDiff(raw.elementId(), raw.properties());
        } else if (raw.type() == ChangeEventType.NODE_CREATED
            || raw.type() == ChangeEventType.RELATIONSHIP_CREATED) {
            diffCalculator.computeDiff(raw.elementId(), raw.properties());
        } else if (raw.type() == ChangeEventType.NODE_DELETED
            || raw.type() == ChangeEventType.RELATIONSHIP_DELETED) {
            diffCalculator.evict(raw.elementId());
        }

        EntityType entityType = switch (raw.type()) {
            case NODE_CREATED, NODE_UPDATED, NODE_DELETED -> EntityType.NODE;
            case RELATIONSHIP_CREATED, RELATIONSHIP_UPDATED, RELATIONSHIP_DELETED -> EntityType.RELATIONSHIP;
            default -> EntityType.NODE;
        };

        EntityData entity = new EntityData(
            entityType,
            raw.elementId(),
            raw.labels(),
            raw.properties(),
            beforeState,
            raw.startNodeEid(),
            raw.endNodeEid(),
            raw.relType(),
            raw.startNodeLabels(),
            raw.endNodeLabels()
        );

        return new ChangeEvent(
            IdGenerator.uuidV7(),
            raw.type(),
            database,
            raw.timestamp(),
            fencingToken,
            null,
            entity,
            null
        );
    }
}
