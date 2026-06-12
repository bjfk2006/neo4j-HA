package com.neo4j.ha.common.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.common.model.*;

import java.util.List;
import java.util.Map;

public class EventDeserializer {

    private final ObjectMapper objectMapper;

    public EventDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public EventDeserializer() {
        this(new ObjectMapper());
    }

    public ChangeEvent fromJson(String json) {
        try {
            return objectMapper.readValue(json, ChangeEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventSerializer.SerializationException("Failed to deserialize ChangeEvent", e);
        }
    }

    public ChangeEvent fromMap(Map<String, String> map) {
        try {
            EntityData entity = objectMapper.readValue(
                map.getOrDefault("entity", "{}"), EntityData.class);
            EventMetadata metadata = map.containsKey("metadata")
                ? objectMapper.readValue(map.get("metadata"), EventMetadata.class)
                : null;

            return new ChangeEvent(
                map.get("eventId"),
                ChangeEventType.valueOf(map.get("eventType")),
                map.get("database"),
                Long.parseLong(map.getOrDefault("timestamp", "0")),
                Long.parseLong(map.getOrDefault("fencingToken", "0")),
                map.getOrDefault("txId", ""),
                entity,
                metadata
            );
        } catch (JsonProcessingException e) {
            throw new EventSerializer.SerializationException("Failed to deserialize ChangeEvent from map", e);
        }
    }

    public FullSyncBatch fullSyncBatchFromMap(Map<String, String> map) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> entities = (List<Map<String, Object>>) (Object) objectMapper.readValue(
                map.getOrDefault("entities", "[]"),
                objectMapper.getTypeFactory().constructCollectionType(
                    java.util.List.class,
                    objectMapper.getTypeFactory().constructMapType(
                        java.util.Map.class, String.class, Object.class)));

            return new FullSyncBatch(
                map.get("batchId"),
                Integer.parseInt(map.getOrDefault("batchIndex", "0")),
                Integer.parseInt(map.getOrDefault("totalBatches", "0")),
                EntityType.valueOf(map.get("entityType")),
                entities,
                Long.parseLong(map.getOrDefault("timestamp", "0"))
            );
        } catch (JsonProcessingException e) {
            throw new EventSerializer.SerializationException("Failed to deserialize FullSyncBatch", e);
        }
    }
}
