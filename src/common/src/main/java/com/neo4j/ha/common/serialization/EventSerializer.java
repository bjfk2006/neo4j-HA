package com.neo4j.ha.common.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.FullSyncBatch;

import java.util.HashMap;
import java.util.Map;

public class EventSerializer {

    private final ObjectMapper objectMapper;

    public EventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public EventSerializer() {
        this(new ObjectMapper());
    }

    public String toJson(ChangeEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize ChangeEvent", e);
        }
    }

    public Map<String, String> toMap(ChangeEvent event) {
        Map<String, String> map = new HashMap<>();
        map.put("eventId", event.eventId());
        map.put("eventType", event.eventType().name());
        map.put("database", event.database());
        map.put("timestamp", String.valueOf(event.timestamp()));
        map.put("fencingToken", String.valueOf(event.fencingToken()));
        map.put("txId", event.txId() != null ? event.txId() : "");
        try {
            map.put("entity", objectMapper.writeValueAsString(event.entity()));
            if (event.metadata() != null) {
                map.put("metadata", objectMapper.writeValueAsString(event.metadata()));
            }
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize ChangeEvent fields", e);
        }
        return map;
    }

    public Map<String, String> fullSyncBatchToMap(FullSyncBatch batch) {
        Map<String, String> map = new HashMap<>();
        map.put("batchId", batch.batchId());
        map.put("batchIndex", String.valueOf(batch.batchIndex()));
        map.put("totalBatches", String.valueOf(batch.totalBatches()));
        map.put("entityType", batch.entityType().name());
        map.put("timestamp", String.valueOf(batch.timestamp()));
        try {
            map.put("entities", objectMapper.writeValueAsString(batch.entities()));
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize FullSyncBatch", e);
        }
        return map;
    }

    public static class SerializationException extends RuntimeException {
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
