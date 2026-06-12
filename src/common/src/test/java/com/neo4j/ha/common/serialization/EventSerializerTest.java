package com.neo4j.ha.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.common.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventSerializerTest {

    private EventSerializer serializer;
    private EventDeserializer deserializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        serializer = new EventSerializer(objectMapper);
        deserializer = new EventDeserializer(objectMapper);
    }

    private ChangeEvent sampleChangeEvent() {
        EntityData entity = new EntityData(
                EntityType.NODE,
                "4:abc:123",
                List.of("Person"),
                Map.of("name", "Alice", "age", 30),
                null,
                null,
                null,
                null
        );
        EventMetadata metadata = new EventMetadata(0, 1, "corr-001");
        return new ChangeEvent(
                "evt-001",
                ChangeEventType.NODE_CREATED,
                "neo4j",
                System.currentTimeMillis(),
                42L,
                "tx-100",
                entity,
                metadata
        );
    }

    @Test
    void serializeToJsonAndBack() {
        ChangeEvent original = sampleChangeEvent();

        String json = serializer.toJson(original);
        assertNotNull(json);
        assertFalse(json.isBlank());

        ChangeEvent restored = deserializer.fromJson(json);

        assertEquals(original.eventId(), restored.eventId());
        assertEquals(original.eventType(), restored.eventType());
        assertEquals(original.database(), restored.database());
        assertEquals(original.timestamp(), restored.timestamp());
        assertEquals(original.fencingToken(), restored.fencingToken());
        assertEquals(original.txId(), restored.txId());
        assertEquals(original.entity().type(), restored.entity().type());
        assertEquals(original.entity().elementId(), restored.entity().elementId());
        assertEquals(original.entity().labels(), restored.entity().labels());
        assertEquals(original.metadata().correlationId(), restored.metadata().correlationId());
    }

    @Test
    void serializeToMapAndBackViaDeserializer() {
        ChangeEvent original = sampleChangeEvent();

        Map<String, String> map = serializer.toMap(original);
        assertNotNull(map);
        assertEquals("evt-001", map.get("eventId"));
        assertEquals("NODE_CREATED", map.get("eventType"));
        assertEquals("neo4j", map.get("database"));
        assertEquals("tx-100", map.get("txId"));

        ChangeEvent restored = deserializer.fromMap(map);

        assertEquals(original.eventId(), restored.eventId());
        assertEquals(original.eventType(), restored.eventType());
        assertEquals(original.database(), restored.database());
        assertEquals(original.timestamp(), restored.timestamp());
        assertEquals(original.fencingToken(), restored.fencingToken());
        assertEquals(original.entity().elementId(), restored.entity().elementId());
    }

    @Test
    void serializeChangeEventWithNullMetadata() {
        EntityData entity = new EntityData(
                EntityType.NODE, "4:abc:456", List.of("Item"),
                Map.of("key", "value"), null, null, null, null
        );
        ChangeEvent event = new ChangeEvent(
                "evt-002", ChangeEventType.NODE_UPDATED, "neo4j",
                1000L, 1L, null, entity, null
        );

        Map<String, String> map = serializer.toMap(event);
        assertFalse(map.containsKey("metadata"));

        ChangeEvent restored = deserializer.fromMap(map);
        assertNull(restored.metadata());
        assertEquals("evt-002", restored.eventId());
    }

    @Test
    void fullSyncBatchSerializationRoundtrip() {
        List<Map<String, Object>> entities = List.of(
                Map.of("elementId", "4:abc:1", "labels", List.of("Person"), "name", "Bob"),
                Map.of("elementId", "4:abc:2", "labels", List.of("Person"), "name", "Carol")
        );
        FullSyncBatch batch = new FullSyncBatch(
                "batch-001", 0, 3, EntityType.NODE, entities, System.currentTimeMillis()
        );

        Map<String, String> map = serializer.fullSyncBatchToMap(batch);
        assertNotNull(map);
        assertEquals("batch-001", map.get("batchId"));
        assertEquals("0", map.get("batchIndex"));
        assertEquals("3", map.get("totalBatches"));
        assertEquals("NODE", map.get("entityType"));

        FullSyncBatch restored = deserializer.fullSyncBatchFromMap(map);
        assertEquals(batch.batchId(), restored.batchId());
        assertEquals(batch.batchIndex(), restored.batchIndex());
        assertEquals(batch.totalBatches(), restored.totalBatches());
        assertEquals(batch.entityType(), restored.entityType());
        assertEquals(batch.timestamp(), restored.timestamp());
        assertEquals(2, restored.entities().size());
    }

    @Test
    void defaultConstructorsWork() {
        EventSerializer defaultSer = new EventSerializer();
        EventDeserializer defaultDes = new EventDeserializer();

        ChangeEvent event = sampleChangeEvent();
        String json = defaultSer.toJson(event);
        ChangeEvent restored = defaultDes.fromJson(json);
        assertEquals(event.eventId(), restored.eventId());
    }
}
