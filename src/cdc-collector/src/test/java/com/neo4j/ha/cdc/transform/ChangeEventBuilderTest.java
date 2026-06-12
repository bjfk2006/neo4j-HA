package com.neo4j.ha.cdc.transform;

import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.common.model.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ChangeEventBuilderTest {

    private DiffCalculator diffCalculator;
    private ChangeEventBuilder builder;

    private static final String DATABASE = "testdb";
    private static final long FENCING_TOKEN = 42L;

    @BeforeEach
    void setUp() {
        diffCalculator = new DiffCalculator(100);
        builder = new ChangeEventBuilder(diffCalculator, DATABASE, FENCING_TOKEN);
    }

    @Test
    void buildsNodeCreatedEvent() {
        RawChange raw = new RawChange(
                ChangeEventType.NODE_CREATED,
                "node-1",
                List.of("Person"),
                Map.of("name", "Alice"),
                null,
                null,
                null,
                null,
                1000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        assertEquals(1, events.size());
        ChangeEvent event = events.get(0);
        assertNotNull(event.eventId());
        assertEquals(ChangeEventType.NODE_CREATED, event.eventType());
        assertEquals(DATABASE, event.database());
        assertEquals(1000L, event.timestamp());
        assertEquals(FENCING_TOKEN, event.fencingToken());

        EntityData entity = event.entity();
        assertEquals(EntityType.NODE, entity.type());
        assertEquals("node-1", entity.elementId());
        assertEquals(List.of("Person"), entity.labels());
        assertEquals(Map.of("name", "Alice"), entity.properties());
        assertNull(entity.beforeState(), "NODE_CREATED should not have beforeState in the event");
    }

    @Test
    void buildsNodeDeletedEvent() {
        // Seed the cache so there is something to evict
        diffCalculator.computeDiff("node-1", Map.of("name", "Alice"));

        RawChange raw = new RawChange(
                ChangeEventType.NODE_DELETED,
                "node-1",
                List.of("Person"),
                Map.of(),
                null,
                null,
                null,
                null,
                2000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        assertEquals(1, events.size());
        ChangeEvent event = events.get(0);
        assertEquals(ChangeEventType.NODE_DELETED, event.eventType());
        assertEquals(EntityType.NODE, event.entity().type());
        assertNull(event.entity().beforeState(), "NODE_DELETED should not populate beforeState");

        // Cache entry should be evicted
        assertEquals(0, diffCalculator.size());
    }

    @Test
    void includesDiffWhenPreviousStateExistsInCache() {
        // Seed the cache with initial properties
        diffCalculator.computeDiff("node-1", Map.of("name", "Alice", "age", 30));

        RawChange raw = new RawChange(
                ChangeEventType.NODE_UPDATED,
                "node-1",
                List.of("Person"),
                Map.of("name", "Bob", "age", 31),
                null,
                null,
                null,
                null,
                3000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        assertEquals(1, events.size());
        ChangeEvent event = events.get(0);
        assertEquals(ChangeEventType.NODE_UPDATED, event.eventType());

        Map<String, Object> before = event.entity().beforeState();
        assertNotNull(before, "NODE_UPDATED should include beforeState when cache has previous");
        assertEquals("Alice", before.get("name"));
        assertEquals(30, before.get("age"));
    }

    @Test
    void nodeUpdatedWithNoPreviousCacheReturnsNullBeforeState() {
        // No prior cache entry for node-1
        RawChange raw = new RawChange(
                ChangeEventType.NODE_UPDATED,
                "node-1",
                List.of("Person"),
                Map.of("name", "Bob"),
                null,
                null,
                null,
                null,
                3000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        assertEquals(1, events.size());
        assertNull(events.get(0).entity().beforeState(),
                "First update should have null beforeState when no cache entry exists");
    }

    @Test
    void correctFencingTokenAndDatabaseInOutput() {
        RawChange raw = new RawChange(
                ChangeEventType.NODE_CREATED,
                "node-5",
                List.of("Label"),
                Map.of(),
                null,
                null,
                null,
                null,
                5000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        ChangeEvent event = events.get(0);
        assertEquals(DATABASE, event.database());
        assertEquals(FENCING_TOKEN, event.fencingToken());
    }

    @Test
    void buildsRelationshipCreatedEvent() {
        RawChange raw = new RawChange(
                ChangeEventType.RELATIONSHIP_CREATED,
                "rel-1",
                List.of(),
                Map.of("since", 2020),
                null,
                "KNOWS",
                "node-1",
                "node-2",
                4000L
        );

        List<ChangeEvent> events = builder.build(List.of(raw));

        assertEquals(1, events.size());
        ChangeEvent event = events.get(0);
        assertEquals(ChangeEventType.RELATIONSHIP_CREATED, event.eventType());
        assertEquals(EntityType.RELATIONSHIP, event.entity().type());
        assertEquals("node-1", event.entity().startNodeElementId());
        assertEquals("node-2", event.entity().endNodeElementId());
        assertEquals("KNOWS", event.entity().relationshipType());
    }

    @Test
    void buildMultipleRawChanges() {
        RawChange create = new RawChange(
                ChangeEventType.NODE_CREATED, "n1", List.of("A"), Map.of("x", 1),
                null, null, null, null, 100L);
        RawChange update = new RawChange(
                ChangeEventType.NODE_UPDATED, "n1", List.of("A"), Map.of("x", 2),
                null, null, null, null, 200L);

        List<ChangeEvent> events = builder.build(List.of(create, update));

        assertEquals(2, events.size());
        assertEquals(ChangeEventType.NODE_CREATED, events.get(0).eventType());
        assertEquals(ChangeEventType.NODE_UPDATED, events.get(1).eventType());

        // The update should have beforeState from the create
        Map<String, Object> before = events.get(1).entity().beforeState();
        assertNotNull(before);
        assertEquals(1, before.get("x"));
    }
}
