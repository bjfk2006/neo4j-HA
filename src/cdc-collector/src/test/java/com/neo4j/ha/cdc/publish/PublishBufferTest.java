package com.neo4j.ha.cdc.publish;

import com.neo4j.ha.common.model.*;
import com.neo4j.ha.common.serialization.EventSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PublishBufferTest {

    @TempDir
    Path tempDir;

    private PublishBuffer buffer;
    private EventSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = mock(EventSerializer.class);
        buffer = new PublishBuffer(serializer, tempDir.toString(), 10);
    }

    private ChangeEvent createEvent(String id) {
        EntityData entity = new EntityData(
                EntityType.NODE, id, List.of("Label"),
                Map.of("key", "value"), null, null, null, null);
        return new ChangeEvent(
                id, ChangeEventType.NODE_CREATED, "testdb",
                System.currentTimeMillis(), 1L, null, entity, null);
    }

    @Test
    void addEventsIncreasesSize() {
        assertEquals(0, buffer.size());

        buffer.add(List.of(createEvent("e1"), createEvent("e2")));
        assertEquals(2, buffer.size());

        buffer.add(List.of(createEvent("e3")));
        assertEquals(3, buffer.size());
    }

    @Test
    void drainReturnsEventsInOrder() {
        ChangeEvent e1 = createEvent("e1");
        ChangeEvent e2 = createEvent("e2");
        ChangeEvent e3 = createEvent("e3");

        buffer.add(List.of(e1, e2, e3));

        List<ChangeEvent> drained = buffer.drain(10);

        assertEquals(3, drained.size());
        assertEquals("e1", drained.get(0).eventId());
        assertEquals("e2", drained.get(1).eventId());
        assertEquals("e3", drained.get(2).eventId());
    }

    @Test
    void drainUpToMaxCount() {
        buffer.add(List.of(createEvent("e1"), createEvent("e2"), createEvent("e3")));

        List<ChangeEvent> batch = buffer.drain(2);

        assertEquals(2, batch.size());
        assertEquals(1, buffer.size(), "One event should remain in the buffer");
    }

    @Test
    void drainReturnsEmptyListWhenBufferIsEmpty() {
        List<ChangeEvent> drained = buffer.drain(10);
        assertTrue(drained.isEmpty());
    }

    @Test
    void hasBufferedReturnsFalseWhenEmpty() {
        assertFalse(buffer.hasBuffered());
    }

    @Test
    void hasBufferedReturnsTrueAfterAdd() {
        buffer.add(List.of(createEvent("e1")));
        assertTrue(buffer.hasBuffered());
    }

    @Test
    void hasBufferedReturnsFalseAfterFullDrain() {
        buffer.add(List.of(createEvent("e1")));
        buffer.drain(10);
        assertFalse(buffer.hasBuffered());
    }

    @Test
    void sizeDecreasesAfterDrain() {
        buffer.add(List.of(createEvent("e1"), createEvent("e2"), createEvent("e3")));
        assertEquals(3, buffer.size());

        buffer.drain(2);
        assertEquals(1, buffer.size());

        buffer.drain(1);
        assertEquals(0, buffer.size());
    }

    @Test
    void drainMoreThanAvailableReturnsAllEvents() {
        buffer.add(List.of(createEvent("e1")));

        List<ChangeEvent> drained = buffer.drain(100);

        assertEquals(1, drained.size());
        assertEquals(0, buffer.size());
    }
}
