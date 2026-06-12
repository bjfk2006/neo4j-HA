package com.neo4j.ha.common.util;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorTest {

    @Test
    void generatedIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            ids.add(IdGenerator.uuidV7());
        }
        assertEquals(count, ids.size(), "All generated UUIDs should be unique");
    }

    @Test
    void generatedIdsAreTimeOrdered() throws InterruptedException {
        String first = IdGenerator.uuidV7();
        Thread.sleep(2); // small gap to ensure distinct timestamps
        String second = IdGenerator.uuidV7();

        // UUID v7 is time-ordered so lexicographic comparison on the string should hold
        // because the most significant bits encode the timestamp
        assertTrue(first.compareTo(second) < 0,
                "Earlier UUID should sort before later UUID: " + first + " vs " + second);
    }

    @Test
    void generatedIdIsValidUuid() {
        String id = IdGenerator.uuidV7();
        assertDoesNotThrow(() -> UUID.fromString(id));

        UUID uuid = UUID.fromString(id);
        // version should be 7
        assertEquals(7, uuid.version(), "UUID version should be 7");
        // variant should be 2 (RFC 4122)
        assertEquals(2, uuid.variant(), "UUID variant should be 2 (RFC 4122)");
    }

    @Test
    void shortIdHasEightCharacters() {
        String shortId = IdGenerator.shortId();
        assertNotNull(shortId);
        assertEquals(8, shortId.length());
    }

    @Test
    void shortIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        int count = 500;
        for (int i = 0; i < count; i++) {
            ids.add(IdGenerator.shortId());
        }
        assertEquals(count, ids.size());
    }
}
