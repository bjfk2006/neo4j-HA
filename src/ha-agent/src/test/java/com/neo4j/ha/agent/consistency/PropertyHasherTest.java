package com.neo4j.ha.agent.consistency;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PropertyHasherTest {

    @Test
    void identicalMapsHashEqual() {
        Map<String, Object> a = Map.of("name", "Alice", "age", 30);
        Map<String, Object> b = Map.of("age", 30, "name", "Alice");  // diff order
        assertEquals(PropertyHasher.hash(a), PropertyHasher.hash(b),
            "Hash must be insertion-order independent");
    }

    @Test
    void differentValuesProduceDifferentHashes() {
        Map<String, Object> a = Map.of("name", "Alice", "age", 30);
        Map<String, Object> b = Map.of("name", "Alice", "age", 31);
        assertNotEquals(PropertyHasher.hash(a), PropertyHasher.hash(b));
    }

    @Test
    void haInternalFieldsExcluded() {
        // _updated_at, _elementId, _created_at, _labels must not affect hash.
        Map<String, Object> withInternals = new LinkedHashMap<>();
        withInternals.put("name", "Alice");
        withInternals.put("_updated_at", 1715731190000L);
        withInternals.put("_elementId", "4:abc:1234");
        withInternals.put("_created_at", 1715731000000L);
        withInternals.put("_labels", "[\"Person\"]");

        Map<String, Object> plain = Map.of("name", "Alice");

        assertEquals(PropertyHasher.hash(plain), PropertyHasher.hash(withInternals),
            "_updated_at / _elementId / _created_at / _labels must be ignored");
    }

    @Test
    void nullValuesIgnored() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("name", "Alice");
        a.put("nickname", null);
        Map<String, Object> b = Map.of("name", "Alice");
        assertEquals(PropertyHasher.hash(a), PropertyHasher.hash(b));
    }

    @Test
    void emptyAndNullProduceSameHash() {
        assertEquals(PropertyHasher.hash(null), PropertyHasher.hash(Map.of()));
    }

    @Test
    void temporalTypesStableAcrossInstances() {
        // ZonedDateTime is a JSR-310 type. With JavaTimeModule registered,
        // Jackson produces a deterministic string (epoch millis by default).
        var dt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1_715_000_000_000L), ZoneOffset.UTC);
        Map<String, Object> a = Map.of("createdAt", dt);
        Map<String, Object> b = Map.of("createdAt", dt);
        assertEquals(PropertyHasher.hash(a), PropertyHasher.hash(b),
            "Two identical ZonedDateTime instances must hash the same — proves "
          + "JavaTimeModule registered (otherwise fallback uses memory-address toString)");
    }

    @Test
    void listsCompareStructurally() {
        Map<String, Object> a = Map.of("tags", List.of("a", "b", "c"));
        Map<String, Object> b = Map.of("tags", List.of("a", "b", "c"));
        assertEquals(PropertyHasher.hash(a), PropertyHasher.hash(b));

        Map<String, Object> c = Map.of("tags", List.of("a", "b", "d"));
        assertNotEquals(PropertyHasher.hash(a), PropertyHasher.hash(c));
    }
}
