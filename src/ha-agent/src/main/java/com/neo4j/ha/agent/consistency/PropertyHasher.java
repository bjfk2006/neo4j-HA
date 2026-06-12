package com.neo4j.ha.agent.consistency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stable, order-independent fingerprint over a Neo4j entity's property map.
 *
 * <p>Canonical-JSON requirements:
 * <ul>
 *   <li>Keys sorted alphabetically (TreeMap)</li>
 *   <li>Null values omitted</li>
 *   <li>Internal HA fields ({@code _elementId}, {@code _updated_at},
 *       {@code _created_at}, {@code _labels}) excluded — they intentionally
 *       differ across nodes (esp. {@code _updated_at} on standbys vs primary)
 *       and would always trigger false-positive diffs.</li>
 * </ul>
 *
 * <p>Hash is hex-encoded SHA-256 (64 chars). Collisions are infeasible in
 * the input space we care about.</p>
 */
public final class PropertyHasher {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    /** HA-internal fields that must NOT participate in the hash. */
    private static final java.util.Set<String> EXCLUDED_FIELDS = java.util.Set.of(
        "_elementId", "_updated_at", "_created_at", "_labels"
    );

    private PropertyHasher() {}

    public static String hash(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) return EMPTY_HASH;
        TreeMap<String, Object> canonical = new TreeMap<>();
        for (var e : properties.entrySet()) {
            if (e.getValue() == null) continue;
            if (EXCLUDED_FIELDS.contains(e.getKey())) continue;
            canonical.put(e.getKey(), e.getValue());
        }
        if (canonical.isEmpty()) return EMPTY_HASH;
        try {
            String json = MAPPER.writeValueAsString(canonical);
            return sha256Hex(json);
        } catch (JsonProcessingException e) {
            // Fallback: a deterministic toString (slower but never throws).
            return sha256Hex(canonical.toString());
        }
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable on this JVM", e);
        }
    }

    /** SHA-256 of an empty JSON object {} — used as the "no properties" sentinel. */
    private static final String EMPTY_HASH =
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
}
