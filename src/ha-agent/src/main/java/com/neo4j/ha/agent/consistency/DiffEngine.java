package com.neo4j.ha.agent.consistency;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Three-way diff engine: primary's "truth set" of {@code _elementId}s vs
 * standby's local set, with property-hash comparison for entries present on
 * both sides.
 *
 * <p>The algorithm picks the comparison universe from the primary
 * (controlled by {@link Scope}), then asks each standby to look up the same
 * set of elementIds via {@code UNWIND $eids AS eid OPTIONAL MATCH ...}.
 * Diff is computed in Java, never in Cypher (keeps the queries cacheable
 * and small).</p>
 */
public class DiffEngine {

    private static final Logger log = LoggerFactory.getLogger(DiffEngine.class);

    public enum Scope { RECENT, LABEL, RANDOM }
    public enum Kind  { NODE, REL, BOTH }

    private final String database;

    public DiffEngine(String database) { this.database = database; }

    public record DiffEntry(
        String elementId,
        String kind,           // "node" | "rel"
        List<String> labels,
        Map<String, Object> primaryProps,
        Map<String, Object> standbyProps,   // null for missing/extra
        String primaryHash,
        String standbyHash,
        Map<String, Map<String, Object>> delta   // propKey -> {primary, standby}; null for missing/extra
    ) {}

    public record NodeDiff(
        List<DiffEntry> missing,
        List<DiffEntry> extra,
        List<DiffEntry> propDiff,
        int matched,
        String error
    ) {}

    /**
     * @param primaryDriver  driver for the current primary
     * @param standbys       map of nodeId -> driver
     * @param scope          how to pick the comparison universe
     * @param scopeArg       label name (for LABEL scope) or null
     * @param limit          max entries to compare from primary (100..10000)
     * @param kind           node / rel / both
     * @return map of standbyNodeId -> NodeDiff
     */
    public Map<String, NodeDiff> diff(Driver primaryDriver,
                                      Map<String, Driver> standbys,
                                      Scope scope, String scopeArg,
                                      int limit, Kind kind) {
        Map<String, NodeDiff> result = new LinkedHashMap<>();
        if (primaryDriver == null) {
            for (var sid : standbys.keySet())
                result.put(sid, new NodeDiff(List.of(), List.of(), List.of(), 0, "primary driver unavailable"));
            return result;
        }

        // 1) Snapshot the primary's "truth set".
        List<EntitySnapshot> primaryEntities = new ArrayList<>();
        try (Session s = primaryDriver.session(SessionConfig.forDatabase(database))) {
            if (kind == Kind.NODE || kind == Kind.BOTH) {
                primaryEntities.addAll(scanPrimaryNodes(s, scope, scopeArg, limit));
            }
            if (kind == Kind.REL || kind == Kind.BOTH) {
                primaryEntities.addAll(scanPrimaryRels(s, scope, scopeArg, limit));
            }
        } catch (Exception e) {
            log.warn("Diff: primary scan failed: {}", e.toString());
            for (var sid : standbys.keySet())
                result.put(sid, new NodeDiff(List.of(), List.of(), List.of(), 0, "primary scan: " + e.getMessage()));
            return result;
        }

        // 2) Index by elementId for fast lookup during the per-standby diff.
        Map<String, EntitySnapshot> primaryByEid = new HashMap<>();
        for (var e : primaryEntities) primaryByEid.put(e.elementId, e);

        // 3) For each standby, batch-fetch the same elementIds + scan extras.
        for (var entry : standbys.entrySet()) {
            String standbyId = entry.getKey();
            Driver driver = entry.getValue();
            try {
                NodeDiff nd = diffStandby(driver, primaryEntities, primaryByEid, limit, kind);
                result.put(standbyId, nd);
            } catch (Exception e) {
                log.warn("Diff: standby {} scan failed: {}", standbyId, e.toString());
                result.put(standbyId, new NodeDiff(List.of(), List.of(), List.of(), 0,
                    "standby scan: " + e.getMessage()));
            }
        }
        return result;
    }

    private NodeDiff diffStandby(Driver driver, List<EntitySnapshot> primaryEntities,
                                  Map<String, EntitySnapshot> primaryByEid,
                                  int limit, Kind kind) {
        List<String> eids = primaryEntities.stream().map(e -> e.elementId).toList();
        Map<String, EntitySnapshot> standbyByEid = new HashMap<>();

        try (Session s = driver.session(SessionConfig.forDatabase(database))) {
            if (!eids.isEmpty() && (kind == Kind.NODE || kind == Kind.BOTH)) {
                var rows = s.run(
                    "UNWIND $eids AS eid "
                  + "OPTIONAL MATCH (n) WHERE n._elementId = eid "
                  + "RETURN eid, labels(n) AS ls, properties(n) AS p",
                    Map.of("eids", eids)
                ).list();
                for (Record r : rows) {
                    if (r.get("ls").isNull()) continue;
                    String eid = r.get("eid").asString();
                    standbyByEid.put(eid, new EntitySnapshot(
                        eid, "node",
                        r.get("ls").asList(Value::asString),
                        recordToMap(r.get("p"))
                    ));
                }
            }
            if (!eids.isEmpty() && (kind == Kind.REL || kind == Kind.BOTH)) {
                var rows = s.run(
                    "UNWIND $eids AS eid "
                  + "OPTIONAL MATCH ()-[r]->() WHERE r._elementId = eid "
                  + "RETURN eid, type(r) AS t, properties(r) AS p",
                    Map.of("eids", eids)
                ).list();
                for (Record r : rows) {
                    if (r.get("t").isNull()) continue;
                    String eid = r.get("eid").asString();
                    standbyByEid.put(eid, new EntitySnapshot(
                        eid, "rel",
                        List.of(r.get("t").asString()),
                        recordToMap(r.get("p"))
                    ));
                }
            }

            // Scan for standby "extras" — _elementId values present on standby but
            // not in the primary's universe. Cap at limit/10 to avoid full-graph scan.
            int extraCap = Math.max(10, limit / 10);
            List<EntitySnapshot> extras = new ArrayList<>();
            if (kind == Kind.NODE || kind == Kind.BOTH) {
                var rows = s.run(
                    "MATCH (n) WHERE n._elementId IS NOT NULL "
                  + "AND NOT n._elementId IN $known "
                  + "RETURN n._elementId AS eid, labels(n) AS ls, properties(n) AS p "
                  + "ORDER BY coalesce(n._updated_at, 0) DESC LIMIT $cap",
                    Map.of("known", eids, "cap", extraCap)
                ).list();
                for (Record r : rows) {
                    extras.add(new EntitySnapshot(
                        r.get("eid").asString(), "node",
                        r.get("ls").asList(Value::asString),
                        recordToMap(r.get("p"))
                    ));
                }
            }
            if (kind == Kind.REL || kind == Kind.BOTH) {
                var rows = s.run(
                    "MATCH ()-[r]->() WHERE r._elementId IS NOT NULL "
                  + "AND NOT r._elementId IN $known "
                  + "RETURN r._elementId AS eid, type(r) AS t, properties(r) AS p "
                  + "ORDER BY coalesce(r._updated_at, 0) DESC LIMIT $cap",
                    Map.of("known", eids, "cap", extraCap)
                ).list();
                for (Record r : rows) {
                    extras.add(new EntitySnapshot(
                        r.get("eid").asString(), "rel",
                        List.of(r.get("t").asString()),
                        recordToMap(r.get("p"))
                    ));
                }
            }

            // 4) Three-way diff in Java.
            List<DiffEntry> missing = new ArrayList<>();
            List<DiffEntry> propDiff = new ArrayList<>();
            int matched = 0;
            for (EntitySnapshot p : primaryEntities) {
                EntitySnapshot st = standbyByEid.get(p.elementId);
                if (st == null) {
                    missing.add(new DiffEntry(p.elementId, p.kind, p.labels,
                        p.properties, null, PropertyHasher.hash(p.properties), null, null));
                    continue;
                }
                String pHash = PropertyHasher.hash(p.properties);
                String sHash = PropertyHasher.hash(st.properties);
                if (pHash.equals(sHash)) {
                    matched++;
                } else {
                    propDiff.add(new DiffEntry(p.elementId, p.kind, p.labels,
                        p.properties, st.properties, pHash, sHash,
                        computeDelta(p.properties, st.properties)));
                }
            }

            List<DiffEntry> extraOut = new ArrayList<>();
            for (EntitySnapshot e : extras) {
                extraOut.add(new DiffEntry(e.elementId, e.kind, e.labels,
                    null, e.properties, null, PropertyHasher.hash(e.properties), null));
            }
            return new NodeDiff(missing, extraOut, propDiff, matched, null);
        }
    }

    // === primary-side scanners ===

    private List<EntitySnapshot> scanPrimaryNodes(Session s, Scope scope, String labelArg, int limit) {
        String cypher;
        Map<String, Object> params = new HashMap<>();
        params.put("limit", limit);
        switch (scope) {
            case LABEL -> {
                cypher = "MATCH (n) WHERE n._elementId IS NOT NULL AND $label IN labels(n) "
                       + "RETURN n._elementId AS eid, labels(n) AS ls, properties(n) AS p "
                       + "ORDER BY coalesce(n._updated_at, 0) DESC LIMIT $limit";
                params.put("label", labelArg == null ? "" : labelArg);
            }
            case RANDOM -> cypher =
                "MATCH (n) WHERE n._elementId IS NOT NULL "
              + "WITH n, rand() AS r ORDER BY r LIMIT $limit "
              + "RETURN n._elementId AS eid, labels(n) AS ls, properties(n) AS p";
            default -> cypher =
                "MATCH (n) WHERE n._elementId IS NOT NULL "
              + "RETURN n._elementId AS eid, labels(n) AS ls, properties(n) AS p "
              + "ORDER BY coalesce(n._updated_at, 0) DESC LIMIT $limit";
        }
        var rows = s.run(cypher, params).list();
        List<EntitySnapshot> out = new ArrayList<>(rows.size());
        for (Record r : rows) {
            out.add(new EntitySnapshot(
                r.get("eid").asString(), "node",
                r.get("ls").asList(Value::asString),
                recordToMap(r.get("p"))
            ));
        }
        return out;
    }

    private List<EntitySnapshot> scanPrimaryRels(Session s, Scope scope, String labelArg, int limit) {
        String cypher;
        Map<String, Object> params = new HashMap<>();
        params.put("limit", limit);
        switch (scope) {
            case LABEL -> {
                cypher = "MATCH ()-[r]->() WHERE r._elementId IS NOT NULL AND type(r) = $label "
                       + "RETURN r._elementId AS eid, type(r) AS t, properties(r) AS p "
                       + "ORDER BY coalesce(r._updated_at, 0) DESC LIMIT $limit";
                params.put("label", labelArg == null ? "" : labelArg);
            }
            case RANDOM -> cypher =
                "MATCH ()-[r]->() WHERE r._elementId IS NOT NULL "
              + "WITH r, rand() AS x ORDER BY x LIMIT $limit "
              + "RETURN r._elementId AS eid, type(r) AS t, properties(r) AS p";
            default -> cypher =
                "MATCH ()-[r]->() WHERE r._elementId IS NOT NULL "
              + "RETURN r._elementId AS eid, type(r) AS t, properties(r) AS p "
              + "ORDER BY coalesce(r._updated_at, 0) DESC LIMIT $limit";
        }
        var rows = s.run(cypher, params).list();
        List<EntitySnapshot> out = new ArrayList<>(rows.size());
        for (Record r : rows) {
            out.add(new EntitySnapshot(
                r.get("eid").asString(), "rel",
                List.of(r.get("t").asString()),
                recordToMap(r.get("p"))
            ));
        }
        return out;
    }

    private Map<String, Map<String, Object>> computeDelta(Map<String, Object> p, Map<String, Object> s) {
        Set<String> keys = new HashSet<>();
        keys.addAll(p.keySet());
        keys.addAll(s.keySet());
        Map<String, Map<String, Object>> delta = new LinkedHashMap<>();
        for (String k : keys) {
            if (k.startsWith("_") && (k.equals("_updated_at") || k.equals("_created_at")
                || k.equals("_labels") || k.equals("_elementId"))) continue;
            Object pv = p.get(k);
            Object sv = s.get(k);
            if (!java.util.Objects.equals(pv, sv)) {
                Map<String, Object> pair = new LinkedHashMap<>();
                pair.put("primary", pv);
                pair.put("standby", sv);
                delta.put(k, pair);
            }
        }
        return delta;
    }

    /**
     * Convert a Neo4j Value (expected to be a Map / properties bag) into a
     * Java Map whose values can be JSON-serialized by Jackson.
     *
     * <p>Neo4j-specific types (Point, Duration, ZonedDateTime, ...) are
     * normalized to their {@code toString()} representation so the downstream
     * {@link PropertyHasher} produces stable, cross-process-identical hashes.
     * Otherwise these types either throw {@code InvalidDefinitionException}
     * during serialization or, worse, fall back to {@code Object.toString()}
     * which returns the JVM memory address (different on every node → false
     * propDiff for every entry).</p>
     */
    private static Map<String, Object> recordToMap(Value v) {
        if (v == null || v.isNull()) return Map.of();
        Map<String, Object> raw = v.asMap();
        Map<String, Object> out = new HashMap<>(raw.size());
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            out.put(entry.getKey(), normalizeValue(entry.getValue()));
        }
        return out;
    }

    private static Object normalizeValue(Object v) {
        if (v == null) return null;
        // Neo4j spatial / temporal types — toString() is stable across JVMs.
        if (v instanceof org.neo4j.driver.types.Point
                || v instanceof org.neo4j.driver.types.IsoDuration
                || v instanceof java.time.temporal.TemporalAccessor) {
            return v.toString();
        }
        // Lists may contain Neo4j types; recurse.
        if (v instanceof java.util.List<?> list) {
            java.util.List<Object> norm = new java.util.ArrayList<>(list.size());
            for (Object item : list) norm.add(normalizeValue(item));
            return norm;
        }
        // Maps too (nested object properties — uncommon in Neo4j but legal).
        if (v instanceof Map<?, ?> map) {
            Map<String, Object> norm = new HashMap<>(map.size());
            for (var e : map.entrySet()) {
                norm.put(String.valueOf(e.getKey()), normalizeValue(e.getValue()));
            }
            return norm;
        }
        return v;
    }

    private record EntitySnapshot(
        String elementId, String kind, List<String> labels, Map<String, Object> properties
    ) {}
}
