package com.neo4j.ha.sync.applier;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which (nodeKey, label) pairs already had their _elementId index created.
 *
 * C2 fix: the previous implementation used a single global Set, which caused
 * multi-standby deployments to skip index creation on the 2nd+ standby (the first
 * standby's success would mark the label as "done" and short-circuit other nodes).
 * The cache key now includes a nodeKey so each standby database is tracked
 * independently.
 */
public class IndexManager {

    private static final Logger log = LoggerFactory.getLogger(IndexManager.class);

    // Keys are "<nodeKey>|<label>" or "<nodeKey>|rel:<relType>".
    private final Set<String> indexedKeys = ConcurrentHashMap.newKeySet();

    public void ensureIndex(Session session, String nodeKey, String label) {
        if (label == null || label.isBlank()) return;
        // Skip internal/system labels (anything starting with `_`). These include
        // `_CDCDeleteEvent`, `_TriggerReadinessProbe`, etc. — they should never
        // carry application data and indexing them is pure overhead.
        if (label.startsWith("_")) return;
        String sanitized = sanitizeLabel(label);
        String cacheKey = cacheKey(nodeKey, sanitized, false);
        if (indexedKeys.contains(cacheKey)) return;

        try {
            session.run(
                "CREATE RANGE INDEX IF NOT EXISTS FOR (n:%s) ON (n._elementId)".formatted(sanitized)
            ).consume();
            indexedKeys.add(cacheKey);
            log.info("Ensured _elementId index on node {} for label: {}", nodeKey, sanitized);
        } catch (Exception e) {
            log.warn("Failed to create index on node {} for label {}: {}", nodeKey, sanitized, e.getMessage());
        }
    }

    public void ensureRelIndex(Session session, String nodeKey, String relType) {
        if (relType == null || relType.isBlank()) return;
        String sanitized = sanitizeLabel(relType);
        String cacheKey = cacheKey(nodeKey, sanitized, true);
        if (indexedKeys.contains(cacheKey)) return;

        try {
            session.run(
                "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[r:%s]-() ON (r._elementId)".formatted(sanitized)
            ).consume();
            indexedKeys.add(cacheKey);
            log.info("Ensured _elementId index on node {} for relationship type: {}", nodeKey, sanitized);
        } catch (Exception e) {
            log.warn("Failed to create rel index on node {} for type {}: {}", nodeKey, sanitized, e.getMessage());
        }
    }

    public void ensureIndexesForAllLabels(Session session, String nodeKey) {
        var result = session.run("CALL db.labels() YIELD label RETURN label");
        for (Record record : result.list()) {
            String label = record.get("label").asString();
            if (!label.startsWith("_")) {
                ensureIndex(session, nodeKey, label);
            }
        }
    }

    public static String sanitizeLabel(String label) {
        // Backtick-escape labels that contain special characters
        if (label.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            return label;
        }
        return "`" + label.replace("`", "``") + "`";
    }

    private static String cacheKey(String nodeKey, String sanitized, boolean isRel) {
        String key = nodeKey == null ? "" : nodeKey;
        return key + "|" + (isRel ? "rel:" : "") + sanitized;
    }
}
