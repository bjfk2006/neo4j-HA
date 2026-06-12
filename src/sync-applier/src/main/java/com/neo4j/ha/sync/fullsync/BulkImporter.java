package com.neo4j.ha.sync.fullsync;

import com.neo4j.ha.common.model.EntityType;
import com.neo4j.ha.common.model.FullSyncBatch;
import com.neo4j.ha.sync.applier.IndexManager;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BulkImporter {

    private static final Logger log = LoggerFactory.getLogger(BulkImporter.class);

    private static final String BULK_NODE_IMPORT = """
        UNWIND $nodes AS node
        CALL {
            WITH node
            MERGE (n {_elementId: node.elementId})
            SET n = node.properties
            SET n._elementId = node.elementId
            WITH n, node
            CALL apoc.create.addLabels(n, node.labels) YIELD node AS labeled
            RETURN labeled
        }
        """;

    private static final String BULK_REL_IMPORT = """
        UNWIND $rels AS rel
        CALL {
            WITH rel
            MATCH (a {_elementId: rel.startNodeId})
            MATCH (b {_elementId: rel.endNodeId})
            CALL apoc.merge.relationship(a, rel.relType, {_elementId: rel.elementId}, rel.properties, b, {}) YIELD rel AS r
            RETURN r
        }
        """;

    // Fallback without APOC for nodes
    private static final String BULK_NODE_IMPORT_SIMPLE = """
        UNWIND $nodes AS node
        MERGE (n {_elementId: node.elementId})
        SET n = node.properties
        SET n._elementId = node.elementId
        """;

    private final IndexManager indexManager;
    private final String nodeKey;

    public BulkImporter(IndexManager indexManager, String nodeKey) {
        this.indexManager = indexManager;
        this.nodeKey = nodeKey;
    }

    @SuppressWarnings("unchecked")
    public void importBatch(Session session, FullSyncBatch batch) {
        if (batch.entityType() == EntityType.NODE) {
            // Ensure indexes for labels in this batch
            for (Map<String, Object> entity : batch.entities()) {
                List<String> labels = (List<String>) entity.get("labels");
                if (labels != null) {
                    for (String label : labels) {
                        indexManager.ensureIndex(session, nodeKey, label);
                    }
                }
            }

            // M9 fix: group entities by their full label set and issue one MERGE per group
            // with labels embedded so the MERGE can hit the (label, _elementId) index.
            // The previous unlabeled MERGE degraded to a full-graph scan on any sizeable DB.
            var labelGroups = new java.util.HashMap<String, java.util.List<Map<String, Object>>>();
            var labellessEntities = new java.util.ArrayList<Map<String, Object>>();
            for (Map<String, Object> entity : batch.entities()) {
                List<String> labels = (List<String>) entity.get("labels");
                if (labels == null || labels.isEmpty()) {
                    labellessEntities.add(entity);
                    continue;
                }
                String labelStr = String.join(":", labels.stream()
                    .map(IndexManager::sanitizeLabel).toList());
                labelGroups.computeIfAbsent(labelStr, k -> new java.util.ArrayList<>()).add(entity);
            }

            for (var entry : labelGroups.entrySet()) {
                String labelStr = entry.getKey();
                List<Map<String, Object>> group = entry.getValue();
                String cypher = """
                    UNWIND $nodes AS node
                    MERGE (n:%s {_elementId: node.elementId})
                    SET n = node.properties
                    SET n._elementId = node.elementId
                    """.formatted(labelStr);
                session.executeWrite(tx -> {
                    tx.run(cypher, Map.of("nodes", group)).consume();
                    return null;
                });
            }

            if (!labellessEntities.isEmpty()) {
                // Fallback for entities without labels (should be rare in practice).
                session.executeWrite(tx -> {
                    tx.run(BULK_NODE_IMPORT_SIMPLE, Map.of("nodes", labellessEntities)).consume();
                    return null;
                });
            }
        } else {
            // === Relationships ===
            //
            // BUG-086: previous impl had two compounding perf bugs:
            //   1) MATCH (a {_elementId: ...}) without a label → could NOT use
            //      the per-label `_elementId` UNIQUE constraint's backing
            //      index, degrading to a full-graph scan for every match.
            //   2) Each rel ran its own `session.executeWrite` → 1000 commits
            //      per batch instead of 1.
            // Observed effect: ~14 s per 1000-rel batch (≈ 23 batches × 14s
            //   = 5 minutes for 22k rels) on a 6k-node graph.
            //
            // Fix: group rels by (startLabel, relType, endLabel) → one
            // UNWIND-batched cypher per group, with label-aware MATCH that
            // hits the per-label index. RelationshipExporter already ships
            // `startLabels` / `endLabels` in each entity, so no protocol
            // change is needed. Expected speedup: 10–100× depending on
            // graph size; one MERGE per rel still, but the MATCH path uses
            // the per-label index.
            for (Map<String, Object> entity : batch.entities()) {
                String relType = (String) entity.get("relType");
                indexManager.ensureRelIndex(session, nodeKey, relType);
            }

            // Bucket by (startLabel0, relType, endLabel0). When labels are
            // missing on a rel's endpoints (legacy data) we fall back into
            // an "unlabeled" bucket that uses the slow label-less MATCH.
            // In practice business graphs have ≤ a few dozen such triples
            // so the per-batch group count stays small.
            var groups = new java.util.LinkedHashMap<String, java.util.List<Map<String, Object>>>();
            for (Map<String, Object> rel : batch.entities()) {
                @SuppressWarnings("unchecked")
                List<String> sLabels = (List<String>) rel.get("startLabels");
                @SuppressWarnings("unchecked")
                List<String> eLabels = (List<String>) rel.get("endLabels");
                String sLabel = (sLabels == null || sLabels.isEmpty())
                    ? "" : IndexManager.sanitizeLabel(sLabels.get(0));
                String eLabel = (eLabels == null || eLabels.isEmpty())
                    ? "" : IndexManager.sanitizeLabel(eLabels.get(0));
                String relType = IndexManager.sanitizeLabel((String) rel.get("relType"));
                String key = sLabel + "|" + relType + "|" + eLabel;
                groups.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(rel);
            }

            for (var entry : groups.entrySet()) {
                String[] parts = entry.getKey().split("\\|", -1);
                String sLabel = parts[0];
                String relType = parts[1];
                String eLabel = parts[2];
                List<Map<String, Object>> bucket = entry.getValue();

                String startMatch = sLabel.isEmpty()
                    ? "MATCH (a {_elementId: rel.startNodeId})"
                    : "MATCH (a:" + sLabel + " {_elementId: rel.startNodeId})";
                String endMatch = eLabel.isEmpty()
                    ? "MATCH (b {_elementId: rel.endNodeId})"
                    : "MATCH (b:" + eLabel + " {_elementId: rel.endNodeId})";

                String cypher = """
                    UNWIND $rels AS rel
                    %s
                    %s
                    MERGE (a)-[r:%s {_elementId: rel.elementId}]->(b)
                    SET r = rel.properties
                    SET r._elementId = rel.elementId
                    """.formatted(startMatch, endMatch, relType);

                session.executeWrite(tx -> {
                    tx.run(cypher, Map.of("rels", bucket)).consume();
                    return null;
                });
            }
        }

        log.debug("Imported batch {}/{} ({} entities)",
            batch.batchIndex() + 1, batch.totalBatches(), batch.entities().size());
    }
}
