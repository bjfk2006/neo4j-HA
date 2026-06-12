package com.neo4j.ha.sync.applier;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.EntityData;
import org.neo4j.driver.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RelationshipApplier {

    private static final Logger log = LoggerFactory.getLogger(RelationshipApplier.class);

    public void create(TransactionContext tx, ChangeEvent event) {
        mergeRelationship(tx, event);
    }

    public void update(TransactionContext tx, ChangeEvent event) {
        mergeRelationship(tx, event);
    }

    public void delete(TransactionContext tx, ChangeEvent event) {
        EntityData entity = event.entity();
        String relType = entity.relationshipType();
        if (relType == null || relType.isBlank()) {
            log.warn("Relationship delete without type, skipping: {}", entity.elementId());
            return;
        }

        String startNodeId = entity.startNodeElementId();
        String endNodeId = entity.endNodeElementId();
        String safeType = IndexManager.sanitizeLabel(relType);

        // BUG-082: prefer the endpoint-scoped delete. Under Neo4j 5.x
        // `_elementId` recycling, matching a rel by `_elementId` alone can
        // delete a later, unrelated rel that inherited the recycled id.
        // Scoping to (start_eid, end_eid, type, _elementId) makes the
        // deletion safe. Fall back to legacy elementId-only delete only
        // when the CDC event lacks endpoint ids (pre-BUG-082 transit
        // nodes written by the old REL_DELETE_TRIGGER during rolling
        // upgrade).
        if (startNodeId != null && endNodeId != null) {
            String cypher = CypherTemplates.REL_DELETE_SCOPED.formatted(safeType);
            tx.run(cypher, Map.of(
                "startNodeId", startNodeId,
                "endNodeId", endNodeId,
                "relElementId", entity.elementId()
            )).consume();
            log.debug("Deleted relationship (scoped): {} [{}] ({}->{})",
                entity.elementId(), relType, startNodeId, endNodeId);
            return;
        }

        String cypher = CypherTemplates.REL_DELETE.formatted(safeType);
        tx.run(cypher, Map.of("relElementId", entity.elementId())).consume();
        log.debug("Deleted relationship (legacy, no endpoint ids): {} [{}]",
            entity.elementId(), relType);
    }

    private void mergeRelationship(TransactionContext tx, ChangeEvent event) {
        EntityData entity = event.entity();
        String relType = entity.relationshipType();
        String startNodeId = entity.startNodeElementId();
        String endNodeId = entity.endNodeElementId();

        if (relType == null || startNodeId == null || endNodeId == null) {
            log.warn("Incomplete relationship event, skipping: {}", entity.elementId());
            return;
        }

        String cypher;
        boolean hasLabels = hasNonEmptyLabels(entity.startNodeLabels())
                         && hasNonEmptyLabels(entity.endNodeLabels());
        if (hasLabels) {
            String startLabels = joinLabels(entity.startNodeLabels());
            String endLabels = joinLabels(entity.endNodeLabels());
            cypher = CypherTemplates.REL_MERGE.formatted(startLabels, endLabels,
                IndexManager.sanitizeLabel(relType));
        } else {
            // BUG-078 + BUG-079: see CypherTemplates.REL_MERGE for rationale.
            // Without labels we can't MERGE on label+eid (would risk matching a
            // node with a different label but the same _elementId), so stick
            // with MATCH + drop the rel if endpoints genuinely don't exist —
            // the test workloads always carry labels, so this path is rare.
            // If we do hit it, the rel simply won't be created; the lack of
            // labels means we also can't safely create a stub node.
            // BUG-082: stale MATCH is scoped to (a)-[stale]->(b) — see
            // CypherTemplates.REL_MERGE for the full rationale. Without
            // endpoint scoping, `_elementId` recycling silently deletes
            // unrelated rels that inherit the recycled id.
            cypher = """
                MATCH (a {_elementId: $startNodeId})
                MATCH (b {_elementId: $endNodeId})
                OPTIONAL MATCH (a)-[stale:%s {_elementId: $relElementId}]->(b)
                DELETE stale
                CREATE (a)-[r:%s]->(b)
                SET r = $properties
                SET r._elementId = $relElementId
                """.formatted(IndexManager.sanitizeLabel(relType),
                              IndexManager.sanitizeLabel(relType));
        }

        Map<String, Object> properties = entity.properties() != null
            ? new HashMap<>(entity.properties()) : new HashMap<>();
        properties.put("_elementId", entity.elementId());

        tx.run(cypher, Map.of(
            "startNodeId", startNodeId,
            "endNodeId", endNodeId,
            "relElementId", entity.elementId(),
            "properties", properties
        )).consume();

        log.debug("Merged relationship: {} [{}]", entity.elementId(), relType);
    }

    private static boolean hasNonEmptyLabels(List<String> labels) {
        return labels != null && !labels.isEmpty();
    }

    private static String joinLabels(List<String> labels) {
        return labels.stream()
            .map(IndexManager::sanitizeLabel)
            .collect(Collectors.joining(":"));
    }
}
