package com.neo4j.ha.sync.applier;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.model.EntityData;
import org.neo4j.driver.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeApplier {

    private static final Logger log = LoggerFactory.getLogger(NodeApplier.class);

    public void create(TransactionContext tx, ChangeEvent event) {
        mergeNode(tx, event);
    }

    public void update(TransactionContext tx, ChangeEvent event) {
        mergeNode(tx, event);
    }

    public void delete(TransactionContext tx, ChangeEvent event) {
        EntityData entity = event.entity();
        String elementId = entity.elementId();
        List<String> labels = entity.labels();

        String cypher;
        if (labels != null && !labels.isEmpty()) {
            String labelStr = String.join(":", labels.stream()
                .map(IndexManager::sanitizeLabel).toList());
            cypher = CypherTemplates.NODE_DELETE.formatted(labelStr);
        } else {
            cypher = CypherTemplates.NODE_DELETE_NO_LABEL;
        }

        tx.run(cypher, Map.of("elementId", elementId)).consume();
        log.debug("Deleted node: {}", elementId);
    }

    private void mergeNode(TransactionContext tx, ChangeEvent event) {
        EntityData entity = event.entity();
        String elementId = entity.elementId();
        List<String> labels = entity.labels();

        if (labels == null || labels.isEmpty()) {
            log.warn("Node event without labels, skipping: {}", elementId);
            return;
        }

        String labelStr = String.join(":", labels.stream()
            .map(IndexManager::sanitizeLabel).toList());
        String cypher = CypherTemplates.NODE_MERGE.formatted(labelStr);

        Map<String, Object> properties = entity.properties() != null
            ? new HashMap<>(entity.properties()) : new HashMap<>();
        properties.put("_elementId", elementId);

        tx.run(cypher, Map.of("elementId", elementId, "properties", properties)).consume();
        log.debug("Merged node: {} [{}]", elementId, labelStr);
    }
}
