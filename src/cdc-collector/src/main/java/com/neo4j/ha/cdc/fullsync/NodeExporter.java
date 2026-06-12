package com.neo4j.ha.cdc.fullsync;

import com.neo4j.ha.common.model.EntityType;
import com.neo4j.ha.common.model.FullSyncBatch;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.util.IdGenerator;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeExporter {

    private static final Logger log = LoggerFactory.getLogger(NodeExporter.class);

    private static final String COUNT_QUERY = "MATCH (n) WHERE NOT n:_CDCDeleteEvent RETURN count(n) AS cnt";
    // BUG-053 fix: emit the cluster-stable `_elementId` PROPERTY (falling back to
    // local id only for legacy nodes that predate the trigger). Sync-applier on
    // standbys MERGEs by `{_elementId: $elementId}`, so local ids from a new
    // primary would create duplicate orphan nodes instead of updating in place.
    private static final String EXPORT_QUERY = """
        MATCH (n) WHERE NOT n:_CDCDeleteEvent
        RETURN n, labels(n) AS labels, properties(n) AS props,
               coalesce(n._elementId, elementId(n)) AS eid
        ORDER BY elementId(n)
        SKIP $offset LIMIT $batchSize
        """;

    public int export(Session session, StreamPublisher publisher, String streamKey,
                      int batchSize, int throttleMs) {
        long totalNodes = session.run(COUNT_QUERY).single().get("cnt").asLong();
        int totalBatches = (int) Math.ceil((double) totalNodes / batchSize);
        log.info("Full sync: exporting {} nodes in {} batches", totalNodes, totalBatches);

        int exported = 0;
        for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            List<Map<String, Object>> entities = new ArrayList<>();
            var result = session.run(EXPORT_QUERY, Map.of(
                "offset", (long) batchIndex * batchSize,
                "batchSize", batchSize
            ));

            for (Record record : result.list()) {
                Map<String, Object> entity = new HashMap<>();
                entity.put("elementId", record.get("eid").asString());
                entity.put("labels", record.get("labels").asList(Value::asString));
                entity.put("properties", record.get("props").asMap());
                entities.add(entity);
            }

            FullSyncBatch batch = new FullSyncBatch(
                IdGenerator.uuidV7(), batchIndex, totalBatches,
                EntityType.NODE, entities, System.currentTimeMillis()
            );
            publisher.publishFullSyncBatch(streamKey, batch);
            exported += entities.size();

            if (throttleMs > 0) {
                try { Thread.sleep(throttleMs); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (batchIndex % 10 == 0) {
                log.info("Full sync node export progress: {}/{}", batchIndex + 1, totalBatches);
            }
        }

        log.info("Full sync: exported {} nodes", exported);
        return exported;
    }
}
