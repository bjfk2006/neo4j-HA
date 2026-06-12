package com.neo4j.ha.cdc.fullsync;

import com.neo4j.ha.common.model.EntityType;
import com.neo4j.ha.common.model.FullSyncBatch;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.util.IdGenerator;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelationshipExporter {

    private static final Logger log = LoggerFactory.getLogger(RelationshipExporter.class);

    private static final String COUNT_QUERY = "MATCH ()-[r]->() RETURN count(r) AS cnt";
    // BUG-053 fix: prefer the cluster-stable `_elementId` PROPERTY over local
    // `elementId(...)` for both the relationship and its endpoint nodes. The
    // sync-applier on standbys matches everything by the `_elementId` property,
    // so publishing local ids that differ between source primary and standby
    // makes MERGE/MATCH miss. ORDER BY elementId(r) is kept only for stable
    // pagination — it does not flow to the wire.
    private static final String EXPORT_QUERY = """
        MATCH (a)-[r]->(b)
        RETURN type(r) AS relType, properties(r) AS props,
               coalesce(r._elementId, elementId(r)) AS eid,
               coalesce(a._elementId, elementId(a)) AS startEid,
               coalesce(b._elementId, elementId(b)) AS endEid,
               labels(a) AS startLabels, labels(b) AS endLabels
        ORDER BY elementId(r)
        SKIP $offset LIMIT $batchSize
        """;

    public int export(Session session, StreamPublisher publisher, String streamKey,
                      int batchSize, int throttleMs) {
        long totalRels = session.run(COUNT_QUERY).single().get("cnt").asLong();
        int totalBatches = (int) Math.ceil((double) totalRels / batchSize);
        log.info("Full sync: exporting {} relationships in {} batches", totalRels, totalBatches);

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
                entity.put("relType", record.get("relType").asString());
                entity.put("properties", record.get("props").asMap());
                entity.put("startNodeId", record.get("startEid").asString());
                entity.put("endNodeId", record.get("endEid").asString());
                // BUG-086: ship startLabels / endLabels so BulkImporter can issue
                // a label-aware MATCH (a:<Label> {_elementId: ...}) that uses
                // the per-label `_elementId` index. Without these the receiver's
                // MATCH degrades to a full-graph scan per rel — ~14s per 1000-rel
                // batch on a 6k-node graph.
                entity.put("startLabels",
                    record.get("startLabels").asList(org.neo4j.driver.Value::asString));
                entity.put("endLabels",
                    record.get("endLabels").asList(org.neo4j.driver.Value::asString));
                entities.add(entity);
            }

            FullSyncBatch batch = new FullSyncBatch(
                IdGenerator.uuidV7(), batchIndex, totalBatches,
                EntityType.RELATIONSHIP, entities, System.currentTimeMillis()
            );
            publisher.publishFullSyncBatch(streamKey, batch);
            exported += entities.size();

            if (throttleMs > 0) {
                try { Thread.sleep(throttleMs); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Full sync: exported {} relationships", exported);
        return exported;
    }
}
