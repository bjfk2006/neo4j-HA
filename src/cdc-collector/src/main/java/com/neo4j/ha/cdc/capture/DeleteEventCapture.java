package com.neo4j.ha.cdc.capture;

import com.neo4j.ha.cdc.polling.ChangeDetector.RawChange;
import com.neo4j.ha.common.model.ChangeEventType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteEventCapture {

    private static final Logger log = LoggerFactory.getLogger(DeleteEventCapture.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CAPTURE_QUERY = """
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp > $lastDeleteTs
           OR (e.timestamp = $lastDeleteTs AND elementId(e) > $lastDeleteEid)
        RETURN e, elementId(e) AS eid
        ORDER BY e.timestamp ASC, elementId(e) ASC
        LIMIT $batchSize
        """;

    private static final String CLEANUP_QUERY = """
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp <= $publishedTs
        WITH e LIMIT 10000
        DETACH DELETE e
        RETURN count(*) AS deleted
        """;

    public List<RawChange> captureDeleteEvents(Session session, long lastDeleteTs,
                                                 String lastDeleteEid, int batchSize) {
        var result = session.run(CAPTURE_QUERY, Map.of(
            "lastDeleteTs", lastDeleteTs,
            "lastDeleteEid", lastDeleteEid != null ? lastDeleteEid : "",
            "batchSize", batchSize
        ));

        List<RawChange> events = new ArrayList<>();
        for (Record record : result.list()) {
            Node e = record.get("e").asNode();
            String eid = record.get("eid").asString();
            String eventType = e.get("eventType").asString();
            String elementId = e.get("elementId").asString();
            long timestamp = e.get("timestamp").asLong();

            List<String> labels = parseJsonList(e.get("labels").asString("[]"));
            Map<String, Object> properties = parseJsonMap(e.get("properties").asString("{}"));

            ChangeEventType type = "REL_DELETED".equals(eventType)
                ? ChangeEventType.RELATIONSHIP_DELETED
                : ChangeEventType.NODE_DELETED;

            String relType = e.containsKey("relType") ? e.get("relType").asString(null) : null;

            // BUG-082: rel-delete transit nodes now carry the deleted rel's
            // start/end node `_elementId`s (stamped by REL_DELETE_TRIGGER).
            // Old transit nodes written before the trigger upgrade won't
            // have these fields; `asString(null)` keeps the applier on the
            // legacy elementId-only delete path in that case.
            String startEid = e.containsKey("startElementId")
                ? e.get("startElementId").asString(null) : null;
            String endEid = e.containsKey("endElementId")
                ? e.get("endElementId").asString(null) : null;

            events.add(new RawChange(type, elementId, labels, properties, null,
                relType, startEid, endEid, timestamp));
        }

        log.debug("Captured {} delete events", events.size());
        return events;
    }

    public long cleanupDeleteEvents(Session session, long publishedTs) {
        var result = session.run(CLEANUP_QUERY, Map.of("publishedTs", publishedTs));
        long deleted = result.single().get("deleted").asLong();
        if (deleted > 0) {
            log.info("Cleaned up {} _CDCDeleteEvent transit nodes", deleted);
        }
        return deleted;
    }

    @SuppressWarnings("unchecked")
    private List<String> parseJsonList(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<List<String>>() {});
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonMap(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}
