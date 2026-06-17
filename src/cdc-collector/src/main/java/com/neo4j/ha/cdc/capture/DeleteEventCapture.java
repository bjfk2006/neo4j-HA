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

    // BUG-087: page by the deleted entity's `_elementId` PROPERTY (e.elementId),
    // NOT the transit node's own `elementId(e)`. The cursor that is fed back in
    // ($lastDeleteEid) is RawChange.elementId == e.elementId (see
    // captureDeleteEvents below), and the post-publish cleanup bounds itself on
    // the same key. Keeping all three — capture keyset, cursor advance, cleanup
    // boundary — on e.elementId makes the delete path internally consistent so
    // same-timestamp overflow is paged across polls instead of being dropped.
    private static final String CAPTURE_QUERY = """
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp > $lastDeleteTs
           OR (e.timestamp = $lastDeleteTs AND e.elementId > $lastDeleteEid)
        RETURN e, elementId(e) AS eid
        ORDER BY e.timestamp ASC, e.elementId ASC
        LIMIT $batchSize
        """;

    // BUG-067 startup sweep ONLY. Threshold delete of every orphan transit node
    // older than a cutoff. Correct for "garbage-collect a previous tenure's
    // leftovers" but UNSAFE as a post-publish cleanup — see
    // cleanupCapturedDeleteEvents for why.
    private static final String CLEANUP_QUERY = """
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp <= $publishedTs
        WITH e LIMIT 10000
        DETACH DELETE e
        RETURN count(*) AS deleted
        """;

    // BUG-087: bounded cleanup of exactly the keyset prefix captured this batch:
    // everything strictly below maxTs, plus — at maxTs — only transit nodes with
    // e.elementId <= lastEid (the last captured boundary). Same-timestamp
    // overflow (e.elementId > lastEid at maxTs) survives for the next poll.
    private static final String CLEANUP_CAPTURED_QUERY = """
        MATCH (e:_CDCDeleteEvent)
        WHERE e.timestamp < $maxTs
           OR (e.timestamp = $maxTs AND e.elementId <= $lastEid)
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

    /**
     * BUG-087: clean up ONLY the delete-event transit nodes actually captured
     * (and published) in the current poll batch, keyed on the same
     * {@code (timestamp, e.elementId)} keyset the capture query pages by.
     *
     * <p>The legacy {@link #cleanupDeleteEvents} deletes every
     * {@code _CDCDeleteEvent} with {@code timestamp <= publishedTs}. As a
     * post-publish cleanup that is unsafe: {@code timestamp()} is constant
     * within a transaction, so a single {@code DETACH DELETE} of more than
     * {@code batchSize} entities produces more same-timestamp transit nodes
     * than one batch can capture. The capture query pages the
     * {@code (timestamp, e.elementId)} keyset and stops at {@code batchSize};
     * a threshold cleanup would then delete the un-captured same-timestamp
     * overflow BEFORE it was ever published, so those deletes never reach
     * standbys — leaving them holding data the primary already deleted
     * (standby &gt; primary).</p>
     *
     * <p>This deletes exactly the captured keyset prefix — everything strictly
     * below {@code maxTs}, plus at {@code maxTs} only transit nodes with
     * {@code e.elementId <= lastEid} — so same-timestamp overflow survives and
     * the next poll pages into it via {@code e.elementId > lastEid}.</p>
     *
     * @param maxTs   timestamp of the last captured delete event (batch max)
     * @param lastEid {@code _elementId} of the last captured delete event at {@code maxTs}
     */
    public long cleanupCapturedDeleteEvents(Session session, long maxTs, String lastEid) {
        var result = session.run(CLEANUP_CAPTURED_QUERY, Map.of(
            "maxTs", maxTs,
            "lastEid", lastEid != null ? lastEid : ""
        ));
        long deleted = result.single().get("deleted").asLong();
        if (deleted > 0) {
            log.info("Cleaned up {} captured _CDCDeleteEvent transit nodes", deleted);
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
