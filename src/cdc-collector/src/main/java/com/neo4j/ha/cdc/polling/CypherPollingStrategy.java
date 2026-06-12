package com.neo4j.ha.cdc.polling;

import com.neo4j.ha.cdc.CdcCollectorConfig;
import com.neo4j.ha.cdc.capture.DeleteEventCapture;
import com.neo4j.ha.cdc.capture.NodeChangeCapture;
import com.neo4j.ha.cdc.capture.RelationshipChangeCapture;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CypherPollingStrategy implements ChangeDetector {

    private static final Logger log = LoggerFactory.getLogger(CypherPollingStrategy.class);

    private final NodeChangeCapture nodeCapture;
    private final RelationshipChangeCapture relCapture;
    private final DeleteEventCapture deleteCapture;
    private volatile Driver driver;
    private final String database;

    public CypherPollingStrategy(Driver driver, String database, CdcCollectorConfig config) {
        this.driver = driver;
        this.database = database;
        this.nodeCapture = new NodeChangeCapture(
            config.timestampField(), config.createdAtField(), config.elementIdField());
        this.relCapture = new RelationshipChangeCapture(
            config.timestampField(), config.createdAtField(), config.elementIdField());
        this.deleteCapture = new DeleteEventCapture();
    }

    public void switchDriver(Driver newDriver) {
        this.driver = newDriver;
    }

    @Override
    public List<RawChange> detectChanges(PollingState state, int batchSize) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            List<RawChange> allChanges = new ArrayList<>();

            // BUG-057: use independent (ts, eid) cursors for node, relationship
            // and delete scans. Sharing a single cursor lets a "5:..."
            // relationship eid from a previous poll silently drop any node
            // with matching lastTs from the next poll (node eids start with
            // "4:" which is < "5:" in dictionary order, so
            // `_elementId > $lastEid` is unsatisfiable).

            // Node changes — node cursor only
            allChanges.addAll(nodeCapture.detectChanges(
                session, state.getLastNodeTs(), state.getLastNodeEid(), batchSize));

            // Relationship changes — rel cursor only
            allChanges.addAll(relCapture.detectChanges(
                session, state.getLastRelTs(), state.getLastRelEid(), batchSize));

            // Delete events from transit nodes — dedicated delete cursor
            allChanges.addAll(deleteCapture.captureDeleteEvents(
                session, state.getLastDeleteTs(), state.getLastDeleteEid(), batchSize));

            // Sort by timestamp for consistent ordering of the merged batch
            // (does NOT affect cursor advancement — CdcCollector.pollLoop now
            // picks the per-type last, not the merged last).
            allChanges.sort(Comparator.comparingLong(RawChange::timestamp)
                .thenComparing(RawChange::elementId));

            return allChanges;
        }
    }

    public DeleteEventCapture getDeleteCapture() {
        return deleteCapture;
    }

    public Driver getDriver() {
        return driver;
    }
}
