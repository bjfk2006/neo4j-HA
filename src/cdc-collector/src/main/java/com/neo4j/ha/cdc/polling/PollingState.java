package com.neo4j.ha.cdc.polling;

/**
 * Keyset-pagination cursor state for the CDC collector.
 *
 * <h3>BUG-057: why node / relationship / delete cursors are split</h3>
 *
 * <p>Prior to BUG-057 this class held a single {@code (lastTs, lastElementId)}
 * pair shared by the node and relationship scans. Because Neo4j 5.x
 * {@code elementId()} is formatted {@code "<kind>:<db-uuid>:<internal-id>"}
 * where {@code kind} is {@code "4"} for nodes and {@code "5"} for
 * relationships, any poll that saw both a node and a relationship at the same
 * millisecond would always push the cursor to a {@code "5:..."} eid (larger in
 * dictionary order). The next poll's node query
 * {@code n._elementId > $lastEid} would then be unsatisfiable for every node
 * with {@code _updated_at == lastTs}, silently dropping those writes.</p>
 *
 * <p>The fix keeps three independent {@code (ts, eid)} cursors that each
 * capture advances only its own cursor. The composite legacy getter
 * {@link #getLastTs()} returns the max of node and rel cursors so downstream
 * freshness checks keep working without a schema hop.</p>
 */
public class PollingState {

    private long lastNodeTs;
    private String lastNodeEid;
    private long lastRelTs;
    private String lastRelEid;
    private long lastDeleteTs;
    private String lastDeleteEid;

    public PollingState(long lastNodeTs, String lastNodeEid,
                         long lastRelTs, String lastRelEid,
                         long lastDeleteTs, String lastDeleteEid) {
        this.lastNodeTs = lastNodeTs;
        this.lastNodeEid = lastNodeEid != null ? lastNodeEid : "";
        this.lastRelTs = lastRelTs;
        this.lastRelEid = lastRelEid != null ? lastRelEid : "";
        this.lastDeleteTs = lastDeleteTs;
        this.lastDeleteEid = lastDeleteEid != null ? lastDeleteEid : "";
    }

    public static PollingState initial() {
        return new PollingState(0, "", 0, "", 0, "");
    }

    public long getLastNodeTs() { return lastNodeTs; }
    public void setLastNodeTs(long lastNodeTs) { this.lastNodeTs = lastNodeTs; }

    public String getLastNodeEid() { return lastNodeEid; }
    public void setLastNodeEid(String lastNodeEid) {
        this.lastNodeEid = lastNodeEid != null ? lastNodeEid : "";
    }

    public long getLastRelTs() { return lastRelTs; }
    public void setLastRelTs(long lastRelTs) { this.lastRelTs = lastRelTs; }

    public String getLastRelEid() { return lastRelEid; }
    public void setLastRelEid(String lastRelEid) {
        this.lastRelEid = lastRelEid != null ? lastRelEid : "";
    }

    public long getLastDeleteTs() { return lastDeleteTs; }
    public void setLastDeleteTs(long lastDeleteTs) { this.lastDeleteTs = lastDeleteTs; }

    public String getLastDeleteEid() { return lastDeleteEid; }
    public void setLastDeleteEid(String lastDeleteEid) {
        this.lastDeleteEid = lastDeleteEid != null ? lastDeleteEid : "";
    }

    /**
     * Legacy "global" last timestamp. Returns the max of the node and rel
     * cursors. Used by checkpoint freshness checks (HaAgent evaluateServiceStates)
     * and drain-loop stability detection, where monotonic progress across all
     * change kinds is all that matters.
     */
    public long getLastTs() {
        return Math.max(lastNodeTs, lastRelTs);
    }

    /**
     * Legacy composite eid: returns the eid of whichever of (node, rel)
     * cursor has the higher lastTs; used by v1 checkpoint writers so existing
     * readers (and the legacy Redis HASH field) can still decode a sensible
     * value after upgrade.
     */
    public String getLastElementId() {
        if (lastRelTs > lastNodeTs) return lastRelEid;
        if (lastNodeTs > lastRelTs) return lastNodeEid;
        // tie: prefer rel eid (old cursor ordering would have picked "5:..." too)
        return !lastRelEid.isEmpty() ? lastRelEid : lastNodeEid;
    }
}
