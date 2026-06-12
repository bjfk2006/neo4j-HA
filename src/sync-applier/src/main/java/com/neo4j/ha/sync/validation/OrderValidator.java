package com.neo4j.ha.sync.validation;

import com.neo4j.ha.common.model.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OrderValidator {

    private static final Logger log = LoggerFactory.getLogger(OrderValidator.class);

    /**
     * BUG-070: collapse per-event out-of-order WARNs into one per batch and
     * then further rate-limit to at most one log line per {@code
     * WARN_SUPPRESS_MS} across all batches. Before the fix, a single burst
     * of 200 out-of-order events produced 200 WARN lines (one per event),
     * which in the chaos-test run drowned out the actual failover events.
     * After: one WARN per batch at most, with a periodic "+N suppressed
     * since last log" counter so no information is permanently lost.
     */
    private static final long WARN_SUPPRESS_MS = 10_000L;

    private long lastTimestamp = 0;

    // BUG-070 rate-limit state. Accessed from the single sync-applier
    // thread so non-volatile is safe; kept simple for clarity.
    private long lastWarnMs = 0;
    private long suppressedSinceLastWarn = 0;

    public void validate(List<ChangeEvent> events) {
        int outOfOrder = 0;
        String firstEventId = null;
        long firstTs = 0;
        long firstLastTs = 0;
        for (ChangeEvent event : events) {
            if (event.timestamp() < lastTimestamp) {
                if (outOfOrder == 0) {
                    firstEventId = event.eventId();
                    firstTs = event.timestamp();
                    firstLastTs = lastTimestamp;
                }
                outOfOrder++;
            }
            if (event.timestamp() > lastTimestamp) {
                lastTimestamp = event.timestamp();
            }
        }
        if (outOfOrder == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastWarnMs >= WARN_SUPPRESS_MS) {
            long previouslySuppressed = suppressedSinceLastWarn;
            lastWarnMs = now;
            suppressedSinceLastWarn = 0;
            if (previouslySuppressed > 0) {
                log.warn("Out-of-order events in batch: {} (first: eventId={}, ts={}, lastTs={}); " +
                        "+{} also suppressed since last warn",
                    outOfOrder, firstEventId, firstTs, firstLastTs, previouslySuppressed);
            } else {
                log.warn("Out-of-order events in batch: {} (first: eventId={}, ts={}, lastTs={})",
                    outOfOrder, firstEventId, firstTs, firstLastTs);
            }
        } else {
            suppressedSinceLastWarn += outOfOrder;
        }
    }
}
