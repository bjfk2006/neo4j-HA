package com.neo4j.ha.sync.validation;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.util.FencingTokenValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Tracks the highest fencing token observed in the Redis Stream. It never drops events.
 *
 * Design rationale (BUG-037): every event that reaches the Redis Stream has already been
 * admitted by the publisher-side Lua script (see {@link
 * com.neo4j.ha.common.redis.StreamPublisher#publish}) which checks `event.token ≥
 * redis.fencing-token` atomically before XADD. By the time Sync Applier reads an event,
 * the cluster has already agreed that this event is legitimate for its epoch. Dropping
 * events whose token is LOWER than our `knownMaxToken` corrupts data: in-flight events
 * written by the pre-switchover primary with the old token are valid data that simply
 * haven't been consumed yet; they MUST be applied.
 *
 * The earlier implementation used `token >= knownMaxToken` as a filter and threw those
 * legitimate-but-old events away, producing a 601-event data loss reproducible by the
 * load/switchover test: all Phase-1 writes with token=0 that were still in the Stream
 * at switchover time were silently discarded when Sync Applier resumed with token=1.
 *
 * This class is kept for observability: it logs any event whose token is GREATER than
 * the token we were told to expect — that would indicate a publisher bug (someone else
 * bypassed the Lua check) and is worth an operator alert.
 */
public class FencingTokenFilter {

    private static final Logger log = LoggerFactory.getLogger(FencingTokenFilter.class);

    private final FencingTokenValidator validator = new FencingTokenValidator();

    /**
     * Pass all events through unchanged. We keep the signature as a filter so callers
     * don't need restructuring; in effect this is an identity transform plus a sanity
     * log.
     */
    public List<ChangeEvent> filter(List<ChangeEvent> events) {
        for (ChangeEvent event : events) {
            long known = validator.getCurrentToken();
            if (event.fencingToken() > known) {
                // Future token observed — update our known-max for metrics, and log once.
                // This is not an error path (Lua check on publish side already approved it).
                log.info("Advancing knownMaxToken {} -> {} based on event {}",
                    known, event.fencingToken(), event.eventId());
                validator.updateToken(event.fencingToken());
            }
        }
        return events;
    }

    public void updateToken(long token) {
        validator.updateToken(token);
    }
}
