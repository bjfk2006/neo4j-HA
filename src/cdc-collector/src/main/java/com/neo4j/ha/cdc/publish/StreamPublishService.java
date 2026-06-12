package com.neo4j.ha.cdc.publish;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.redis.StreamPublisher;
import com.neo4j.ha.common.redis.StreamPublisher.FencingTokenRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StreamPublishService {

    private static final Logger log = LoggerFactory.getLogger(StreamPublishService.class);

    private final StreamPublisher streamPublisher;
    private final PublishBuffer publishBuffer;
    private final String streamKey;
    private final HaMetrics metrics;
    private volatile long fencingToken;

    public StreamPublishService(StreamPublisher streamPublisher, PublishBuffer publishBuffer,
                                 String streamKey, long fencingToken, HaMetrics metrics) {
        this.streamPublisher = streamPublisher;
        this.publishBuffer = publishBuffer;
        this.streamKey = streamKey;
        this.fencingToken = fencingToken;
        this.metrics = metrics;
    }

    public void setFencingToken(long newToken) {
        this.fencingToken = newToken;
    }

    /**
     * Append events to the local PublishBuffer without trying to publish them to
     * Redis. Used by {@code CdcCollector.pollLoop} when a {@link FencingTokenRejectedException}
     * is seen mid-publish (BUG-047 defence in depth) — the events have already been
     * extracted from Neo4j and we do not want to lose them. A subsequent
     * {@code start()} on the new primary will call {@link #retryBuffered()} on every
     * tick and the next epoch's poll will flush them with the current token.
     */
    public void bufferForRetry(List<ChangeEvent> events) {
        if (events == null || events.isEmpty()) return;
        publishBuffer.add(events);
        metrics.bufferSize.set(publishBuffer.size());
    }

    /**
     * @return the last Stream message ID if published successfully, null if buffered due to failure.
     */
    public String publishBatch(List<ChangeEvent> events) throws FencingTokenRejectedException {
        if (events.isEmpty()) return null;

        retryBuffered();

        try {
            String lastStreamId = streamPublisher.publishBatch(streamKey, events, fencingToken);
            metrics.cdcEventsPublished.increment(events.size());
            if (!events.isEmpty()) {
                metrics.cdcLastPublishedTs.set(events.get(events.size() - 1).timestamp());
            }
            log.debug("Published {} events to stream", events.size());
            return lastStreamId;
        } catch (FencingTokenRejectedException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Failed to publish to Redis, buffering {} events", events.size(), e);
            publishBuffer.add(events);
            metrics.bufferSize.set(publishBuffer.size());
            return null;
        }
    }

    public void retryBuffered() throws FencingTokenRejectedException {
        if (!publishBuffer.hasBuffered()) return;

        List<ChangeEvent> buffered = publishBuffer.drain(500);
        if (buffered.isEmpty()) return;

        try {
            streamPublisher.publishBatch(streamKey, buffered, fencingToken);
            metrics.cdcEventsPublished.increment(buffered.size());
            metrics.bufferSize.set(publishBuffer.size());
            log.info("Flushed {} buffered events", buffered.size());
        } catch (FencingTokenRejectedException e) {
            // Do NOT swallow. Re-buffer (so events are not lost) and propagate so the CDC loop
            // can stop itself. The orchestrator will handle role transition.
            publishBuffer.add(buffered);
            log.error("Fencing token rejected while flushing buffer; CDC must step down", e);
            throw e;
        } catch (Exception e) {
            publishBuffer.add(buffered); // Re-buffer
            log.warn("Failed to flush buffered events, re-buffering", e);
        }
    }
}
