package com.neo4j.ha.agent.http;

import com.neo4j.ha.common.metrics.HaMetrics;
import com.neo4j.ha.common.metrics.MetricsRegistry;
import io.javalin.http.Context;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GET /api/metrics-summary — small JSON projection of the most operator-
 * relevant HA metrics. Aimed at the UI Dashboard's 4–6 stat cards; NOT a
 * replacement for {@code /metrics} (Prometheus scrape endpoint), which
 * still exists unchanged.
 */
public class MetricsSummaryController {

    private static final Logger log = LoggerFactory.getLogger(MetricsSummaryController.class);

    private final HaMetrics metrics;
    private final JedisPool jedisPool;
    private final String fencingTokenKey;
    private final String changesStreamKey;

    public MetricsSummaryController(HaMetrics metrics, JedisPool jedisPool,
                                     String fencingTokenKey, String changesStreamKey) {
        this.metrics = metrics;
        this.jedisPool = jedisPool;
        this.fencingTokenKey = fencingTokenKey;
        this.changesStreamKey = changesStreamKey;
    }

    public void getSummary(Context ctx) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ts", System.currentTimeMillis());

        // From Micrometer (in-process)
        body.put("syncLagMs", metrics.syncLagMs.get());
        body.put("cdcLastPublishedTs", metrics.cdcLastPublishedTs.get());
        body.put("uiSessionActive", metrics.uiSessionActive.get());

        MeterRegistry reg = MetricsRegistry.get();
        body.put("cdcEventsPublishedTotal", counterValue(reg, "neo4j_ha_cdc_events_published_total"));
        body.put("syncEventsAppliedTotal", counterValue(reg, "neo4j_ha_sync_events_applied_total"));
        body.put("failoverSuccessTotal", counterValue(reg, "neo4j_ha_failover_success_total"));
        body.put("failoverFailedTotal", counterValue(reg, "neo4j_ha_failover_failed_total"));
        body.put("cdcPollErrorsTotal", counterValue(reg, "neo4j_ha_cdc_poll_errors_total"));

        // From Redis (cross-process, authoritative)
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                String tok = jedis.get(fencingTokenKey);
                body.put("fencingToken", tok == null ? 0L : Long.parseLong(tok));
            } catch (Exception e) {
                body.put("fencingToken", null);
            }
            try {
                body.put("changesStreamLen", jedis.xlen(changesStreamKey));
            } catch (Exception e) {
                body.put("changesStreamLen", null);
            }
        } catch (Exception e) {
            log.warn("metrics-summary: redis unavailable: {}", e.toString());
            body.put("fencingToken", null);
            body.put("changesStreamLen", null);
            body.put("redisAvailable", false);
        }

        ctx.json(body);
    }

    private static double counterValue(MeterRegistry registry, String name) {
        for (Meter m : registry.getMeters()) {
            if (m.getId().getName().equals(name) && m instanceof Counter c) {
                return c.count();
            }
        }
        // For composite counter-with-tags (ha_ui_login_total), sum all tagged variants:
        double sum = 0;
        boolean found = false;
        for (Meter m : registry.getMeters()) {
            if (m.getId().getName().equals(name) && m instanceof Counter c) {
                sum += c.count();
                found = true;
            }
        }
        return found ? sum : 0d;
    }
}
