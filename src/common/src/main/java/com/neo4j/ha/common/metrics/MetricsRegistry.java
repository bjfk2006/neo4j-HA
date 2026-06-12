package com.neo4j.ha.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class MetricsRegistry {

    private static volatile PrometheusMeterRegistry registry;

    public static PrometheusMeterRegistry init() {
        if (registry == null) {
            synchronized (MetricsRegistry.class) {
                if (registry == null) {
                    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                }
            }
        }
        return registry;
    }

    public static PrometheusMeterRegistry get() {
        if (registry == null) {
            return init();
        }
        return registry;
    }

    public static MeterRegistry getMeterRegistry() {
        return get();
    }

    public static String scrape() {
        return get().scrape();
    }
}
