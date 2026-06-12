package com.neo4j.ha.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record HaConfig(
    ClusterConfig cluster,
    RedisConfig redis,
    CdcConfig cdc,
    SyncConfig sync,
    StreamConfig stream,
    FullSyncConfig fullsync,
    ServiceStateConfig serviceState,
    FailoverConfig failover,
    BackupConfig backup,
    RegistryConfig registry,
    AdminConfig admin,
    MonitoringConfig monitoring,
    BufferConfig buffer
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ClusterConfig(
        List<NodeConfig> nodes,
        Neo4jPoolConfig neo4j,
        HaProxyConfig haproxy
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record NodeConfig(
        String id,
        String role,
        Neo4jConnectionConfig neo4j
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Neo4jConnectionConfig(
        String uri,
        String username,
        String password,
        String database
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Neo4jPoolConfig(
        int maxConnectionPoolSize,
        String connectionAcquisitionTimeout
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HaProxyConfig(
        List<HaProxyInstanceConfig> instances,
        String primaryBackend,
        String readBackend,
        String stateSyncInterval
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HaProxyInstanceConfig(
        String id,
        String adminSocket
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RedisConfig(
        String mode,
        StandaloneConfig standalone,
        String password,
        int database,
        String timeout,
        PoolConfig pool
    ) {
        public long timeoutMs() {
            return parseDuration(timeout);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StandaloneConfig(String host, int port) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PoolConfig(int maxTotal, int maxIdle) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CdcConfig(
        boolean enabled,
        String mode,
        PollConfig poll,
        String timestampField,
        String createdAtField,
        String elementIdField,
        CacheConfig cache,
        IndexConfig index
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PollConfig(String interval, int batchSize, String timeout) {
        public long intervalMs() { return parseDuration(interval); }
        public long timeoutMs() { return parseDuration(timeout); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CacheConfig(int maxSize) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record IndexConfig(boolean autoCreate, boolean skipSystemLabels) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SyncConfig(
        ConsumerConfig consumer,
        ApplyConfig apply,
        DuplicateDetectorConfig duplicateDetector
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ConsumerConfig(String group, int batchSize, String blockTimeout) {
        public long blockTimeoutMs() { return parseDuration(blockTimeout); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApplyConfig(String mode, boolean batchCommit, int maxRetries, String retryDelay) {
        public long retryDelayMs() { return parseDuration(retryDelay); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DuplicateDetectorConfig(int maxSize) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StreamConfig(
        String changes,
        String fullsync,
        /**
         * Hard upper bound on stream length, enforced by every XADD via
         * MAXLEN ~ N. Last-resort protection against Redis OOM if a standby
         * is down for longer than the retention window could tolerate.
         */
        long maxLen,
        /**
         * "approximate" (default) — use ~ prefix for XADD MAXLEN (Redis 7 keeps
         * trim on radix-tree node boundaries; cheap). "exact" would drop the ~.
         */
        String trimStrategy,
        /**
         * Interval between background maintenance runs that execute
         * `XTRIM MINID safe_cutoff`. Safe because it never trims messages
         * still needed by any standby consumer group. Empty/null disables.
         */
        String maintenanceInterval,
        /**
         * Additional time margin subtracted from "oldest consumer position"
         * before computing the XTRIM MINID cutoff. Gives headroom in case a
         * standby falls briefly behind between scans.
         */
        String retentionSafetyWindow
    ) {
        public long maintenanceIntervalMs() {
            return maintenanceInterval == null || maintenanceInterval.isBlank()
                    ? 0L : parseDuration(maintenanceInterval);
        }
        public long retentionSafetyWindowMs() {
            return retentionSafetyWindow == null || retentionSafetyWindow.isBlank()
                    ? 300_000L : parseDuration(retentionSafetyWindow); // default 5 min
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FullSyncConfig(int batchSize, int throttleMs) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ServiceStateConfig(long syncLagThreshold, String stableDuration, String checkInterval) {
        public long stableDurationMs() { return parseDuration(stableDuration); }
        public long checkIntervalMs() { return parseDuration(checkInterval); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FailoverConfig(
        HealthCheckConfig healthCheck,
        FencingTokenConfig fencingToken,
        String drainTimeout,
        String confirmationWait,
        String minInterval,
        int maxAutoPerHour
    ) {
        public long drainTimeoutMs() { return parseDuration(drainTimeout); }
        public long confirmationWaitMs() { return parseDuration(confirmationWait); }
        public long minIntervalMs() { return parseDuration(minInterval); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HealthCheckConfig(String interval, String timeout, int failThreshold, int successThreshold) {
        public long intervalMs() { return parseDuration(interval); }
        public long timeoutMs() { return parseDuration(timeout); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FencingTokenConfig(String key) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BackupConfig(String maxDuration, String checkpointKey) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RegistryConfig(String key, String updateInterval) {
        public long updateIntervalMs() { return parseDuration(updateInterval); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AdminConfig(int port, AuthConfig auth, UiConfig ui) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AuthConfig(String type, String token) {}

    /**
     * Optional Web UI configuration. When {@code enabled} is false or this section
     * is omitted entirely, the admin HTTP server only serves the legacy
     * {@code X-Admin-Token} backed REST API (backward-compatible behavior).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UiConfig(
        Boolean enabled,
        List<UiUserConfig> users,
        UiSessionConfig session,
        UiRateLimitConfig rateLimit
    ) {
        public boolean isEnabled() { return Boolean.TRUE.equals(enabled); }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UiUserConfig(
        String username,
        String passwordHash,
        String role
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UiSessionConfig(String ttl, Integer maxPerUser) {
        public long ttlMs() { return ttl == null ? 8 * 3_600_000L : parseDuration(ttl); }
        public int maxPerUserOrDefault() { return maxPerUser == null ? 3 : maxPerUser; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record UiRateLimitConfig(Integer maxFailuresPerMinute, String lockDuration) {
        public int maxFailuresPerMinuteOrDefault() {
            return maxFailuresPerMinute == null ? 5 : maxFailuresPerMinute;
        }
        public long lockDurationMs() {
            return lockDuration == null ? 10 * 60_000L : parseDuration(lockDuration);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record MonitoringConfig(PrometheusConfig prometheus, LoggingConfig logging) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PrometheusConfig(boolean enabled, int port, String path) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record LoggingConfig(String level, String format, String file, String maxSize, int maxHistory) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BufferConfig(String dir, String maxFileSize, int maxFiles) {}

    public static long parseDuration(String value) {
        if (value == null || value.isEmpty()) return 0;
        value = value.trim().toLowerCase();
        if (value.endsWith("ms")) {
            return Long.parseLong(value.replace("ms", "").trim());
        } else if (value.endsWith("s")) {
            return Long.parseLong(value.replace("s", "").trim()) * 1000;
        } else if (value.endsWith("m")) {
            return Long.parseLong(value.replace("m", "").trim()) * 60_000;
        } else if (value.endsWith("h")) {
            return Long.parseLong(value.replace("h", "").trim()) * 3_600_000;
        }
        return Long.parseLong(value);
    }
}
