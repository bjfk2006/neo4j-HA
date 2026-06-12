package com.neo4j.ha.agent.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * In-memory session table. Session tokens are 32 random bytes (256 bits)
 * base64url-encoded, opaque to clients. Sessions are evicted on TTL expiry
 * by a single-threaded scheduled task; eviction also runs synchronously on
 * every lookup to make TTL deterministic in tests.
 *
 * <p>Sliding renewal: when a session's remaining lifetime drops below
 * {@code SLIDING_THRESHOLD_MS}, a successful {@code lookup} extends it back
 * to {@code ttlMs}.
 *
 * <p>Per-user concurrency cap: a user may have at most {@code maxPerUser}
 * active sessions. When the cap is reached, the OLDEST session for that
 * user is evicted before the new one is created (LRU on user).</p>
 */
public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);
    private static final SecureRandom RNG = new SecureRandom();
    private static final int TOKEN_BYTES = 32;

    /** When remaining < 25% of TTL, sliding-renew on next lookup. */
    private static final double SLIDING_RENEW_THRESHOLD_RATIO = 0.25;

    /** Hard ceiling on total active sessions (defence vs. unbounded growth). */
    private static final int MAX_SESSIONS = 10_000;

    public record Session(
        String token,
        String username,
        Role role,
        long createdAtMs,
        long expiresAtMs
    ) {}

    private final long ttlMs;
    private final int maxPerUser;
    private final Supplier<Long> clock;

    private final ConcurrentHashMap<String, Session> byToken = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public SessionManager(long ttlMs, int maxPerUser) {
        this(ttlMs, maxPerUser, System::currentTimeMillis, true);
    }

    /** Test-only constructor: inject clock, optionally disable scheduler. */
    SessionManager(long ttlMs, int maxPerUser, Supplier<Long> clock, boolean startScheduler) {
        this.ttlMs = ttlMs;
        this.maxPerUser = Math.max(1, maxPerUser);
        this.clock = clock;
        if (startScheduler) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "session-evictor");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::evictExpired,
                ttlMs / 4, ttlMs / 4, TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
        }
    }

    public Session create(String username, Role role) {
        long now = clock.get();
        // Enforce per-user cap before inserting.
        evictOldestForUserIfOverCap(username);
        if (byToken.size() >= MAX_SESSIONS) {
            log.warn("SessionManager at hard cap ({}); evicting oldest", MAX_SESSIONS);
            evictOldestGlobal();
        }
        String token = randomToken();
        Session s = new Session(token, username, role, now, now + ttlMs);
        byToken.put(token, s);
        return s;
    }

    public Optional<Session> lookup(String token) {
        if (token == null || token.isEmpty()) return Optional.empty();
        Session s = byToken.get(token);
        if (s == null) return Optional.empty();
        long now = clock.get();
        if (s.expiresAtMs <= now) {
            byToken.remove(token, s);
            return Optional.empty();
        }
        // Sliding renewal: if we're within the last renewal-threshold of life,
        // bump expiry. We do NOT change the token (cookie stays valid as-is).
        long remaining = s.expiresAtMs - now;
        if (remaining < ttlMs * SLIDING_RENEW_THRESHOLD_RATIO) {
            Session renewed = new Session(s.token, s.username, s.role, s.createdAtMs, now + ttlMs);
            byToken.replace(token, s, renewed);
            return Optional.of(renewed);
        }
        return Optional.of(s);
    }

    public boolean revoke(String token) {
        if (token == null) return false;
        return byToken.remove(token) != null;
    }

    public int revokeAllForUser(String username) {
        if (username == null) return 0;
        int[] count = {0};
        byToken.entrySet().removeIf(e -> {
            if (username.equals(e.getValue().username)) { count[0]++; return true; }
            return false;
        });
        return count[0];
    }

    public int activeCount() { return byToken.size(); }

    public void shutdown() {
        if (scheduler != null) scheduler.shutdownNow();
    }

    // === internals ===

    private void evictExpired() {
        long now = clock.get();
        byToken.entrySet().removeIf(e -> e.getValue().expiresAtMs <= now);
    }

    private void evictOldestForUserIfOverCap(String username) {
        long count = byToken.values().stream().filter(s -> username.equals(s.username)).count();
        if (count < maxPerUser) return;
        // Remove oldest (smallest createdAtMs).
        byToken.values().stream()
            .filter(s -> username.equals(s.username))
            .min((a, b) -> Long.compare(a.createdAtMs, b.createdAtMs))
            .ifPresent(s -> byToken.remove(s.token));
    }

    private void evictOldestGlobal() {
        byToken.values().stream()
            .min((a, b) -> Long.compare(a.createdAtMs, b.createdAtMs))
            .ifPresent(s -> byToken.remove(s.token));
    }

    private static String randomToken() {
        byte[] buf = new byte[TOKEN_BYTES];
        RNG.nextBytes(buf);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
    }
}
