package com.neo4j.ha.agent.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.ha.agent.http.auth.AuthFilter;
import com.neo4j.ha.agent.http.auth.Principal;
import com.neo4j.ha.agent.http.auth.RateLimiter;
import com.neo4j.ha.agent.http.auth.SessionManager;
import com.neo4j.ha.agent.http.auth.UiAuditLog;
import com.neo4j.ha.agent.http.auth.UserStore;
import com.neo4j.ha.common.metrics.HaMetrics;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;

/**
 * Handles {@code POST /api/login}, {@code POST /api/logout}, and
 * {@code GET /api/me}. Cookie {@code ha_session} is set on success; HttpOnly
 * + SameSite=Strict. Add {@code Secure} when serving over HTTPS (controlled
 * by {@link #setSecureCookie}).
 */
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    /** Equalize response time on failure to mask username-existence timing. */
    private static final long FAILURE_DELAY_MS = 200L;

    private final UserStore userStore;
    private final SessionManager sessionManager;
    private final RateLimiter rateLimiter;
    private final UiAuditLog auditLog;
    private final HaMetrics metrics;
    private final ObjectMapper mapper = new ObjectMapper();
    private final long sessionTtlMs;

    private volatile boolean secureCookie = false;

    /** REVIEW-C4: peers allowed to set X-Forwarded-For. Empty = trust nobody. */
    private volatile List<String> trustedProxies = List.of();

    /**
     * REVIEW-C5: bound how many logins can be in their (deliberately slow)
     * bcrypt + failure-delay path at once.
     *
     * <p>{@code login} sleeps {@code FAILURE_DELAY_MS} on the request thread to
     * equalise timing, and bcrypt itself costs ~80 ms. Those are correct
     * defences against credential attacks, but they run on Jetty worker threads:
     * combined with the REVIEW-C4 rate-limit bypass, a few hundred concurrent
     * bad logins could occupy the whole pool and take {@code /health} and the
     * emergency {@code /cluster/switchover} endpoint down with it. Excess
     * attempts now get an immediate 429 instead of a parked thread.</p>
     */
    private static final int MAX_CONCURRENT_LOGINS = 8;
    private final Semaphore loginSlots = new Semaphore(MAX_CONCURRENT_LOGINS);

    public AuthController(UserStore userStore, SessionManager sessionManager,
                          RateLimiter rateLimiter, UiAuditLog auditLog,
                          HaMetrics metrics, long sessionTtlMs) {
        this.userStore = userStore;
        this.sessionManager = sessionManager;
        this.rateLimiter = rateLimiter;
        this.auditLog = auditLog;
        this.metrics = metrics;
        this.sessionTtlMs = sessionTtlMs;
    }

    public AuthController setSecureCookie(boolean secure) {
        this.secureCookie = secure;
        return this;
    }

    public AuthController setTrustedProxies(List<String> proxies) {
        this.trustedProxies = proxies == null ? List.of() : List.copyOf(proxies);
        return this;
    }

    public void login(Context ctx) {
        // REVIEW-C5: shed load rather than parking a Jetty thread.
        if (!loginSlots.tryAcquire()) {
            ctx.status(429).json(Map.of(
                "error", "too_many_requests",
                "message", "Too many concurrent login attempts; retry shortly."));
            return;
        }
        try {
            doLogin(ctx);
        } finally {
            loginSlots.release();
        }
    }

    private void doLogin(Context ctx) {
        String ip = clientIp(ctx, trustedProxies);
        String userAgent = headerOrEmpty(ctx, "User-Agent");

        // 0. RateLimiter: if IP currently locked, refuse without bcrypt.
        long lockedMs = rateLimiter.checkLockedRemainingMs(ip);
        if (lockedMs > 0) {
            metrics.uiLoginLocked.increment();
            auditLog.logLoginLocked(ip, lockedMs);
            ctx.status(423).json(Map.of(
                "error", "locked",
                "message", "Too many failed attempts. Try again later.",
                "retryAfterMs", lockedMs
            ));
            return;
        }

        // 1. Parse body
        String username;
        String password;
        try {
            JsonNode body = mapper.readTree(ctx.body());
            username = textOrNull(body, "username");
            password = textOrNull(body, "password");
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", "bad_request",
                "message", "Invalid JSON body"));
            return;
        }
        if (username == null || password == null
                || username.isBlank() || password.isBlank()) {
            ctx.status(400).json(Map.of("error", "bad_request",
                "message", "username and password are required"));
            return;
        }

        // 2. Verify
        Optional<UserStore.User> user = userStore.verify(username, password);
        if (user.isEmpty()) {
            rateLimiter.recordFailure(ip);
            metrics.uiLoginFailure.increment();
            auditLog.logLoginFailure(username, ip, "invalid_credentials");
            // Time-equalize: don't tell attacker whether bcrypt verified or not.
            sleepQuiet(FAILURE_DELAY_MS);
            ctx.status(401).json(Map.of("error", "unauthorized",
                "message", "Invalid credentials"));
            return;
        }

        // 3. Issue session
        var session = sessionManager.create(user.get().username(), user.get().role());
        rateLimiter.recordSuccess(ip);
        metrics.uiLoginSuccess.increment();
        metrics.uiSessionActive.set(sessionManager.activeCount());
        auditLog.logLoginSuccess(user.get().username(), ip, userAgent);

        // 4. Cookie + body
        setSessionCookie(ctx, session.token(), sessionTtlMs / 1000);
        ctx.json(Map.of(
            "username", user.get().username(),
            "role", user.get().role().name().toLowerCase(),
            "expiresAt", session.expiresAtMs()
        ));
    }

    public void logout(Context ctx) {
        Principal p = AuthFilter.principal(ctx);
        if (p != null && p.authKind() == Principal.AuthKind.SESSION
                && p.sessionToken() != null) {
            sessionManager.revoke(p.sessionToken());
            auditLog.logLogout(p.username());
            metrics.uiSessionActive.set(sessionManager.activeCount());
        }
        clearSessionCookie(ctx);
        ctx.status(204);
    }

    public void me(Context ctx) {
        Principal p = AuthFilter.principal(ctx);
        if (p == null) {
            ctx.status(401).json(Map.of("error", "unauthorized"));
            return;
        }
        // For token-based callers, expiresAt is not meaningful — report -1.
        long expiresAt = -1L;
        if (p.authKind() == Principal.AuthKind.SESSION) {
            var s = sessionManager.lookup(p.sessionToken());
            if (s.isPresent()) expiresAt = s.get().expiresAtMs();
        }
        ctx.json(Map.of(
            "username", p.username(),
            "role", p.role().name().toLowerCase(),
            "authKind", p.authKind().name().toLowerCase(),
            "expiresAt", expiresAt
        ));
    }

    // === helpers ===

    private void setSessionCookie(Context ctx, String token, long maxAgeSeconds) {
        StringBuilder sb = new StringBuilder();
        sb.append(AuthFilter.COOKIE_NAME).append("=").append(token);
        sb.append("; Path=/");
        sb.append("; HttpOnly");
        sb.append("; SameSite=Strict");
        sb.append("; Max-Age=").append(maxAgeSeconds);
        if (secureCookie) sb.append("; Secure");
        ctx.header("Set-Cookie", sb.toString());
    }

    private void clearSessionCookie(Context ctx) {
        StringBuilder sb = new StringBuilder();
        sb.append(AuthFilter.COOKIE_NAME).append("=");
        sb.append("; Path=/");
        sb.append("; HttpOnly");
        sb.append("; SameSite=Strict");
        sb.append("; Max-Age=0");
        if (secureCookie) sb.append("; Secure");
        ctx.header("Set-Cookie", sb.toString());
    }

    /**
     * REVIEW-C4: {@code X-Forwarded-For} is attacker-controlled unless the
     * immediate peer is a proxy we trust.
     *
     * <p>This value keys the login rate limiter, the IP lockout table and the
     * audit trail. Taking it unconditionally from the header meant:</p>
     * <ul>
     *   <li><b>Rate limiting was trivially bypassable</b> — a different forged
     *       XFF per request means failures never accumulate against one key, so
     *       online password brute force ran unthrottled against the admin UI
     *       (which is enabled on the HK deployment);</li>
     *   <li><b>the limiter could be disabled outright</b> — once
     *       {@code RateLimiter.windows} exceeds MAX_TRACKED_IPS it stops
     *       recording failures at all, and forged IPs fill it on demand;</li>
     *   <li><b>{@code lockedUntil} grows without bound</b> — entries are only
     *       evicted when that same IP is seen again, so forged keys accumulate;</li>
     *   <li><b>the audit log recorded whatever the attacker typed.</b></li>
     * </ul>
     *
     * <p>Now the header is honoured only when the direct peer matches
     * {@code admin.ui.trustedProxies}; otherwise the socket address wins.</p>
     */
    static String clientIp(Context ctx, List<String> trustedProxies) {
        String peer = ctx.ip();
        if (trustedProxies == null || trustedProxies.isEmpty()) return peer;
        if (!isTrustedProxy(peer, trustedProxies)) return peer;

        String fwd = ctx.header("X-Forwarded-For");
        if (fwd == null || fwd.isBlank()) return peer;
        // First entry only (chained proxies).
        int comma = fwd.indexOf(',');
        String claimed = (comma < 0 ? fwd : fwd.substring(0, comma)).trim();
        return claimed.isEmpty() ? peer : claimed;
    }

    /** Exact-IP or CIDR match against the configured trusted-proxy list. */
    static boolean isTrustedProxy(String peer, List<String> trustedProxies) {
        if (peer == null) return false;
        for (String entry : trustedProxies) {
            if (entry == null || entry.isBlank()) continue;
            String e = entry.trim();
            if (e.equals(peer)) return true;
            int slash = e.indexOf('/');
            if (slash < 0) continue;
            try {
                if (cidrMatches(peer, e.substring(0, slash),
                        Integer.parseInt(e.substring(slash + 1)))) {
                    return true;
                }
            } catch (Exception ignored) { /* malformed entry — treat as no match */ }
        }
        return false;
    }

    private static boolean cidrMatches(String ip, String network, int prefixLen) throws Exception {
        byte[] a = InetAddress.getByName(ip).getAddress();
        byte[] b = InetAddress.getByName(network).getAddress();
        if (a.length != b.length) return false;
        if (prefixLen < 0 || prefixLen > a.length * 8) return false;
        int fullBytes = prefixLen / 8;
        for (int i = 0; i < fullBytes; i++) {
            if (a[i] != b[i]) return false;
        }
        int remaining = prefixLen % 8;
        if (remaining == 0) return true;
        int mask = (0xFF << (8 - remaining)) & 0xFF;
        return (a[fullBytes] & mask) == (b[fullBytes] & mask);
    }

    private static String headerOrEmpty(Context ctx, String name) {
        String v = ctx.header(name);
        return v == null ? "" : v;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
