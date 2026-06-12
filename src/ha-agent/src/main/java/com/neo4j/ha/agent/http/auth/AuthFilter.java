package com.neo4j.ha.agent.http.auth;

import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Javalin before-handler enforcing the two-track admin auth:
 * <ol>
 *   <li>Path whitelist (login / health / metrics / static assets)</li>
 *   <li>{@code X-Admin-Token} header OR {@code Authorization: Bearer &lt;token&gt;}
 *       — backward compatible with curl scripts; treated as
 *       {@link Role#ADMIN}.</li>
 *   <li>Cookie {@code ha_session} → {@link SessionManager#lookup} →
 *       role-bound {@link Principal}.</li>
 *   <li>Otherwise 401.</li>
 * </ol>
 *
 * <p>RBAC for write operations: writers call
 * {@link #requireWriter(Context)} from their own handler. This is one extra
 * line per endpoint but keeps the filter stateless.</p>
 */
public class AuthFilter {

    private static final Logger log = LoggerFactory.getLogger(AuthFilter.class);

    public static final String COOKIE_NAME = "ha_session";
    public static final String CTX_PRINCIPAL = "principal";

    /**
     * API path prefixes that require authentication. Anything OUTSIDE these
     * prefixes (static assets, SPA routes like /login /dashboard /operations
     * /audit served as index.html via Javalin's spaRoot fallback) is allowed
     * through unchecked — the front-end router takes over from there.
     */
    private static final List<String> AUTH_REQUIRED_PREFIXES = List.of(
        "/api/",
        "/cluster/"
    );

    /** Auth-required paths that are nonetheless whitelisted (the login endpoint itself). */
    private static final List<String> AUTH_REQUIRED_EXEMPT = List.of(
        "/api/login"
    );

    /** Always public regardless of prefix (health probes, Prometheus scrape). */
    private static final List<String> ALWAYS_PUBLIC = List.of(
        "/health",
        "/metrics"
    );

    private final SessionManager sessionManager;
    private final String adminToken;

    public AuthFilter(SessionManager sessionManager, String adminToken) {
        this.sessionManager = sessionManager;
        this.adminToken = adminToken;
    }

    /** Register with Javalin: {@code app.before(filter::handle)}. */
    public void handle(Context ctx) {
        String path = ctx.path();

        // OPTIONS preflight always passes (CORS is OFF by default; same-origin SPA).
        if (ctx.method() == HandlerType.OPTIONS) return;

        // Always-public paths (probes, scrape).
        if (ALWAYS_PUBLIC.contains(path)) return;

        // Login endpoint itself is exempt despite being under /api/.
        if (AUTH_REQUIRED_EXEMPT.contains(path)) return;

        // Anything OUTSIDE the API prefixes is treated as a static asset or
        // SPA route — let Javalin's staticFiles + spaRoot fallback handle it.
        // No auth required at this stage; the SPA fetches /api/me and bounces
        // to /login itself when no session cookie exists.
        boolean requiresAuth = false;
        for (String prefix : AUTH_REQUIRED_PREFIXES) {
            if (path.startsWith(prefix)) { requiresAuth = true; break; }
        }
        if (!requiresAuth) return;

        // === API path: enforce one of the two auth tracks ===

        // Track 1: header token (X-Admin-Token or Authorization: Bearer)
        if (adminToken != null && !adminToken.isBlank()) {
            String headerToken = extractHeaderToken(ctx);
            if (headerToken != null && Objects.equals(headerToken, adminToken)) {
                ctx.attribute(CTX_PRINCIPAL, Principal.forToken());
                return;
            }
        }

        // Track 2: cookie session
        String cookie = ctx.cookie(COOKIE_NAME);
        if (cookie != null) {
            var s = sessionManager.lookup(cookie);
            if (s.isPresent()) {
                ctx.attribute(CTX_PRINCIPAL, Principal.forSession(s.get()));
                return;
            }
        }

        ctx.status(401).json(Map.of(
            "error", "unauthorized",
            "message", "Authentication required"
        ));
        // Block further handlers from running for this request.
        ctx.skipRemainingHandlers();
    }

    /** Called from inside write-operation handlers to enforce admin role. */
    public static void requireWriter(Context ctx) {
        Principal p = ctx.attribute(CTX_PRINCIPAL);
        if (p == null) {
            // Shouldn't happen — before-handler must run first. Defensive 401.
            ctx.status(401).json(Map.of("error", "unauthorized"));
            throw new io.javalin.http.UnauthorizedResponse();
        }
        if (!p.canWrite()) {
            ctx.status(403).json(Map.of(
                "error", "forbidden",
                "message", "Read-only user cannot perform write operations"
            ));
            throw new io.javalin.http.ForbiddenResponse();
        }
    }

    /** Convenience accessor for handlers that just want to know who's calling. */
    public static Principal principal(Context ctx) {
        return ctx.attribute(CTX_PRINCIPAL);
    }

    // === internals ===

    private String extractHeaderToken(Context ctx) {
        String xAdmin = ctx.header("X-Admin-Token");
        if (xAdmin != null && !xAdmin.isBlank()) return xAdmin;
        String auth = ctx.header("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            return auth.substring("Bearer ".length()).trim();
        }
        return null;
    }
}
