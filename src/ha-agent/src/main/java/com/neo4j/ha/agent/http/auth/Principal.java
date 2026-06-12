package com.neo4j.ha.agent.http.auth;

/**
 * Represents the authenticated caller of a single HTTP request. Stored on
 * the Javalin {@code Context} attribute "principal" by {@link AuthFilter}
 * and consumed by RBAC-gated handlers + {@code UiAuditLog} for actor field.
 *
 * <p>{@code authKind} distinguishes the two auth tracks:
 * <ul>
 *   <li>{@link AuthKind#SESSION} — cookie-backed UI login</li>
 *   <li>{@link AuthKind#TOKEN}   — legacy {@code X-Admin-Token} /
 *       {@code Authorization: Bearer} header (always {@link Role#ADMIN})</li>
 * </ul>
 */
public final class Principal {
    public enum AuthKind { SESSION, TOKEN }

    private final String username;
    private final Role role;
    private final AuthKind authKind;
    private final String sessionToken; // null for TOKEN auth

    public Principal(String username, Role role, AuthKind authKind, String sessionToken) {
        this.username = username;
        this.role = role;
        this.authKind = authKind;
        this.sessionToken = sessionToken;
    }

    public static Principal forToken() {
        return new Principal("admin-token", Role.ADMIN, AuthKind.TOKEN, null);
    }

    public static Principal forSession(SessionManager.Session s) {
        return new Principal(s.username(), s.role(), AuthKind.SESSION, s.token());
    }

    public String username() { return username; }
    public Role role() { return role; }
    public AuthKind authKind() { return authKind; }
    public String sessionToken() { return sessionToken; }
    public boolean canWrite() { return role.canWrite(); }

    public String displayActor() {
        return authKind == AuthKind.TOKEN ? "token" : username;
    }
}
