package com.neo4j.ha.agent.http.auth;

public enum Role {
    ADMIN,
    VIEWER;

    public static Role parse(String s) {
        if (s == null || s.isBlank()) return ADMIN;
        return switch (s.trim().toLowerCase()) {
            case "viewer", "ro", "read-only" -> VIEWER;
            default -> ADMIN;
        };
    }

    public boolean canWrite() {
        return this == ADMIN;
    }
}
