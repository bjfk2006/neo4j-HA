package com.neo4j.ha.common.config;

import java.util.ArrayList;
import java.util.List;

public class ConfigValidator {

    public static List<String> validate(HaConfig config) {
        List<String> errors = new ArrayList<>();

        if (config.cluster() == null) {
            errors.add("cluster configuration is required");
            return errors;
        }
        if (config.cluster().nodes() == null || config.cluster().nodes().isEmpty()) {
            errors.add("cluster.nodes must contain at least one node");
        } else {
            long primaryCount = config.cluster().nodes().stream()
                .filter(n -> "primary".equalsIgnoreCase(n.role()))
                .count();
            if (primaryCount != 1) {
                errors.add("cluster.nodes must contain exactly one primary node, found: " + primaryCount);
            }
            for (var node : config.cluster().nodes()) {
                if (node.id() == null || node.id().isBlank()) {
                    errors.add("cluster.nodes[].id is required");
                } else if (!node.id().matches("[a-zA-Z0-9_.-]+")) {
                    errors.add("cluster.nodes[].id contains invalid characters (allowed: a-z, A-Z, 0-9, _, ., -): " + node.id());
                }
                if (node.neo4j() == null || node.neo4j().uri() == null) {
                    errors.add("cluster.nodes[" + node.id() + "].neo4j.uri is required");
                }
            }
        }

        if (config.redis() == null) {
            errors.add("redis configuration is required");
        }

        if (config.stream() == null || config.stream().changes() == null) {
            errors.add("stream.changes is required");
        }

        // admin.ui is optional, but when enabled, validate users list.
        if (config.admin() != null && config.admin().ui() != null && config.admin().ui().isEnabled()) {
            var ui = config.admin().ui();
            if (ui.users() == null || ui.users().isEmpty()) {
                errors.add("admin.ui.enabled is true but admin.ui.users is empty");
            } else {
                for (var u : ui.users()) {
                    if (u.username() == null || u.username().isBlank()) {
                        errors.add("admin.ui.users[].username is required");
                    }
                    if (u.passwordHash() == null || u.passwordHash().isBlank()) {
                        errors.add("admin.ui.users[" + u.username() + "].passwordHash is required");
                    } else if (!u.passwordHash().startsWith("$2")) {
                        errors.add("admin.ui.users[" + u.username() + "].passwordHash must be a bcrypt hash (starts with $2)");
                    }
                    if (u.role() != null && !u.role().equalsIgnoreCase("admin") && !u.role().equalsIgnoreCase("viewer")) {
                        errors.add("admin.ui.users[" + u.username() + "].role must be 'admin' or 'viewer'");
                    }
                }
            }
        }

        return errors;
    }
}
