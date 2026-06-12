package com.neo4j.ha.common.config;

import com.neo4j.ha.common.config.HaConfig.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConfigValidatorTest {

    // ----- helper builders -----

    private static NodeConfig node(String id, String role, String uri) {
        Neo4jConnectionConfig neo4j = uri != null
                ? new Neo4jConnectionConfig(uri, "neo4j", "password", "neo4j")
                : null;
        return new NodeConfig(id, role, neo4j);
    }

    private static HaConfig validConfig() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", "bolt://localhost:7687"),
                node("node-2", "secondary", "bolt://localhost:7688")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        return new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);
    }

    // ----- tests -----

    @Test
    void validConfigPassesValidation() {
        List<String> errors = ConfigValidator.validate(validConfig());
        assertTrue(errors.isEmpty(), () -> "Expected no errors but got: " + errors);
    }

    @Test
    void missingClusterConfigReturnsError() {
        HaConfig config = new HaConfig(null, null, null, null, null, null, null, null, null, null, null, null, null);
        List<String> errors = ConfigValidator.validate(config);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("cluster configuration is required"));
    }

    @Test
    void wrongPrimaryCountReturnsError_zeroPrimaries() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "secondary", "bolt://localhost:7687"),
                node("node-2", "secondary", "bolt://localhost:7688")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("exactly one primary")));
    }

    @Test
    void wrongPrimaryCountReturnsError_twoPrimaries() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", "bolt://localhost:7687"),
                node("node-2", "primary", "bolt://localhost:7688")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("exactly one primary") && e.contains("2")));
    }

    @Test
    void blankNodeIdReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("", "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("id is required")));
    }

    @Test
    void nullNodeIdReturnsError() {
        List<NodeConfig> nodes = List.of(
                node(null, "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("id is required")));
    }

    @Test
    void invalidNodeIdCharactersReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("node@1!", "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("invalid characters")));
    }

    @Test
    void missingNeo4jUriReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", null)
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("neo4j.uri is required")));
    }

    @Test
    void missingRedisReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, null, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("redis configuration is required")));
    }

    @Test
    void missingStreamReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        HaConfig config = new HaConfig(cluster, redis, null, null, null, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("stream.changes is required")));
    }

    @Test
    void missingStreamChangesReturnsError() {
        List<NodeConfig> nodes = List.of(
                node("node-1", "primary", "bolt://localhost:7687")
        );
        ClusterConfig cluster = new ClusterConfig(nodes, null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig(null, "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("stream.changes is required")));
    }

    @Test
    void emptyNodesListReturnsError() {
        ClusterConfig cluster = new ClusterConfig(List.of(), null, null);
        RedisConfig redis = new RedisConfig("standalone", null, null, 0, "5s", null);
        StreamConfig stream = new StreamConfig("ha:changes", "ha:fullsync", 10000, "MAXLEN", null, null);
        HaConfig config = new HaConfig(cluster, redis, null, null, stream, null, null, null, null, null, null, null, null);

        List<String> errors = ConfigValidator.validate(config);
        assertTrue(errors.stream().anyMatch(e -> e.contains("at least one node")));
    }
}
