package com.neo4j.ha.common.neo4j;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Neo4jClientFactory {

    private static final Logger log = LoggerFactory.getLogger(Neo4jClientFactory.class);

    private final Map<String, Driver> drivers = new ConcurrentHashMap<>();
    private final int maxConnectionPoolSize;
    private final long connectionAcquisitionTimeoutMs;

    public Neo4jClientFactory(int maxConnectionPoolSize, long connectionAcquisitionTimeoutMs) {
        this.maxConnectionPoolSize = maxConnectionPoolSize;
        this.connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs;
    }

    public Driver getOrCreateDriver(String nodeId, String uri, String username, String password) {
        return drivers.computeIfAbsent(nodeId, id -> {
            Config config = Config.builder()
                .withMaxConnectionPoolSize(maxConnectionPoolSize)
                .withConnectionAcquisitionTimeout(connectionAcquisitionTimeoutMs, TimeUnit.MILLISECONDS)
                .withMaxTransactionRetryTime(5, TimeUnit.SECONDS)
                .build();
            Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password), config);
            log.info("Neo4j Driver created for node '{}' at {}", nodeId, uri);
            return driver;
        });
    }

    public Driver getDriver(String nodeId) {
        Driver driver = drivers.get(nodeId);
        if (driver == null) {
            throw new IllegalStateException("No driver found for node: " + nodeId);
        }
        return driver;
    }

    public void closeAll() {
        drivers.forEach((id, driver) -> {
            try {
                driver.close();
                log.info("Neo4j Driver closed for node '{}'", id);
            } catch (Exception e) {
                log.warn("Error closing driver for node '{}'", id, e);
            }
        });
        drivers.clear();
    }

    public void closeDriver(String nodeId) {
        Driver driver = drivers.remove(nodeId);
        if (driver != null) {
            driver.close();
            log.info("Neo4j Driver closed for node '{}'", nodeId);
        }
    }
}
