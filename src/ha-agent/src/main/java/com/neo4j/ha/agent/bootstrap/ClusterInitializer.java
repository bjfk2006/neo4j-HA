package com.neo4j.ha.agent.bootstrap;

import com.neo4j.ha.agent.lifecycle.ClusterStateManager;
import com.neo4j.ha.agent.registry.NodeRegistry;
import com.neo4j.ha.common.config.HaConfig;
import com.neo4j.ha.common.model.*;
import com.neo4j.ha.common.neo4j.Neo4jClientFactory;
import com.neo4j.ha.common.neo4j.Neo4jHealthChecker;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ClusterInitializer {

    private static final Logger log = LoggerFactory.getLogger(ClusterInitializer.class);

    private final HaConfig config;
    private final Neo4jClientFactory neo4jFactory;
    private final ClusterStateManager clusterState;
    private final NodeRegistry nodeRegistry;
    private final Neo4jHealthChecker healthChecker;

    public ClusterInitializer(HaConfig config, Neo4jClientFactory neo4jFactory,
                               ClusterStateManager clusterState, NodeRegistry nodeRegistry) {
        this.config = config;
        this.neo4jFactory = neo4jFactory;
        this.clusterState = clusterState;
        this.nodeRegistry = nodeRegistry;
        this.healthChecker = new Neo4jHealthChecker();
    }

    public void init() {
        log.info("Initializing cluster with {} nodes", config.cluster().nodes().size());

        // Try to restore state from Redis
        Map<String, NodeInfo> savedState = nodeRegistry.loadAll();

        for (HaConfig.NodeConfig nodeConfig : config.cluster().nodes()) {
            // Create Neo4j driver
            Driver driver = neo4jFactory.getOrCreateDriver(
                nodeConfig.id(),
                nodeConfig.neo4j().uri(),
                nodeConfig.neo4j().username(),
                nodeConfig.neo4j().password()
            );

            // Check connectivity
            boolean reachable = healthChecker.checkBolt(driver);

            // Determine role
            NodeRole role;
            if (savedState.containsKey(nodeConfig.id())) {
                role = savedState.get(nodeConfig.id()).role();
                log.info("Restored node {} role from registry: {}", nodeConfig.id(), role);
            } else {
                role = NodeRole.valueOf(nodeConfig.role().toUpperCase());
            }

            NodeServiceState serviceState = reachable ? NodeServiceState.SYNCING : NodeServiceState.OFFLINE;
            NodeHealth health = reachable ? NodeHealth.HEALTHY : NodeHealth.DOWN;

            NodeInfo nodeInfo = new NodeInfo(
                nodeConfig.id(), role, nodeConfig.neo4j().uri(),
                health, serviceState, 0, System.currentTimeMillis(), false
            );

            clusterState.addNode(nodeConfig.id(), nodeInfo, driver);
            nodeRegistry.register(nodeInfo);

            log.info("Node {} initialized: role={}, health={}, reachable={}",
                nodeConfig.id(), role, health, reachable);
        }

        log.info("Cluster initialized. Primary: {}", clusterState.getPrimaryNodeId());
    }
}
