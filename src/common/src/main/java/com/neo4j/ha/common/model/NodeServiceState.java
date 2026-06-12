package com.neo4j.ha.common.model;

/**
 * Node service state - determines whether data is ready and the node can serve traffic.
 * Orthogonal to NodeHealth (process liveness); both must be satisfied for traffic routing.
 *
 * State transitions:
 *   OFFLINE -> SYNCING: Neo4j process starts, HA Agent connects successfully
 *   SYNCING -> ONLINE:  sync lag < syncLagThreshold for stableDuration
 *   ONLINE  -> SYNCING: Full sync triggered (data being rebuilt)
 *   ONLINE  -> OFFLINE: Node goes down
 *   SYNCING -> OFFLINE: Node goes down
 */
public enum NodeServiceState {
    OFFLINE,
    SYNCING,
    ONLINE
}
