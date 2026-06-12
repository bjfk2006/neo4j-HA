package com.neo4j.ha.sync.fullsync;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseCleaner {

    private static final Logger log = LoggerFactory.getLogger(DatabaseCleaner.class);

    private static final String DELETE_BATCH = """
        MATCH (n)
        WITH n LIMIT 10000
        DETACH DELETE n
        RETURN count(*) AS deleted
        """;

    public void clean(Driver driver, String database) {
        log.info("Cleaning database '{}' for full sync...", database);
        long totalDeleted = 0;

        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            while (true) {
                long deleted = session.executeWrite(tx ->
                    tx.run(DELETE_BATCH).single().get("deleted").asLong()
                );
                totalDeleted += deleted;
                if (deleted == 0) break;
                log.debug("Deleted {} nodes (total: {})", deleted, totalDeleted);
            }
        }

        log.info("Database clean complete: deleted {} nodes total", totalDeleted);
    }
}
