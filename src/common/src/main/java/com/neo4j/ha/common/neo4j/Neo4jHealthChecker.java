package com.neo4j.ha.common.neo4j;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Neo4jHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(Neo4jHealthChecker.class);

    public boolean checkTcp(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException e) {
            log.debug("TCP check failed for {}:{}", host, port);
            return false;
        }
    }

    public boolean checkBolt(Driver driver) {
        try {
            driver.verifyConnectivity();
            return true;
        } catch (Exception e) {
            log.debug("Bolt check failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean checkCypher(Driver driver, String database) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            session.run("RETURN 1").consume();
            return true;
        } catch (Exception e) {
            log.debug("Cypher check failed: {}", e.getMessage());
            return false;
        }
    }

    public boolean checkWrite(Driver driver, String database) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            session.executeWrite(tx -> {
                tx.run("CREATE (n:_HealthCheck) DELETE n").consume();
                return null;
            });
            return true;
        } catch (Exception e) {
            log.debug("Write check failed: {}", e.getMessage());
            return false;
        }
    }
}
