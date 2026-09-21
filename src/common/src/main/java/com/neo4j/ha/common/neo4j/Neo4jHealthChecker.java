package com.neo4j.ha.common.neo4j;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public class Neo4jHealthChecker {

    private static final Logger log = LoggerFactory.getLogger(Neo4jHealthChecker.class);

    private static final long DEFAULT_PROBE_TIMEOUT_MS = 5_000L;

    /**
     * REVIEW-H1: server-side timeout applied to every Cypher probe. Kept as an
     * instance property rather than a per-call argument so existing call sites
     * and their tests keep working unchanged.
     */
    private final long probeTimeoutMs;

    public Neo4jHealthChecker() {
        this(DEFAULT_PROBE_TIMEOUT_MS);
    }

    public Neo4jHealthChecker(long probeTimeoutMs) {
        this.probeTimeoutMs = probeTimeoutMs > 0 ? probeTimeoutMs : DEFAULT_PROBE_TIMEOUT_MS;
    }

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

    /**
     * REVIEW-H1: every Cypher probe now carries an explicit transaction timeout.
     *
     * <p>Without one, {@code session.run(...)} blocks for as long as the server
     * takes to answer — unbounded. "TCP connects but Bolt never answers" is a
     * real and common failure mode (long GC pause, stalled checkpoint, full
     * disk), and {@code checkTcp} deliberately passes in that state. Because
     * {@code HealthChecker} probes every node serially on ONE scheduled thread,
     * a single wedged node froze the whole health loop: the agent stopped
     * evaluating every other node, so a genuinely dead primary never reached
     * DOWN and failover never fired. The agent looked "hung" while doing
     * exactly what it was told to do.</p>
     */
    public boolean checkCypher(Driver driver, String database) {
        return checkCypher(driver, database, probeTimeoutMs);
    }

    public boolean checkCypher(Driver driver, String database, long timeoutMs) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            session.run("RETURN 1", txConfig(timeoutMs)).consume();
            return true;
        } catch (Exception e) {
            log.debug("Cypher check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Write probe. Runs in an <b>auto-commit</b> transaction with an explicit
     * timeout rather than {@code executeWrite}: the managed-transaction wrapper
     * retries internally for up to {@code maxTransactionRetryTime} (5 s here),
     * which on a sick primary means every probe costs 5 s on the single health
     * thread while the probe interval is 2 s — the loop can never keep up, and
     * DOWN detection is delayed by exactly the amount of time it matters most.
     *
     * <p>REVIEW-H2: the {@code _HealthCheck} label is now excluded by the APOC
     * node-delete and node-create triggers. It previously was not, so at the
     * default 2 s interval every primary emitted ~43k NODE_DELETED events per
     * day into the Redis stream, each one creating a {@code _CDCDeleteEvent}
     * transit node on the primary and a no-op delete on every standby — pure
     * replication noise that also kept the stream permanently non-empty.</p>
     */
    public boolean checkWrite(Driver driver, String database) {
        return checkWrite(driver, database, probeTimeoutMs);
    }

    public boolean checkWrite(Driver driver, String database, long timeoutMs) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            session.run("CREATE (n:" + WRITE_PROBE_LABEL + ") DELETE n", txConfig(timeoutMs))
                .consume();
            return true;
        } catch (Exception e) {
            log.debug("Write check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Label used by the L4 write probe. MUST stay in sync with the exclusion
     * lists in {@code ApocTriggerInstaller.NODE_DELETE_TRIGGER} and
     * {@code TIMESTAMP_CREATED_TRIGGER} — see REVIEW-H2.
     */
    public static final String WRITE_PROBE_LABEL = "_HealthCheck";

    private static TransactionConfig txConfig(long timeoutMs) {
        long ms = timeoutMs > 0 ? timeoutMs : DEFAULT_PROBE_TIMEOUT_MS;
        return TransactionConfig.builder()
            .withTimeout(Duration.ofMillis(ms))
            .build();
    }
}
