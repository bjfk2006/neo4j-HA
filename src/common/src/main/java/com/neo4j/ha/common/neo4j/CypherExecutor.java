package com.neo4j.ha.common.neo4j;

import com.neo4j.ha.common.util.RetryUtil;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CypherExecutor {

    private static final Logger log = LoggerFactory.getLogger(CypherExecutor.class);

    private final Driver driver;
    private final String database;
    private final int maxRetries;
    private final long retryDelayMs;

    public CypherExecutor(Driver driver, String database, int maxRetries, long retryDelayMs) {
        this.driver = driver;
        this.database = database;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    public List<Record> executeRead(String cypher, Map<String, Object> params) {
        return RetryUtil.retry(maxRetries, retryDelayMs, TransientException.class, () -> {
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                return session.executeRead(tx -> tx.run(cypher, params).list());
            }
        });
    }

    public void executeWrite(String cypher, Map<String, Object> params) {
        RetryUtil.retry(maxRetries, retryDelayMs, TransientException.class, () -> {
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                session.executeWrite(tx -> {
                    tx.run(cypher, params).consume();
                    return null;
                });
            }
            return null;
        });
    }

    public <T> T executeWriteTransaction(Function<org.neo4j.driver.TransactionContext, T> work) {
        return RetryUtil.retry(maxRetries, retryDelayMs, TransientException.class, () -> {
            try (Session session = driver.session(SessionConfig.forDatabase(database))) {
                return session.executeWrite(work::apply);
            }
        });
    }

    public List<Record> executeReadNoRetry(String cypher, Map<String, Object> params) {
        try (Session session = driver.session(SessionConfig.forDatabase(database))) {
            return session.executeRead(tx -> tx.run(cypher, params).list());
        }
    }
}
