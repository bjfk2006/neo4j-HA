package com.neo4j.ha.agent.bootstrap;

import com.neo4j.ha.common.metrics.HaMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for IndexInstaller's BUG-083 changes:
 *   - per-label UNIQUE constraint on `_elementId` (no standalone RANGE index
 *     for nodes; the constraint provisions an equivalent backing index)
 *   - orphan-`_elementId`-RANGE-index migration so an upgraded database can
 *     install the constraint without DBA intervention
 *   - dup-`_elementId` scan that drives the `dupElementIdNodes` gauge
 *
 * The Driver/Session API surface is mocked so the test runs without a Neo4j
 * instance. We assert on the literal Cypher strings issued by IndexInstaller
 * because those are the contract the operator runbook (and the dedup script)
 * relies on; format drift would silently break ops.
 */
class IndexInstallerTest {

    private Driver driver;
    private Session session;
    private HaMetrics metrics;

    /** Routes Cypher queries to programmable answers based on the query text. */
    private final Map<String, Function<String, Result>> stubs = new LinkedHashMap<>();
    private final List<String> executedQueries = new ArrayList<>();

    @BeforeEach
    void setUp() {
        driver = mock(Driver.class);
        session = mock(Session.class);
        metrics = new HaMetrics(new SimpleMeterRegistry());

        when(driver.session(any(SessionConfig.class))).thenReturn(session);
        // Auto-close in try-with-resources is a no-op against mock; nothing to stub.

        // Default stub for the no-parameter overload: every query records itself
        // and returns an empty result unless a stub matches.
        when(session.run(any(String.class))).thenAnswer(inv -> {
            String q = inv.getArgument(0);
            executedQueries.add(q);
            return resolve(q);
        });
        // Parameterised overload (Map params): IndexInstaller uses this for
        // SHOW INDEXES with $label. We record the query verbatim and route by
        // substring match just like the no-arg overload.
        when(session.run(any(String.class), anyMap())).thenAnswer(inv -> {
            String q = inv.getArgument(0);
            executedQueries.add(q);
            return resolve(q);
        });
    }

    private Result resolve(String q) {
        for (Map.Entry<String, Function<String, Result>> e : stubs.entrySet()) {
            if (q.contains(e.getKey())) {
                return e.getValue().apply(q);
            }
        }
        return emptyResult();
    }

    @Test
    void installsUniqueConstraintForEachUserLabelAndDoesNotPreCreateElementIdRangeIndex() {
        stubLabels("TestNode", "Movie", "_System");
        stubRelTypes(); // none
        stubDupScanReturns(0L);
        stubOrphanElementIdIndex(null); // no orphan to drop

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        // Per-label UNIQUE constraint must be issued for each user label
        // (system labels starting with `_` are skipped).
        assertTrue(executedQueries.stream().anyMatch(q -> q.contains(
            "CREATE CONSTRAINT uniq_elementid_TestNode IF NOT EXISTS")
            && q.contains("FOR (n:TestNode)")
            && q.contains("REQUIRE n._elementId IS UNIQUE")),
            "Expected UNIQUE constraint for TestNode; got: " + executedQueries);

        assertTrue(executedQueries.stream().anyMatch(q -> q.contains(
            "CREATE CONSTRAINT uniq_elementid_Movie IF NOT EXISTS")
            && q.contains("FOR (n:Movie)")
            && q.contains("REQUIRE n._elementId IS UNIQUE")),
            "Expected UNIQUE constraint for Movie; got: " + executedQueries);

        assertTrue(executedQueries.stream().noneMatch(q -> q.contains("uniq_elementid__System")),
            "System labels (`_*`) must be skipped: " + executedQueries);

        // BUG-083: must NOT pre-create a standalone RANGE index on _elementId
        // for nodes; the constraint creates its backing index. Verify by
        // inspecting node-shaped index creates only — relationship indexes
        // (e.g. `()-[r:T]-() ON (r._elementId)`) are still allowed because
        // we don't add UNIQUE constraints there.
        assertTrue(executedQueries.stream().noneMatch(q ->
            q.contains("CREATE RANGE INDEX") && q.contains("FOR (n:") && q.contains("ON (n._elementId)")),
            "Standalone _elementId node RANGE index must NOT be issued (it conflicts with " +
            "the UNIQUE constraint backing index); got: " + executedQueries);

        // The standalone _updated_at RANGE index must still be created — it's
        // unrelated to the constraint and required for CDC keyset polling.
        assertTrue(executedQueries.stream().anyMatch(q ->
            q.contains("CREATE RANGE INDEX") && q.contains("FOR (n:TestNode)") && q.contains("ON (n._updated_at)")),
            "Expected _updated_at RANGE index for TestNode; got: " + executedQueries);
    }

    @Test
    void dropsOrphanElementIdRangeIndexBeforeCreatingConstraint() {
        stubLabels("TestNode");
        stubRelTypes();
        stubDupScanReturns(0L);
        stubOrphanElementIdIndex("index_legacy_42"); // legacy non-constraint index exists

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        // The orphan must be dropped BEFORE the constraint creation so the
        // latter does not fail with "A constraint cannot be created until the
        // index has been dropped".
        int dropIdx = -1, createIdx = -1;
        for (int i = 0; i < executedQueries.size(); i++) {
            String q = executedQueries.get(i);
            if (q.contains("DROP INDEX index_legacy_42")) dropIdx = i;
            if (q.contains("CREATE CONSTRAINT uniq_elementid_TestNode")) createIdx = i;
        }
        assertTrue(dropIdx >= 0,
            "Expected DROP INDEX index_legacy_42 to be issued; got: " + executedQueries);
        assertTrue(createIdx > dropIdx,
            "DROP INDEX must precede CREATE CONSTRAINT; got DROP@" + dropIdx +
            " CREATE@" + createIdx + " in " + executedQueries);
    }

    @Test
    void noOrphanIndex_skipsDropAndStillCreatesConstraint() {
        stubLabels("TestNode");
        stubRelTypes();
        stubDupScanReturns(0L);
        stubOrphanElementIdIndex(null);

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        assertTrue(executedQueries.stream().noneMatch(q -> q.contains("DROP INDEX")),
            "No DROP INDEX should be issued when there is no orphan; got: " + executedQueries);
        assertTrue(executedQueries.stream().anyMatch(q ->
            q.contains("CREATE CONSTRAINT uniq_elementid_TestNode")),
            "CREATE CONSTRAINT must still be issued; got: " + executedQueries);
    }

    @Test
    void dupElementIdScanUpdatesGaugeWhenZero() {
        stubLabels("TestNode");
        stubRelTypes();
        stubDupScanReturns(0L);
        stubOrphanElementIdIndex(null);

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        assertEquals(0L, metrics.dupElementIdNodes.get());
    }

    @Test
    void dupElementIdScanUpdatesGaugeWhenNonZero() {
        stubLabels("TestNode", "Movie");
        stubRelTypes();
        stubDupScanReturns(2L);
        stubOrphanElementIdIndex(null);

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        // Both labels report 2 dup nodes each => total gauge = 4.
        assertEquals(4L, metrics.dupElementIdNodes.get(),
            "Gauge should be the SUM across labels; got " + metrics.dupElementIdNodes.get());
    }

    @Test
    void constraintFailureDoesNotCrashAndStillScansForDups() {
        stubLabels("TestNode");
        stubRelTypes();
        stubOrphanElementIdIndex(null);
        // CONSTRAINT creation throws (e.g. existing duplicates make Neo4j refuse).
        stubs.put("CREATE CONSTRAINT uniq_elementid_TestNode", q -> {
            throw new RuntimeException("Unable to create CONSTRAINT: existing duplicates");
        });
        stubDupScanReturns(2L);

        // Must NOT throw — bootstrap continues even if constraint creation fails.
        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        // The dup scan still ran and updated the gauge so operators see the
        // residue even on a dirty database.
        assertEquals(2L, metrics.dupElementIdNodes.get());
    }

    @Test
    void noUserLabels_skipsConstraintsAndKeepsGaugeZero() {
        stubLabels("_CDCDeleteEvent"); // only system labels
        stubRelTypes();
        // No dup scan invocations expected (no user labels), gauge stays 0.

        new IndexInstaller(metrics).ensureIndexes(driver, "neo4j");

        assertTrue(executedQueries.stream().noneMatch(q -> q.contains("CREATE CONSTRAINT")),
            "Expected no CREATE CONSTRAINT calls when only system labels exist; got: " + executedQueries);
        assertTrue(executedQueries.stream().noneMatch(q -> q.contains("DROP INDEX")),
            "No DROP INDEX should be issued when there is no user label; got: " + executedQueries);
        assertEquals(0L, metrics.dupElementIdNodes.get());
    }

    // -------------------------------------------------- stubbing helpers

    private void stubLabels(String... labels) {
        stubs.put("CALL db.labels()", q -> labelsResult(List.of(labels)));
    }

    private void stubRelTypes(String... relTypes) {
        stubs.put("CALL db.relationshipTypes()", q -> relTypesResult(List.of(relTypes)));
    }

    /** Each per-label dup scan returns the same `dup_nodes` count. */
    private void stubDupScanReturns(long perLabelCount) {
        stubs.put("WHERE n._elementId IS NOT NULL", q -> {
            // Only the dup-scan query has this exact prefix combined with the
            // count(*) aggregation; the index-creation queries do not call run
            // through this branch.
            if (q.contains("dup_nodes")) {
                return singleLongResult("dup_nodes", perLabelCount);
            }
            return emptyResult();
        });
    }

    /**
     * Stub for the orphan-index lookup. {@code orphanName == null} simulates
     * "no orphan present" (single() with no rows). A non-null name simulates
     * a legacy standalone _elementId RANGE index that must be dropped.
     */
    private void stubOrphanElementIdIndex(String orphanName) {
        stubs.put("SHOW INDEXES YIELD name, labelsOrTypes, properties, owningConstraint", q -> {
            if (orphanName == null) {
                return streamResult(List.of()); // empty stream → findFirst returns empty
            }
            return streamResult(List.of(orphanName));
        });
    }

    // -------------------------------------------------- mock Result builders

    private Result emptyResult() {
        Result r = mock(Result.class);
        ResultSummary s = mock(ResultSummary.class);
        when(r.consume()).thenReturn(s);
        when(r.list(any(Function.class))).thenReturn(List.of());
        when(r.stream()).thenReturn(java.util.stream.Stream.empty());
        return r;
    }

    private Result labelsResult(List<String> labels) {
        Result r = mock(Result.class);
        when(r.list(any(Function.class))).thenAnswer(inv -> {
            Function<Record, String> mapper = inv.getArgument(0);
            List<String> out = new ArrayList<>();
            for (String label : labels) {
                Record rec = mock(Record.class);
                Value v = mock(Value.class);
                when(v.asString()).thenReturn(label);
                when(rec.get("label")).thenReturn(v);
                out.add(mapper.apply(rec));
            }
            return out;
        });
        return r;
    }

    private Result relTypesResult(List<String> relTypes) {
        Result r = mock(Result.class);
        when(r.list(any(Function.class))).thenAnswer(inv -> {
            Function<Record, String> mapper = inv.getArgument(0);
            List<String> out = new ArrayList<>();
            for (String relType : relTypes) {
                Record rec = mock(Record.class);
                Value v = mock(Value.class);
                when(v.asString()).thenReturn(relType);
                when(rec.get("relationshipType")).thenReturn(v);
                out.add(mapper.apply(rec));
            }
            return out;
        });
        return r;
    }

    private Result singleLongResult(String key, long value) {
        Result r = mock(Result.class);
        Record rec = mock(Record.class);
        Value v = mock(Value.class);
        when(v.asLong()).thenReturn(value);
        when(rec.get(key)).thenReturn(v);
        when(r.single()).thenReturn(rec);
        return r;
    }

    /** Builds a Result whose stream() yields one Record per name with key "name". */
    private Result streamResult(List<String> names) {
        Result r = mock(Result.class);
        when(r.stream()).thenAnswer(inv -> names.stream().map(name -> {
            Record rec = mock(Record.class);
            Value v = mock(Value.class);
            when(v.asString()).thenReturn(name);
            when(rec.get("name")).thenReturn(v);
            return rec;
        }));
        return r;
    }
}
