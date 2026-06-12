package com.neo4j.ha.sync.applier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IndexManagerTest {

    private IndexManager indexManager;
    private Session session;
    private Result result;

    @BeforeEach
    void setUp() {
        indexManager = new IndexManager();
        session = mock(Session.class);
        result = mock(Result.class);
        when(session.run(anyString())).thenReturn(result);
    }

    // --- sanitizeLabel tests ---

    @Test
    void sanitizeLabel_plainLabel_returnsUnchanged() {
        assertEquals("Person", IndexManager.sanitizeLabel("Person"));
    }

    @Test
    void sanitizeLabel_underscorePrefix_returnsUnchanged() {
        assertEquals("_internal", IndexManager.sanitizeLabel("_internal"));
    }

    @Test
    void sanitizeLabel_alphanumericWithUnderscore_returnsUnchanged() {
        assertEquals("My_Label_123", IndexManager.sanitizeLabel("My_Label_123"));
    }

    @Test
    void sanitizeLabel_specialChars_backtickEscaped() {
        assertEquals("`My Label`", IndexManager.sanitizeLabel("My Label"));
    }

    @Test
    void sanitizeLabel_hyphen_backtickEscaped() {
        assertEquals("`some-label`", IndexManager.sanitizeLabel("some-label"));
    }

    @Test
    void sanitizeLabel_startsWithDigit_backtickEscaped() {
        assertEquals("`1stLabel`", IndexManager.sanitizeLabel("1stLabel"));
    }

    @Test
    void sanitizeLabel_embeddedBackticks_doubledAndEscaped() {
        assertEquals("`my``label`", IndexManager.sanitizeLabel("my`label"));
    }

    // --- ensureIndex tests ---

    private static final String NODE_KEY = "node-01";

    @Test
    void ensureIndex_nullLabel_doesNotRunQuery() {
        indexManager.ensureIndex(session, NODE_KEY, null);
        verify(session, never()).run(anyString());
    }

    @Test
    void ensureIndex_blankLabel_doesNotRunQuery() {
        indexManager.ensureIndex(session, NODE_KEY, "   ");
        verify(session, never()).run(anyString());
    }

    @Test
    void ensureIndex_emptyLabel_doesNotRunQuery() {
        indexManager.ensureIndex(session, NODE_KEY, "");
        verify(session, never()).run(anyString());
    }

    @Test
    void ensureIndex_validLabel_runsCreateIndex() {
        indexManager.ensureIndex(session, NODE_KEY, "Person");
        verify(session).run(contains("Person"));
        verify(result).consume();
    }

    @Test
    void ensureIndex_calledTwice_onlyCreatesIndexOnce() {
        indexManager.ensureIndex(session, NODE_KEY, "Person");
        indexManager.ensureIndex(session, NODE_KEY, "Person");
        verify(session, times(1)).run(anyString());
    }

    @Test
    void ensureIndex_differentLabels_createsIndexForEach() {
        indexManager.ensureIndex(session, NODE_KEY, "Person");
        indexManager.ensureIndex(session, NODE_KEY, "Company");
        verify(session, times(2)).run(anyString());
    }

    @Test
    void ensureIndex_sameLabelDifferentNodes_createsIndexOnEach() {
        // C2 regression guard: per-node cache keys must prevent one node's success from
        // short-circuiting index creation on another node.
        indexManager.ensureIndex(session, "node-01", "Person");
        indexManager.ensureIndex(session, "node-02", "Person");
        verify(session, times(2)).run(anyString());
    }

    @Test
    void ensureIndex_exceptionDuringCreation_doesNotCache() {
        when(session.run(anyString())).thenThrow(new RuntimeException("connection lost"));

        indexManager.ensureIndex(session, NODE_KEY, "Broken");

        // Reset mock so next call succeeds
        reset(session);
        result = mock(Result.class);
        when(session.run(anyString())).thenReturn(result);

        // Should retry since it was not cached on failure
        indexManager.ensureIndex(session, NODE_KEY, "Broken");
        verify(session).run(anyString());
    }

    private static String contains(String substring) {
        return argThat(arg -> arg != null && arg.contains(substring));
    }
}
