package com.neo4j.ha.common.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REVIEW-T2: these tests were written against the ORIGINAL "validate and
 * advance" contract, where {@code isValid()} both compared the token and
 * promoted {@code knownMaxToken}. BUG-037 deliberately removed that side
 * effect.
 *
 * <p>Why it was removed: the advance-on-validate behaviour was used as a DROP
 * filter in the sync path. At switchover the applier resumed with the new
 * token, and every event still queued in the stream carrying the PREVIOUS
 * token was then judged "stale" and discarded — 601 legitimate events lost,
 * reproducible with the load/switchover test. Those events had already been
 * admitted by the publisher-side Lua check ({@code StreamPublisher.publish}),
 * so the cluster had agreed they belong to their epoch; they simply had not
 * been consumed yet.</p>
 *
 * <p>So today {@code isValid()} is a PURE predicate and {@code updateToken()}
 * is the only mutator; {@code FencingTokenFilter} calls the latter explicitly
 * and never drops anything. The tests below assert that contract, including a
 * regression test pinning the no-side-effect property that BUG-037 depends on.</p>
 */
class FencingTokenValidatorTest {

    private FencingTokenValidator validator;

    @BeforeEach
    void setUp() {
        validator = new FencingTokenValidator();
    }

    @Test
    void isValidDoesNotAdvanceTheToken() {
        // The BUG-037 invariant. If this ever starts failing, the drop-filter
        // behaviour has come back and in-flight events from the previous epoch
        // will be silently discarded at the next switchover.
        assertTrue(validator.isValid(1));
        assertEquals(0, validator.getCurrentToken(), "isValid must not mutate knownMaxToken");

        assertTrue(validator.isValid(100));
        assertEquals(0, validator.getCurrentToken(), "isValid must not mutate knownMaxToken");
    }

    @Test
    void acceptIncreasingTokens() {
        validator.updateToken(1);
        assertTrue(validator.isValid(1));
        assertEquals(1, validator.getCurrentToken());

        validator.updateToken(2);
        assertTrue(validator.isValid(2));
        assertEquals(2, validator.getCurrentToken());

        validator.updateToken(100);
        assertTrue(validator.isValid(100));
        assertEquals(100, validator.getCurrentToken());
    }

    @Test
    void rejectStaleToken() {
        validator.updateToken(10);
        assertTrue(validator.isValid(10));
        assertFalse(validator.isValid(5));
        assertEquals(10, validator.getCurrentToken());
    }

    @Test
    void acceptEqualToken() {
        validator.updateToken(7);
        assertTrue(validator.isValid(7));
        assertTrue(validator.isValid(7));
        assertEquals(7, validator.getCurrentToken());
    }

    @Test
    void initialTokenIsZero() {
        assertEquals(0, validator.getCurrentToken());
        assertTrue(validator.isValid(0));
    }

    @Test
    void updateTokenSetsHigherValue() {
        validator.updateToken(50);
        assertEquals(50, validator.getCurrentToken());

        validator.updateToken(30);
        assertEquals(50, validator.getCurrentToken(), "updateToken should not decrease the token");

        validator.updateToken(100);
        assertEquals(100, validator.getCurrentToken());
    }

    @Test
    void updateTokenAffectsSubsequentValidation() {
        validator.updateToken(50);
        assertFalse(validator.isValid(49));
        assertTrue(validator.isValid(50));
        assertTrue(validator.isValid(51));
    }
}
