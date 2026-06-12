package com.neo4j.ha.common.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FencingTokenValidatorTest {

    private FencingTokenValidator validator;

    @BeforeEach
    void setUp() {
        validator = new FencingTokenValidator();
    }

    @Test
    void acceptIncreasingTokens() {
        assertTrue(validator.isValid(1));
        assertEquals(1, validator.getCurrentToken());

        assertTrue(validator.isValid(2));
        assertEquals(2, validator.getCurrentToken());

        assertTrue(validator.isValid(100));
        assertEquals(100, validator.getCurrentToken());
    }

    @Test
    void rejectStaleToken() {
        assertTrue(validator.isValid(10));
        assertFalse(validator.isValid(5));
        assertEquals(10, validator.getCurrentToken());
    }

    @Test
    void acceptEqualToken() {
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
