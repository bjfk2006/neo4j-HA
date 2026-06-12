package com.neo4j.ha.common.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryUtilTest {

    @Test
    void successOnFirstTry() {
        String result = RetryUtil.retry(3, 1, RuntimeException.class, () -> "ok");
        assertEquals("ok", result);
    }

    @Test
    void successAfterRetries() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = RetryUtil.retry(3, 1, IllegalStateException.class, () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("not yet");
            }
            return "done";
        });

        assertEquals("done", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void allRetriesExhaustedThrowsException() {
        AtomicInteger attempts = new AtomicInteger(0);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                RetryUtil.retry(2, 1, IllegalStateException.class, () -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("always fails");
                })
        );

        assertTrue(ex.getMessage().contains("All 3 attempts failed"));
        assertEquals(3, attempts.get()); // attempt 0, 1, 2
    }

    @Test
    void nonRetryableExceptionPropagatesImmediately() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(IllegalArgumentException.class, () ->
                RetryUtil.retry(3, 1, IllegalStateException.class, () -> {
                    attempts.incrementAndGet();
                    throw new IllegalArgumentException("wrong type");
                })
        );

        assertEquals(1, attempts.get());
    }

    @Test
    void retryVoidSucceedsAfterFailures() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertDoesNotThrow(() ->
                RetryUtil.retryVoid(2, 1, RuntimeException.class, () -> {
                    if (attempts.incrementAndGet() < 2) {
                        throw new RuntimeException("not yet");
                    }
                })
        );

        assertEquals(2, attempts.get());
    }

    @Test
    void retryVoidExhaustedThrows() {
        assertThrows(RuntimeException.class, () ->
                RetryUtil.retryVoid(1, 1, RuntimeException.class, () -> {
                    throw new RuntimeException("always");
                })
        );
    }
}
