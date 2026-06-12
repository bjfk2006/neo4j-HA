package com.neo4j.ha.cdc.transform;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DiffCalculatorTest {

    private DiffCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new DiffCalculator(3);
    }

    @Test
    void firstAccessReturnsNull() {
        Map<String, Object> result = calculator.computeDiff("node-1", Map.of("name", "Alice"));
        assertNull(result, "First access for an element should return null (no previous state)");
    }

    @Test
    void secondAccessReturnsPreviousProps() {
        Map<String, Object> first = Map.of("name", "Alice");
        Map<String, Object> second = Map.of("name", "Bob");

        calculator.computeDiff("node-1", first);
        Map<String, Object> previous = calculator.computeDiff("node-1", second);

        assertNotNull(previous);
        assertEquals("Alice", previous.get("name"));
    }

    @Test
    void evictRemovesEntry() {
        calculator.computeDiff("node-1", Map.of("name", "Alice"));
        assertEquals(1, calculator.size());

        calculator.evict("node-1");
        assertEquals(0, calculator.size());

        // After eviction, next access should return null again
        Map<String, Object> result = calculator.computeDiff("node-1", Map.of("name", "Bob"));
        assertNull(result);
    }

    @Test
    void lruEvictionWhenOverMaxSize() {
        // Max size is 3
        calculator.computeDiff("node-1", Map.of("k", "v1"));
        calculator.computeDiff("node-2", Map.of("k", "v2"));
        calculator.computeDiff("node-3", Map.of("k", "v3"));
        assertEquals(3, calculator.size());

        // Adding a 4th element should evict the least-recently-used (node-1)
        calculator.computeDiff("node-4", Map.of("k", "v4"));
        assertEquals(3, calculator.size());

        // node-1 was evicted, so accessing it again should return null
        Map<String, Object> result = calculator.computeDiff("node-1", Map.of("k", "v1-new"));
        assertNull(result, "node-1 should have been evicted by LRU policy");
    }

    @Test
    void lruEvictionRespectsAccessOrder() {
        // Cache size = 3. Insert node-1, node-2, node-3 (all slots filled)
        calculator.computeDiff("node-1", Map.of("k", "v1"));
        calculator.computeDiff("node-2", Map.of("k", "v2"));
        calculator.computeDiff("node-3", Map.of("k", "v3"));

        // Access node-1 to make it recently used (order now: node-2, node-3, node-1)
        calculator.computeDiff("node-1", Map.of("k", "v1-updated"));

        // Adding node-4 should evict node-2 (the least recently used)
        calculator.computeDiff("node-4", Map.of("k", "v4"));
        assertEquals(3, calculator.size());

        // Verify node-2 was evicted (returns null = no previous state)
        Map<String, Object> result2 = calculator.computeDiff("node-2", Map.of("k", "v2-new"));
        assertNull(result2, "node-2 should have been evicted");

        // node-1 was recently accessed so should still be in cache
        // But adding node-2 back evicted node-3 (now LRU). Verify node-1 survives.
        Map<String, Object> result1 = calculator.computeDiff("node-1", Map.of("k", "v1-again"));
        assertNotNull(result1, "node-1 should still be in cache (recently accessed)");
    }

    @Test
    void threadSafetyConcurrentAccessDoesNotThrow() throws InterruptedException {
        DiffCalculator concurrentCalc = new DiffCalculator(100);
        int threadCount = 10;
        int opsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String id = "elem-" + threadId + "-" + i;
                        concurrentCalc.computeDiff(id, Map.of("val", i));
                        if (i % 3 == 0) {
                            concurrentCalc.evict(id);
                        }
                        concurrentCalc.size();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All threads should complete without deadlock");
        executor.shutdown();
    }
}
