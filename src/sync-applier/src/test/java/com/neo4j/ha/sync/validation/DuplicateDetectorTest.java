package com.neo4j.ha.sync.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DuplicateDetectorTest {

    private DuplicateDetector detector;

    @BeforeEach
    void setUp() {
        detector = new DuplicateDetector(3);
    }

    @Test
    void isDuplicate_firstOccurrence_returnsFalse() {
        assertFalse(detector.isDuplicate("event-1"),
                "First occurrence of an event ID should not be a duplicate");
    }

    @Test
    void isDuplicate_afterMark_returnsTrue() {
        detector.mark("event-1");
        assertTrue(detector.isDuplicate("event-1"),
                "Second occurrence after marking should be a duplicate");
    }

    @Test
    void isDuplicate_differentIds_returnsFalse() {
        detector.mark("event-1");
        assertFalse(detector.isDuplicate("event-2"),
                "A different event ID should not be a duplicate");
    }

    @Test
    void lruEviction_oldestIdEvicted_canBeReaccepted() {
        // maxSize = 3, fill up the detector
        detector.mark("event-1");
        detector.mark("event-2");
        detector.mark("event-3");

        // All three should be recognized as duplicates
        assertTrue(detector.isDuplicate("event-1"));
        assertTrue(detector.isDuplicate("event-2"));
        assertTrue(detector.isDuplicate("event-3"));

        // Adding a 4th element should evict the oldest (event-1)
        detector.mark("event-4");

        assertFalse(detector.isDuplicate("event-1"),
                "Evicted event ID should no longer be detected as duplicate");
        assertTrue(detector.isDuplicate("event-2"),
                "Non-evicted event ID should still be detected as duplicate");
        assertTrue(detector.isDuplicate("event-4"),
                "Newly added event ID should be detected as duplicate");
    }

    @Test
    void lruEviction_multipleEvictions_workCorrectly() {
        detector.mark("a");
        detector.mark("b");
        detector.mark("c");

        // Evict a
        detector.mark("d");
        assertFalse(detector.isDuplicate("a"));

        // Evict b
        detector.mark("e");
        assertFalse(detector.isDuplicate("b"));

        // c, d, e remain
        assertTrue(detector.isDuplicate("c"));
        assertTrue(detector.isDuplicate("d"));
        assertTrue(detector.isDuplicate("e"));
    }

    @Test
    void mark_sameIdTwice_doesNotAffectCapacity() {
        detector.mark("event-1");
        detector.mark("event-1"); // duplicate mark
        detector.mark("event-2");
        detector.mark("event-3");

        // event-1 should still be present since re-adding the same element
        // to a LinkedHashSet does not increase its size
        assertTrue(detector.isDuplicate("event-1"));
        assertTrue(detector.isDuplicate("event-2"));
        assertTrue(detector.isDuplicate("event-3"));
    }

    @Test
    void isDuplicate_beforeAnyMarks_returnsFalse() {
        DuplicateDetector fresh = new DuplicateDetector(10);
        assertFalse(fresh.isDuplicate("anything"));
    }
}
