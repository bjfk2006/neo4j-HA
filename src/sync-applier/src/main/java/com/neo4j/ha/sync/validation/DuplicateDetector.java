package com.neo4j.ha.sync.validation;

import java.util.LinkedHashSet;

public class DuplicateDetector {

    private final LinkedHashSet<String> seenIds;
    private final int maxSize;

    public DuplicateDetector(int maxSize) {
        this.maxSize = maxSize;
        this.seenIds = new LinkedHashSet<>();
    }

    public synchronized boolean isDuplicate(String eventId) {
        return seenIds.contains(eventId);
    }

    public synchronized void mark(String eventId) {
        if (seenIds.size() >= maxSize) {
            var iterator = seenIds.iterator();
            iterator.next();
            iterator.remove();
        }
        seenIds.add(eventId);
    }
}
