package com.neo4j.ha.cdc.transform;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class DiffCalculator {

    private final Map<String, Map<String, Object>> cache;

    public DiffCalculator(int maxSize) {
        this.cache = Collections.synchronizedMap(
            new LinkedHashMap<>(maxSize, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Map<String, Object>> eldest) {
                    return size() > maxSize;
                }
            });
    }

    public Map<String, Object> computeDiff(String elementId, Map<String, Object> currentProps) {
        synchronized (cache) {
            Map<String, Object> previousProps = cache.get(elementId);
            cache.put(elementId, Map.copyOf(currentProps));
            return previousProps;
        }
    }

    public void evict(String elementId) {
        cache.remove(elementId);
    }

    public int size() {
        return cache.size();
    }
}
