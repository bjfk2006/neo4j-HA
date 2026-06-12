package com.neo4j.ha.agent.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * IP-level failed-login rate limiter with sliding-window failure count and
 * hard lockout. When failures exceed {@code maxFailures} within
 * {@code WINDOW_MS}, the IP is locked for {@code lockMs}. Locked IPs cannot
 * even attempt to log in until the lock expires.
 *
 * <p>All state in-memory; resets on Agent restart (acceptable — this protects
 * against bursty online brute force, not against patient offline attackers).
 */
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    private static final long WINDOW_MS = 60_000L;
    private static final int MAX_TRACKED_IPS = 10_000;

    private final int maxFailures;
    private final long lockMs;

    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lockedUntil = new ConcurrentHashMap<>();

    public RateLimiter(int maxFailures, long lockMs) {
        this.maxFailures = Math.max(1, maxFailures);
        this.lockMs = Math.max(1_000L, lockMs);
    }

    /** Returns 0 if not locked, or remaining lock millis if currently locked. */
    public long checkLockedRemainingMs(String ip) {
        if (ip == null) return 0L;
        Long until = lockedUntil.get(ip);
        if (until == null) return 0L;
        long remaining = until - System.currentTimeMillis();
        if (remaining <= 0) {
            lockedUntil.remove(ip);
            return 0L;
        }
        return remaining;
    }

    public void recordFailure(String ip) {
        if (ip == null) return;
        if (windows.size() > MAX_TRACKED_IPS) {
            // Defensive: don't unbounded-grow under flood. New IPs are silently
            // not tracked; existing IPs still feed their counters. Better than OOM.
            log.warn("RateLimiter map at cap ({}); dropping further new IPs", MAX_TRACKED_IPS);
            return;
        }
        long now = System.currentTimeMillis();
        Window w = windows.computeIfAbsent(ip, k -> new Window(now));
        int count = w.recordIfWithinWindow(now);
        if (count >= maxFailures) {
            lockedUntil.put(ip, now + lockMs);
            windows.remove(ip);
            log.warn("Login rate-limit lock: ip={} failures={} lockedFor={}ms",
                ip, count, lockMs);
        }
    }

    public void recordSuccess(String ip) {
        if (ip == null) return;
        windows.remove(ip);
    }

    /** Visible for tests. */
    int trackedIpCount() { return windows.size() + lockedUntil.size(); }

    private static final class Window {
        private long windowStart;
        private final AtomicInteger count = new AtomicInteger(0);

        Window(long now) { this.windowStart = now; }

        synchronized int recordIfWithinWindow(long now) {
            if (now - windowStart > WINDOW_MS) {
                windowStart = now;
                count.set(0);
            }
            return count.incrementAndGet();
        }
    }
}
