package com.neo4j.ha.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class RetryUtil {

    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    public static <T> T retry(int maxRetries, long delayMs,
                               Class<? extends Exception> retryOn,
                               Supplier<T> action) {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return action.get();
            } catch (Exception e) {
                if (!retryOn.isInstance(e)) {
                    throw e;
                }
                lastException = e;
                if (attempt < maxRetries) {
                    log.warn("Attempt {}/{} failed, retrying in {}ms: {}",
                        attempt + 1, maxRetries + 1, delayMs, e.getMessage());
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }
        throw new RuntimeException("All " + (maxRetries + 1) + " attempts failed", lastException);
    }

    public static void retryVoid(int maxRetries, long delayMs,
                                  Class<? extends Exception> retryOn,
                                  Runnable action) {
        retry(maxRetries, delayMs, retryOn, () -> {
            action.run();
            return null;
        });
    }
}
