package com.demo.backgroundjobservice.strategy;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter.
 *
 * <p>Formula: {@code min(base * 2^attempt, cap) + random(0, base)}
 * <ul>
 *   <li>base = 5s — short enough for fast recovery, long enough to avoid thundering herd</li>
 *   <li>cap = 300s — prevents unbounded delays</li>
 *   <li>jitter = random(0, base) — spreads retries across time to avoid synchronized spikes</li>
 * </ul>
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private static final long BASE_MS = 5_000L;
    private static final long CAP_MS  = 300_000L;

    @Override
    public long computeDelayMs(int attemptCount) {
        // cap the exponential to avoid overflow on large attemptCount
        long exp = Math.min(BASE_MS * (1L << Math.min(attemptCount, 30)), CAP_MS);
        long jitter = ThreadLocalRandom.current().nextLong(BASE_MS);
        return exp + jitter;
    }
}
