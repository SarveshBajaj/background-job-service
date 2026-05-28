package com.demo.backgroundjobservice.strategy;

/**
 * Strategy for computing retry delay after a transient failure.
 * Implement this interface to provide custom backoff behavior per job type.
 */
public interface RetryPolicy {

    /**
     * Computes the delay before the next retry attempt.
     *
     * @param attemptCount number of attempts already made (0-based: 0 = first failure)
     * @return delay in milliseconds
     */
    long computeDelayMs(int attemptCount);
}
