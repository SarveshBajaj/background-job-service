package com.demo.backgroundjobservice.model;

/**
 * Result returned by a {@link com.demo.backgroundjobservice.strategy.JobHandler} after execution.
 *
 * <ul>
 *   <li>{@link #SUCCESS} — job completed; transition to SUCCEEDED.</li>
 *   <li>{@link #TRANSIENT_FAILURE} — temporary error; retry with backoff if retries remain.</li>
 *   <li>{@link #PERMANENT_FAILURE} — unrecoverable error; skip retries, move straight to DLQ.</li>
 * </ul>
 */
public enum ExecutionResult {
    SUCCESS,
    TRANSIENT_FAILURE,
    PERMANENT_FAILURE
}
