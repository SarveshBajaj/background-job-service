package com.demo.backgroundjobservice.model;

import java.time.Instant;

/**
 * Immutable record of a job that has been moved to the Dead Letter Queue.
 *
 * @param job       the failed job (snapshot at time of DLQ entry)
 * @param reason    human-readable reason: "MAX_RETRIES_EXHAUSTED" or "PERMANENT_FAILURE"
 * @param failedAt  timestamp when the job was moved to DLQ
 */
public record DLQEntry(Job job, String reason, Instant failedAt) {

    public static final String REASON_MAX_RETRIES   = "MAX_RETRIES_EXHAUSTED";
    public static final String REASON_PERMANENT     = "PERMANENT_FAILURE";

    public DLQEntry {
        if (job == null)                       throw new IllegalArgumentException("job must not be null");
        if (reason == null || reason.isBlank()) throw new IllegalArgumentException("reason must not be blank");
        if (failedAt == null)                  throw new IllegalArgumentException("failedAt must not be null");
    }
}
