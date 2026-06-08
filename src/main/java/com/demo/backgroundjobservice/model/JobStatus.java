package com.demo.backgroundjobservice.model;

import java.util.Map;
import java.util.Set;

/**
 * Lifecycle states of a Job, with valid transition enforcement.
 * Transitions are validated here so no service can make an illegal move silently.
 */
public enum JobStatus {
    PENDING,
    CLAIMED,
    SUCCEEDED,
    RETRY_SCHEDULED,
    CANCELLED,
    FAILED;

    private static final Map<JobStatus, Set<JobStatus>> VALID_TRANSITIONS = Map.of(
            PENDING,          Set.of(CLAIMED, CANCELLED),
            CLAIMED,          Set.of(SUCCEEDED, FAILED, RETRY_SCHEDULED, PENDING), // PENDING = lease expiry path
            RETRY_SCHEDULED,  Set.of(PENDING, CANCELLED),
            SUCCEEDED,        Set.of(),
            FAILED,           Set.of()
    );

    /**
     * Validates that transitioning from this status to {@code next} is legal.
     *
     * @param next the target status
     * @throws IllegalStateException if the transition is not permitted
     */
    public void validateTransition(JobStatus next) {
        if (!VALID_TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateException(
                    "Invalid job status transition: " + this + " → " + next);
        }
    }
}
