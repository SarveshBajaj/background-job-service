package com.demo.backgroundjobservice.model;

/**
 * Immutable specification submitted by a producer to create a job.
 *
 * @param type           job type string (e.g. "send_email"); used for worker routing and exclusivity
 * @param payload        opaque data passed to the handler
 * @param priority       dispatch priority 0-9 (higher = more weight); default 5
 * @param maxRetries     max transient-failure retries before DLQ; default 3
 * @param leaseDurationMs visibility timeout in ms; default 10_000
 */
public record JobSpec(
        String type,
        Object payload,
        int priority,
        int maxRetries,
        long leaseDurationMs
) {
    private static final int DEFAULT_PRIORITY = 5;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_LEASE_MS = 10_000L;

    /** Compact canonical constructor — validates all fields. */
    public JobSpec {
        if (type == null || type.isBlank()) throw new IllegalArgumentException("type must not be blank");
        if (payload == null)               throw new IllegalArgumentException("payload must not be null");
        if (priority < 0 || priority > 9)  throw new IllegalArgumentException("priority must be 0-9");
        if (maxRetries < 0)                throw new IllegalArgumentException("maxRetries must be >= 0");
        if (leaseDurationMs <= 0)          throw new IllegalArgumentException("leaseDurationMs must be > 0");
    }

    /** Convenience factory using all defaults. */
    public static JobSpec of(String type, Object payload) {
        return new JobSpec(type, payload, DEFAULT_PRIORITY, DEFAULT_MAX_RETRIES, DEFAULT_LEASE_MS);
    }

    /** Convenience factory with priority override. */
    public static JobSpec of(String type, Object payload, int priority) {
        return new JobSpec(type, payload, priority, DEFAULT_MAX_RETRIES, DEFAULT_LEASE_MS);
    }
}
