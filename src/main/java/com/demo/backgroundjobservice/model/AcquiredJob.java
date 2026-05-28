package com.demo.backgroundjobservice.model;

/**
 * Returned to a worker after successfully acquiring a lease on a job.
 *
 * @param jobId       unique job identifier; use as idempotency key in the handler
 * @param type        job type string
 * @param payload     opaque data to pass to the handler
 * @param leaseToken  opaque token the worker must present on complete/renew
 */
public record AcquiredJob(String jobId, String type, Object payload, String leaseToken) {

    public AcquiredJob {
        if (jobId == null || jobId.isBlank())       throw new IllegalArgumentException("jobId must not be blank");
        if (type == null || type.isBlank())          throw new IllegalArgumentException("type must not be blank");
        if (payload == null)                         throw new IllegalArgumentException("payload must not be null");
        if (leaseToken == null || leaseToken.isBlank()) throw new IllegalArgumentException("leaseToken must not be blank");
    }
}
