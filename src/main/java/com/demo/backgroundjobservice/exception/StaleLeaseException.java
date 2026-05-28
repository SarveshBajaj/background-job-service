package com.demo.backgroundjobservice.exception;

/**
 * Thrown when a worker presents a leaseToken that does not match the current lease on a job.
 * This happens when a worker's lease expired and the job was re-leased to another worker.
 */
public class StaleLeaseException extends RuntimeException {
    public StaleLeaseException(String jobId) {
        super("Stale lease for job: " + jobId + " — lease may have expired or been re-issued");
    }
}
