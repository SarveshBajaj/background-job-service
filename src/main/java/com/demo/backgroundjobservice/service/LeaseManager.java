package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.exception.StaleLeaseException;
import com.demo.backgroundjobservice.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Issues, validates, renews, and clears leases on jobs.
 * All mutations are performed inside {@code synchronized(job)} by the caller —
 * this class only contains the logic, not the lock.
 *
 * <p>Lease token is a random UUID; opaque to the worker.
 */
public class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);

    /**
     * Issues a new lease on the given job. Must be called inside {@code synchronized(job)}.
     *
     * @param job the job to lease
     * @return the new leaseToken
     */
    public String issueLease(Job job) {
        String token = UUID.randomUUID().toString();
        job.setCurrentLeaseToken(token);
        job.setLeaseExpiresAt(System.currentTimeMillis() + job.getSpec().leaseDurationMs());
        log.debug("Lease issued: jobId={}, token={}, expiresAt={}", job.getJobId(), token, job.getLeaseExpiresAt());
        return token;
    }

    /**
     * Validates that the presented leaseToken matches the current lease on the job.
     * Must be called inside {@code synchronized(job)}.
     *
     * @param job        the job
     * @param leaseToken the token presented by the worker
     * @throws StaleLeaseException if the token does not match (lease expired and re-issued, or already completed)
     */
    public void validate(Job job, String leaseToken) {
        if (!leaseToken.equals(job.getCurrentLeaseToken())) {
            log.warn("Stale lease rejected: jobId={}, presented={}, current={}", job.getJobId(), leaseToken, job.getCurrentLeaseToken());
            throw new StaleLeaseException(job.getJobId());
        }
    }

    /**
     * Extends the lease expiry by the job's configured lease duration.
     * Must be called inside {@code synchronized(job)} after {@link #validate}.
     *
     * @param job the job whose lease to extend
     */
    public void extend(Job job) {
        long newExpiry = System.currentTimeMillis() + job.getSpec().leaseDurationMs();
        job.setLeaseExpiresAt(newExpiry);
        log.debug("Lease extended: jobId={}, newExpiresAt={}", job.getJobId(), newExpiry);
    }

    /**
     * Clears all lease fields on the job (called on completion or reaper reclaim).
     * Must be called inside {@code synchronized(job)}.
     *
     * @param job the job to clear
     */
    public void clearLease(Job job) {
        log.debug("Lease cleared: jobId={}", job.getJobId());
        job.clearLease();
    }
}
