package com.demo.backgroundjobservice.model;

import java.time.Instant;

/**
 * Mutable entity representing a job throughout its lifecycle.
 * All state-mutating methods are package-private or accessed via synchronized blocks in services.
 * Business logic lives in services — this class holds data only.
 */
public class Job {

    private final String jobId;
    private final JobSpec spec;
    private final Instant createdAt;

    // guarded by synchronized(this) in service layer
    private JobStatus status;
    private int attemptCount;
    private String currentLeaseToken;   // null when not claimed
    private long leaseExpiresAt;        // epoch ms; 0 when not claimed
    private long scheduledRunAt;        // epoch ms; set when RETRY_SCHEDULED
    private Instant updatedAt;

    public Job(String jobId, JobSpec spec) {
        if (jobId == null || jobId.isBlank()) throw new IllegalArgumentException("jobId must not be blank");
        if (spec == null)                     throw new IllegalArgumentException("spec must not be null");
        this.jobId        = jobId;
        this.spec         = spec;
        this.status       = JobStatus.PENDING;
        this.attemptCount = 0;
        this.createdAt    = Instant.now();
        this.updatedAt    = this.createdAt;
    }

    // --- Getters ---

    public String getJobId()              { return jobId; }
    public JobSpec getSpec()              { return spec; }
    public JobStatus getStatus()          { return status; }
    public int getAttemptCount()          { return attemptCount; }
    public String getCurrentLeaseToken()  { return currentLeaseToken; }
    public long getLeaseExpiresAt()       { return leaseExpiresAt; }
    public long getScheduledRunAt()       { return scheduledRunAt; }
    public Instant getCreatedAt()         { return createdAt; }
    public Instant getUpdatedAt()         { return updatedAt; }

    // --- Mutators (called only from service layer inside synchronized blocks) ---

    public void setStatus(JobStatus status) {
        this.status    = status;
        this.updatedAt = Instant.now();
    }

    public void incrementAttemptCount()           { this.attemptCount++; }
    public void setCurrentLeaseToken(String token) { this.currentLeaseToken = token; }
    public void setLeaseExpiresAt(long epochMs)    { this.leaseExpiresAt = epochMs; }
    public void setScheduledRunAt(long epochMs)    { this.scheduledRunAt = epochMs; }

    /** Clears all lease fields — called on lease expiry or job completion. */
    public void clearLease() {
        this.currentLeaseToken = null;
        this.leaseExpiresAt    = 0;
    }

    @Override
    public String toString() {
        return "Job{id=" + jobId + ", type=" + spec.type() + ", status=" + status
                + ", attempt=" + attemptCount + "}";
    }
}
