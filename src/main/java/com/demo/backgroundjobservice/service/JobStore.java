package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.model.DLQEntry;
import com.demo.backgroundjobservice.model.Job;
import com.demo.backgroundjobservice.model.JobStatus;

import java.util.List;

/**
 * Persistence boundary for jobs and the DLQ.
 * Swap this implementation to move from in-memory to a database without touching service logic.
 */
public interface JobStore {

    /**
     * Persists a new job. The job must be in PENDING status.
     *
     * @param job the job to save
     * @throws IllegalArgumentException if a job with the same ID already exists
     */
    void save(Job job);

    /**
     * Retrieves a job by ID.
     *
     * @param jobId the job identifier
     * @return the Job
     * @throws com.demo.backgroundjobservice.exception.JobNotFoundException if not found
     */
    Job get(String jobId);

    /**
     * Returns all jobs currently in the given status.
     *
     * @param status the status to filter by
     * @return snapshot list (not live)
     */
    List<Job> getByStatus(JobStatus status);

    /**
     * Moves a job to the DLQ with the given reason.
     *
     * @param job    the failed job
     * @param reason DLQEntry.REASON_MAX_RETRIES or DLQEntry.REASON_PERMANENT
     */
    void addToDLQ(Job job, String reason);

    /** @return current DLQ size */
    long dlqSize();

    /** @return immutable snapshot of all DLQ entries */
    List<DLQEntry> listDLQ();
}
