package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.model.AcquiredJob;
import com.demo.backgroundjobservice.model.DLQEntry;
import com.demo.backgroundjobservice.model.ExecutionResult;
import com.demo.backgroundjobservice.model.JobSpec;
import com.demo.backgroundjobservice.model.WorkerStats;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Primary interface for the Background Job Service.
 * Designed as an interface so a network transport layer can wrap it without changing any handler or worker code.
 */
public interface JobService {

    /**
     * Submits a new job.
     *
     * @param spec job specification
     * @return unique jobId
     */
    String submit(JobSpec spec);

    /**
     * Registers a worker that can handle the given job types.
     *
     * @param workerId     unique worker identifier
     * @param handledTypes non-empty set of job types
     */
    void registerWorker(String workerId, Set<String> handledTypes);

    /**
     * Acquires the next available job for the given worker.
     * Returns null if no eligible job is currently available (non-blocking).
     *
     * @param workerId     the requesting worker
     * @param handledTypes types this worker can handle
     * @return AcquiredJob with leaseToken, or null if nothing available
     */
    AcquiredJob acquireJob(String workerId, Set<String> handledTypes);

    /**
     * Completes a job with the given result.
     *
     * @param workerId   the worker completing the job
     * @param jobId      the job to complete
     * @param leaseToken must match the current lease
     * @param result     execution outcome
     */
    void completeJob(String workerId, String jobId, String leaseToken, ExecutionResult result);

    /**
     * Renews the lease on a claimed job, extending its visibility timeout.
     *
     * @param jobId      the job
     * @param leaseToken must match the current lease
     */
    void renewLease(String jobId, String leaseToken);

    // --- Observability ---

    /** @return pending job count grouped by type */
    Map<String, Long> pendingCountByType();

    /** @return pending job count grouped by priority */
    Map<Integer, Long> pendingCountByPriority();

    /** @return number of currently claimed (in-flight) jobs */
    long inFlightCount();

    /** @return number of entries in the DLQ */
    long dlqSize();

    /** @return immutable snapshot of all DLQ entries */
    List<DLQEntry> listDLQ();

    /** @return stats for a specific worker */
    WorkerStats getWorkerStats(String workerId);

    /** @return stats for all registered workers */
    Map<String, WorkerStats> allWorkerStats();
}
