package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.exception.WorkerNotFoundException;
import com.demo.backgroundjobservice.model.WorkerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of long-lived named workers. Workers register once and run many jobs.
 * Tracks per-worker stats: current lease, success/failure counts.
 */
public class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

    // workerId → WorkerStats; ConcurrentHashMap for thread-safe O(1) access
    private final ConcurrentHashMap<String, WorkerStats> workers = new ConcurrentHashMap<>();

    /**
     * Registers a worker with the set of job types it can handle.
     *
     * @param workerId     unique worker identifier
     * @param handledTypes non-empty set of job types this worker handles
     * @throws IllegalArgumentException if workerId is blank or handledTypes is empty
     * @throws IllegalArgumentException if a worker with this ID is already registered
     */
    public void register(String workerId, Set<String> handledTypes) {
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        if (handledTypes == null || handledTypes.isEmpty()) throw new IllegalArgumentException("handledTypes must not be empty");
        if (workers.putIfAbsent(workerId, new WorkerStats(workerId, handledTypes)) != null) {
            throw new IllegalArgumentException("Worker already registered: " + workerId);
        }
        log.info("Worker registered: workerId={}, types={}", workerId, handledTypes);
    }

    /**
     * Marks a worker as busy with a specific job and lease token.
     *
     * @param workerId   the worker
     * @param jobId      the job being executed
     * @param leaseToken the active lease token
     * @throws WorkerNotFoundException if the worker is not registered
     */
    public void markBusy(String workerId, String jobId, String leaseToken) {
        get(workerId).markBusy(jobId, leaseToken);
        log.debug("Worker busy: workerId={}, jobId={}", workerId, jobId);
    }

    /**
     * Marks a worker as idle (no active job).
     *
     * @param workerId the worker
     * @throws WorkerNotFoundException if the worker is not registered
     */
    public void markIdle(String workerId) {
        get(workerId).markIdle();
        log.debug("Worker idle: workerId={}", workerId);
    }

    /**
     * Records a successful job completion for the worker.
     *
     * @param workerId the worker
     * @throws WorkerNotFoundException if the worker is not registered
     */
    public void recordSuccess(String workerId) {
        get(workerId).incrementSuccess();
    }

    /**
     * Records a failed job for the worker.
     *
     * @param workerId the worker
     * @throws WorkerNotFoundException if the worker is not registered
     */
    public void recordFailure(String workerId) {
        get(workerId).incrementFailure();
    }

    /**
     * Returns the stats for a specific worker.
     *
     * @param workerId the worker
     * @return WorkerStats (live reference)
     * @throws WorkerNotFoundException if the worker is not registered
     */
    public WorkerStats getStats(String workerId) {
        return get(workerId);
    }

    /**
     * Returns an unmodifiable view of all registered workers' stats.
     *
     * @return map of workerId → WorkerStats
     */
    public Map<String, WorkerStats> allStats() {
        return Collections.unmodifiableMap(workers);
    }

    /**
     * Finds the workerId currently holding the given leaseToken, or null if none.
     * Used by the Reaper to clear worker state when reclaiming an expired lease.
     *
     * @param leaseToken the lease token to search for
     * @return workerId or null
     */
    public String findWorkerByLeaseToken(String leaseToken) {
        return workers.values().stream()
                .filter(s -> leaseToken.equals(s.getCurrentLeaseToken()))
                .map(WorkerStats::getWorkerId)
                .findFirst()
                .orElse(null);
    }

    // --- private ---

    private WorkerStats get(String workerId) {
        WorkerStats stats = workers.get(workerId);
        if (stats == null) throw new WorkerNotFoundException(workerId);
        return stats;
    }
}
