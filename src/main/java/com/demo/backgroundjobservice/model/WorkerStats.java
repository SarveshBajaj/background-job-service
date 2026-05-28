package com.demo.backgroundjobservice.model;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable stats for a registered worker. Counters use AtomicLong for thread-safe increments
 * without requiring a lock on the whole object.
 */
public class WorkerStats {

    private final String workerId;
    private final Set<String> handledTypes;

    // guarded by synchronized(this) — must be updated atomically with currentLeaseToken
    private volatile String currentJobId;
    private volatile String currentLeaseToken;

    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);

    public WorkerStats(String workerId, Set<String> handledTypes) {
        if (workerId == null || workerId.isBlank())   throw new IllegalArgumentException("workerId must not be blank");
        if (handledTypes == null || handledTypes.isEmpty()) throw new IllegalArgumentException("handledTypes must not be empty");
        this.workerId     = workerId;
        this.handledTypes = Set.copyOf(handledTypes); // immutable defensive copy
    }

    public String getWorkerId()            { return workerId; }
    public Set<String> getHandledTypes()   { return handledTypes; }
    public String getCurrentJobId()        { return currentJobId; }
    public String getCurrentLeaseToken()   { return currentLeaseToken; }
    public long getSuccessCount()          { return successCount.get(); }
    public long getFailureCount()          { return failureCount.get(); }

    public synchronized void markBusy(String jobId, String leaseToken) {
        this.currentJobId     = jobId;
        this.currentLeaseToken = leaseToken;
    }

    public synchronized void markIdle() {
        this.currentJobId      = null;
        this.currentLeaseToken = null;
    }

    public void incrementSuccess() { successCount.incrementAndGet(); }
    public void incrementFailure() { failureCount.incrementAndGet(); }
}
