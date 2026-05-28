package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.model.*;
import com.demo.backgroundjobservice.strategy.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Core orchestrator for the Background Job Service.
 * All dependencies are constructor-injected for testability.
 *
 * <p>Thread safety: job state transitions are performed inside {@code synchronized(job)}.
 * ExclusivityManager uses lock-free CAS. WorkerRegistry uses ConcurrentHashMap.
 */
public class JobServiceImpl implements JobService, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobServiceImpl.class);

    private final JobStore            jobStore;
    private final JobQueue            jobQueue;
    private final LeaseManager        leaseManager;
    private final ExclusivityManager  exclusivityManager;
    private final WorkerRegistry      workerRegistry;
    private final RetryPolicy         retryPolicy;
    private final ScheduledExecutorService scheduler;

    // Reaper interval — how often expired leases are scanned
    private static final long REAPER_INTERVAL_MS = 2_000L;

    public JobServiceImpl(JobStore jobStore,
                          JobQueue jobQueue,
                          LeaseManager leaseManager,
                          ExclusivityManager exclusivityManager,
                          WorkerRegistry workerRegistry,
                          RetryPolicy retryPolicy,
                          ScheduledExecutorService scheduler) {
        this.jobStore           = jobStore;
        this.jobQueue           = jobQueue;
        this.leaseManager       = leaseManager;
        this.exclusivityManager = exclusivityManager;
        this.workerRegistry     = workerRegistry;
        this.retryPolicy        = retryPolicy;
        this.scheduler          = scheduler;

        startReaper();
    }

    /** Convenience constructor with default scheduler. */
    public JobServiceImpl(JobStore jobStore,
                          JobQueue jobQueue,
                          LeaseManager leaseManager,
                          ExclusivityManager exclusivityManager,
                          WorkerRegistry workerRegistry,
                          RetryPolicy retryPolicy) {
        this(jobStore, jobQueue, leaseManager, exclusivityManager, workerRegistry, retryPolicy,
                Executors.newScheduledThreadPool(2));
    }

    // -------------------------------------------------------------------------
    // Submission
    // -------------------------------------------------------------------------

    @Override
    public String submit(JobSpec spec) {
        if (spec == null) throw new IllegalArgumentException("spec must not be null");
        String jobId = UUID.randomUUID().toString();
        Job job = new Job(jobId, spec);
        jobStore.save(job);
        jobQueue.enqueue(job);
        log.info("Job submitted: jobId={}, type={}, priority={}", jobId, spec.type(), spec.priority());
        return jobId;
    }

    // -------------------------------------------------------------------------
    // Worker registration
    // -------------------------------------------------------------------------

    @Override
    public void registerWorker(String workerId, Set<String> handledTypes) {
        workerRegistry.register(workerId, handledTypes);
    }

    // -------------------------------------------------------------------------
    // Acquire
    // -------------------------------------------------------------------------

    @Override
    public AcquiredJob acquireJob(String workerId, Set<String> handledTypes) {
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        if (handledTypes == null || handledTypes.isEmpty()) throw new IllegalArgumentException("handledTypes must not be empty");

        // Build set of types currently locked by exclusivity (another job of that type is running)
        Set<String> excludedTypes = handledTypes.stream()
                .filter(exclusivityManager::isLocked)
                .collect(Collectors.toSet());

        Job job = jobQueue.poll(handledTypes, excludedTypes);
        if (job == null) return null;

        synchronized (job) {
            // Guard: job may have been reclaimed by reaper between poll and lock
            if (job.getStatus() != JobStatus.PENDING) {
                log.warn("Job no longer PENDING after poll — skipping: jobId={}, status={}", job.getJobId(), job.getStatus());
                return null;
            }

            // Acquire exclusivity lock for this type
            if (!exclusivityManager.tryAcquire(job.getSpec().type())) {
                // Another thread beat us — re-enqueue and return null
                jobQueue.enqueue(job);
                log.debug("Exclusivity race lost — re-enqueued: jobId={}", job.getJobId());
                return null;
            }

            job.getSpec().type(); // no-op, just for clarity
            job.incrementAttemptCount();
            String token = leaseManager.issueLease(job);
            job.getStatus().validateTransition(JobStatus.CLAIMED);
            job.setStatus(JobStatus.CLAIMED);

            workerRegistry.markBusy(workerId, job.getJobId(), token);
            log.info("Job acquired: jobId={}, workerId={}, attempt={}", job.getJobId(), workerId, job.getAttemptCount());
            return new AcquiredJob(job.getJobId(), job.getSpec().type(), job.getSpec().payload(), token);
        }
    }

    // -------------------------------------------------------------------------
    // Complete
    // -------------------------------------------------------------------------

    @Override
    public void completeJob(String workerId, String jobId, String leaseToken, ExecutionResult result) {
        if (jobId == null || jobId.isBlank())         throw new IllegalArgumentException("jobId must not be blank");
        if (leaseToken == null || leaseToken.isBlank()) throw new IllegalArgumentException("leaseToken must not be blank");
        if (result == null)                            throw new IllegalArgumentException("result must not be null");

        Job job = jobStore.get(jobId);

        synchronized (job) {
            leaseManager.validate(job, leaseToken); // throws StaleLeaseException if stale
            leaseManager.clearLease(job);
            exclusivityManager.release(job.getSpec().type());
            workerRegistry.markIdle(workerId);

            switch (result) {
                case SUCCESS -> {
                    job.getStatus().validateTransition(JobStatus.SUCCEEDED);
                    job.setStatus(JobStatus.SUCCEEDED);
                    workerRegistry.recordSuccess(workerId);
                    log.info("Job succeeded: jobId={}, workerId={}, attempts={}", jobId, workerId, job.getAttemptCount());
                }
                case PERMANENT_FAILURE -> {
                    job.getStatus().validateTransition(JobStatus.FAILED);
                    job.setStatus(JobStatus.FAILED);
                    jobStore.addToDLQ(job, DLQEntry.REASON_PERMANENT);
                    workerRegistry.recordFailure(workerId);
                    log.warn("Job permanently failed: jobId={}, workerId={}", jobId, workerId);
                }
                case TRANSIENT_FAILURE -> handleTransientFailure(workerId, job);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Lease renewal
    // -------------------------------------------------------------------------

    @Override
    public void renewLease(String jobId, String leaseToken) {
        if (jobId == null || jobId.isBlank())           throw new IllegalArgumentException("jobId must not be blank");
        if (leaseToken == null || leaseToken.isBlank()) throw new IllegalArgumentException("leaseToken must not be blank");

        Job job = jobStore.get(jobId);
        synchronized (job) {
            leaseManager.validate(job, leaseToken);
            leaseManager.extend(job);
            log.debug("Lease renewed: jobId={}", jobId);
        }
    }

    // -------------------------------------------------------------------------
    // Observability
    // -------------------------------------------------------------------------

    @Override
    public Map<String, Long> pendingCountByType() {
        return jobStore.getByStatus(JobStatus.PENDING).stream()
                .collect(Collectors.groupingBy(j -> j.getSpec().type(), Collectors.counting()));
    }

    @Override
    public Map<Integer, Long> pendingCountByPriority() {
        return jobStore.getByStatus(JobStatus.PENDING).stream()
                .collect(Collectors.groupingBy(j -> j.getSpec().priority(), Collectors.counting()));
    }

    @Override
    public long inFlightCount() {
        return jobStore.getByStatus(JobStatus.CLAIMED).size();
    }

    @Override
    public long dlqSize() { return jobStore.dlqSize(); }

    @Override
    public List<DLQEntry> listDLQ() { return jobStore.listDLQ(); }

    @Override
    public WorkerStats getWorkerStats(String workerId) { return workerRegistry.getStats(workerId); }

    @Override
    public Map<String, WorkerStats> allWorkerStats() { return workerRegistry.allStats(); }

    // -------------------------------------------------------------------------
    // AutoCloseable
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) scheduler.shutdownNow();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void handleTransientFailure(String workerId, Job job) {
        workerRegistry.recordFailure(workerId);
        if (job.getAttemptCount() >= job.getSpec().maxRetries() + 1) {
            // +1 because attemptCount was incremented on acquire (includes the current attempt)
            job.getStatus().validateTransition(JobStatus.FAILED);
            job.setStatus(JobStatus.FAILED);
            jobStore.addToDLQ(job, DLQEntry.REASON_MAX_RETRIES);
            log.warn("Job exhausted retries → DLQ: jobId={}, attempts={}", job.getJobId(), job.getAttemptCount());
        } else {
            long delayMs = retryPolicy.computeDelayMs(job.getAttemptCount() - 1);
            job.getStatus().validateTransition(JobStatus.RETRY_SCHEDULED);
            job.setStatus(JobStatus.RETRY_SCHEDULED);
            job.setScheduledRunAt(System.currentTimeMillis() + delayMs);
            scheduleRetry(job, delayMs);
            log.info("Job scheduled for retry: jobId={}, attempt={}, delayMs={}", job.getJobId(), job.getAttemptCount(), delayMs);
        }
    }

    private void scheduleRetry(Job job, long delayMs) {
        scheduler.schedule(() -> {
            synchronized (job) {
                if (job.getStatus() != JobStatus.RETRY_SCHEDULED) return; // guard against concurrent state change
                job.getStatus().validateTransition(JobStatus.PENDING);
                job.setStatus(JobStatus.PENDING);
                jobQueue.enqueue(job);
                log.debug("Job re-enqueued after retry delay: jobId={}", job.getJobId());
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void startReaper() {
        scheduler.scheduleAtFixedRate(this::reapExpiredLeases, REAPER_INTERVAL_MS, REAPER_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("Reaper started with interval={}ms", REAPER_INTERVAL_MS);
    }

    /**
     * Scans all CLAIMED jobs. If a job's lease has expired, returns it to PENDING.
     * Releases exclusivity lock and clears worker state for the expired lease.
     */
    public void reapExpiredLeases() {
        long now = System.currentTimeMillis();
        for (Job job : jobStore.getByStatus(JobStatus.CLAIMED)) {
            synchronized (job) {
                // Re-check status inside lock — may have been completed between getByStatus and lock
                if (job.getStatus() != JobStatus.CLAIMED) continue;
                if (job.getLeaseExpiresAt() > now) continue;

                log.warn("Lease expired — reclaiming job: jobId={}, type={}", job.getJobId(), job.getSpec().type());

                // Clear worker state for whoever held this lease
                String holdingWorker = workerRegistry.findWorkerByLeaseToken(job.getCurrentLeaseToken());
                if (holdingWorker != null) workerRegistry.markIdle(holdingWorker);

                leaseManager.clearLease(job);
                exclusivityManager.release(job.getSpec().type());

                job.getStatus().validateTransition(JobStatus.PENDING);
                job.setStatus(JobStatus.PENDING);
                jobQueue.enqueue(job);
            }
        }
    }
}
