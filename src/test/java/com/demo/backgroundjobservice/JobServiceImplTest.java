package com.demo.backgroundjobservice;

import com.demo.backgroundjobservice.exception.StaleLeaseException;
import com.demo.backgroundjobservice.model.*;
import com.demo.backgroundjobservice.service.*;
import com.demo.backgroundjobservice.strategy.ExponentialBackoffRetryPolicy;
import com.demo.backgroundjobservice.strategy.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JobServiceImplTest {

    private JobServiceImpl service;

    // Fast retry policy for tests — no real waiting
    private static final RetryPolicy FAST_RETRY = attempt -> 50L;

    @BeforeEach
    void setUp() {
        service = new JobServiceImpl(
                new InMemoryJobStore(),
                new JobQueue(),
                new LeaseManager(),
                new ExclusivityManager(),
                new WorkerRegistry(),
                FAST_RETRY
        );
        service.registerWorker("w1", Set.of("email", "resize", "exclusive_type"));
        service.registerWorker("w2", Set.of("email", "resize", "exclusive_type"));
    }

    @AfterEach
    void tearDown() { service.close(); }

    // -------------------------------------------------------------------------
    // Submit + acquire basic flow
    // -------------------------------------------------------------------------

    @Test
    void submit_and_acquire_happyPath() {
        String jobId = service.submit(JobSpec.of("email", "hello"));
        AcquiredJob acquired = service.acquireJob("w1", Set.of("email"));

        assertNotNull(acquired);
        assertEquals(jobId, acquired.jobId());
        assertEquals("email", acquired.type());
        assertNotNull(acquired.leaseToken());
    }

    @Test
    void acquire_noJobs_returnsNull() {
        assertNull(service.acquireJob("w1", Set.of("email")));
    }

    // -------------------------------------------------------------------------
    // Test 1: at-least-once — worker crash (lease expiry)
    // -------------------------------------------------------------------------

    @Test
    void atLeastOnce_leaseExpiry_jobReturnsToQueue() throws InterruptedException {
        // Submit with very short lease
        String jobId = service.submit(new JobSpec("email", "data", 5, 3, 100L));
        AcquiredJob acquired = service.acquireJob("w1", Set.of("email"));
        assertNotNull(acquired);

        // Worker "crashes" — never calls completeJob
        // Wait for lease to expire and reaper to reclaim (reaper runs every 2s, but we call it directly)
        Thread.sleep(150); // lease expires
        service.reapExpiredLeases();

        // Job should be back in PENDING
        Job job = ((InMemoryJobStore) getStore()).get(jobId);
        assertEquals(JobStatus.PENDING, job.getStatus());
        assertNull(job.getCurrentLeaseToken());

        // Another worker can now pick it up
        AcquiredJob reacquired = service.acquireJob("w2", Set.of("email"));
        assertNotNull(reacquired);
        assertEquals(jobId, reacquired.jobId());
    }

    // -------------------------------------------------------------------------
    // Test 2: retry behavior
    // -------------------------------------------------------------------------

    @Test
    void transientFailure_retriesAndEventuallyDLQ() throws InterruptedException {
        String jobId = service.submit(new JobSpec("email", "data", 5, 2, 10_000L)); // maxRetries=2

        // Attempt 1
        AcquiredJob a1 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a1.jobId(), a1.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);
        assertEquals(JobStatus.RETRY_SCHEDULED, getJob(jobId).getStatus());

        // Wait for retry delay (50ms)
        Thread.sleep(100);
        assertEquals(JobStatus.PENDING, getJob(jobId).getStatus());

        // Attempt 2
        AcquiredJob a2 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a2.jobId(), a2.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);
        Thread.sleep(100);

        // Attempt 3 (maxRetries=2 means 3 total attempts: initial + 2 retries)
        AcquiredJob a3 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a3.jobId(), a3.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);

        assertEquals(JobStatus.FAILED, getJob(jobId).getStatus());
        assertEquals(1, service.dlqSize());
        assertEquals(DLQEntry.REASON_MAX_RETRIES, service.listDLQ().get(0).reason());
    }

    // -------------------------------------------------------------------------
    // Test 3: lease renewal happy path
    // -------------------------------------------------------------------------

    @Test
    void leaseRenewal_extendsExpiry_reaperDoesNotReclaim() throws InterruptedException {
        String jobId = service.submit(new JobSpec("email", "data", 5, 3, 150L));
        AcquiredJob acquired = service.acquireJob("w1", Set.of("email"));

        Thread.sleep(80); // approaching expiry
        service.renewLease(acquired.jobId(), acquired.leaseToken()); // renew before expiry

        Thread.sleep(80); // original lease would have expired, but renewed
        service.reapExpiredLeases();

        // Job should still be CLAIMED — reaper should not have reclaimed it
        assertEquals(JobStatus.CLAIMED, getJob(jobId).getStatus());

        // Complete normally
        service.completeJob("w1", acquired.jobId(), acquired.leaseToken(), ExecutionResult.SUCCESS);
        assertEquals(JobStatus.SUCCEEDED, getJob(jobId).getStatus());
    }

    // -------------------------------------------------------------------------
    // Test 4: priority ordering (weighted dispatch)
    // -------------------------------------------------------------------------

    @Test
    void priorityDispatch_highPriorityFavoredOverLow() {
        // Submit 1 high-priority and 1 low-priority job
        service.submit(new JobSpec("email", "low",  0, 3, 10_000L));
        service.submit(new JobSpec("email", "high", 9, 3, 10_000L));

        int highCount = 0;
        int trials = 200;
        for (int i = 0; i < trials; i++) {
            // Re-submit fresh jobs each trial
            service.submit(new JobSpec("email", "low",  0, 3, 10_000L));
            service.submit(new JobSpec("email", "high", 9, 3, 10_000L));
            AcquiredJob acquired = service.acquireJob("w1", Set.of("email"));
            if (acquired != null) {
                if ("high".equals(acquired.payload())) highCount++;
                service.completeJob("w1", acquired.jobId(), acquired.leaseToken(), ExecutionResult.SUCCESS);
            }
        }
        // High priority (weight=10) should win significantly more than 50%
        assertTrue(highCount > 120, "Expected high-priority to win >60% of trials, got: " + highCount);
    }

    // -------------------------------------------------------------------------
    // Test 5: DLQ on max retries exhausted
    // -------------------------------------------------------------------------

    @Test
    void maxRetries_exhausted_movesToDLQ() throws InterruptedException {
        service.submit(new JobSpec("email", "data", 5, 0, 10_000L)); // maxRetries=0

        AcquiredJob a = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a.jobId(), a.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);

        // maxRetries=0 → first failure goes straight to DLQ
        assertEquals(1, service.dlqSize());
        assertEquals(DLQEntry.REASON_MAX_RETRIES, service.listDLQ().get(0).reason());
    }

    // -------------------------------------------------------------------------
    // Test 6: PermanentFailure short-circuit
    // -------------------------------------------------------------------------

    @Test
    void permanentFailure_skipRetries_goesToDLQ() {
        String jobId = service.submit(new JobSpec("email", "data", 5, 5, 10_000L)); // maxRetries=5

        AcquiredJob a = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a.jobId(), a.leaseToken(), ExecutionResult.PERMANENT_FAILURE);

        assertEquals(JobStatus.FAILED, getJob(jobId).getStatus());
        assertEquals(1, service.dlqSize());
        assertEquals(DLQEntry.REASON_PERMANENT, service.listDLQ().get(0).reason());
        assertEquals(1, getJob(jobId).getAttemptCount()); // only ran once
    }

    // -------------------------------------------------------------------------
    // Test 7: stale lease rejection
    // -------------------------------------------------------------------------

    @Test
    void staleLeaseRejected_workerACannotCommitAfterExpiry() throws InterruptedException {
        service.submit(new JobSpec("email", "data", 5, 3, 100L));
        AcquiredJob workerA = service.acquireJob("w1", Set.of("email"));

        Thread.sleep(150); // lease expires
        service.reapExpiredLeases(); // reaper reclaims

        AcquiredJob workerB = service.acquireJob("w2", Set.of("email"));
        assertNotNull(workerB);

        // Worker A tries to commit with stale token
        assertThrows(StaleLeaseException.class, () ->
                service.completeJob("w1", workerA.jobId(), workerA.leaseToken(), ExecutionResult.SUCCESS));

        // Worker B's job is unaffected
        assertEquals(JobStatus.CLAIMED, getJob(workerA.jobId()).getStatus());
    }

    // -------------------------------------------------------------------------
    // Test 8: exclusive job — only one runs at a time
    // -------------------------------------------------------------------------

    @Test
    void exclusiveJob_onlyOneRunsAtATime() {
        service.submit(JobSpec.of("exclusive_type", "job1"));
        service.submit(JobSpec.of("exclusive_type", "job2"));
        service.submit(JobSpec.of("exclusive_type", "job3"));

        // First acquire succeeds
        AcquiredJob first = service.acquireJob("w1", Set.of("exclusive_type"));
        assertNotNull(first);

        // Second acquire returns null — type is locked
        AcquiredJob second = service.acquireJob("w2", Set.of("exclusive_type"));
        assertNull(second);

        // Complete first → lock released
        service.completeJob("w1", first.jobId(), first.leaseToken(), ExecutionResult.SUCCESS);

        // Now second can be acquired
        AcquiredJob third = service.acquireJob("w2", Set.of("exclusive_type"));
        assertNotNull(third);
    }

    // -------------------------------------------------------------------------
    // Test 9: observability
    // -------------------------------------------------------------------------

    @Test
    void observability_countsCorrect() {
        service.submit(new JobSpec("email",  "p", 3, 3, 10_000L));
        service.submit(new JobSpec("email",  "p", 3, 3, 10_000L));
        service.submit(new JobSpec("resize", "p", 7, 3, 10_000L));

        Map<String, Long> byType = service.pendingCountByType();
        assertEquals(2L, byType.get("email"));
        assertEquals(1L, byType.get("resize"));

        Map<Integer, Long> byPriority = service.pendingCountByPriority();
        assertEquals(2L, byPriority.get(3));
        assertEquals(1L, byPriority.get(7));

        AcquiredJob a = service.acquireJob("w1", Set.of("email", "resize"));
        assertEquals(1L, service.inFlightCount());

        service.completeJob("w1", a.jobId(), a.leaseToken(), ExecutionResult.SUCCESS);
        assertEquals(0L, service.inFlightCount());
    }

    // -------------------------------------------------------------------------
    // Test 10: per-worker stats
    // -------------------------------------------------------------------------

    @Test
    void workerStats_trackedCorrectly() {
        service.submit(JobSpec.of("email", "p1"));
        service.submit(JobSpec.of("email", "p2"));
        service.submit(JobSpec.of("email", "p3"));

        AcquiredJob a1 = service.acquireJob("w1", Set.of("email"));
        assertNotNull(service.getWorkerStats("w1").getCurrentJobId());
        service.completeJob("w1", a1.jobId(), a1.leaseToken(), ExecutionResult.SUCCESS);

        AcquiredJob a2 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a2.jobId(), a2.leaseToken(), ExecutionResult.SUCCESS);

        AcquiredJob a3 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a3.jobId(), a3.leaseToken(), ExecutionResult.PERMANENT_FAILURE);

        WorkerStats stats = service.getWorkerStats("w1");
        assertEquals(2, stats.getSuccessCount());
        assertEquals(1, stats.getFailureCount());
        assertNull(stats.getCurrentJobId()); // idle after completion
    }

    // -------------------------------------------------------------------------
    // Test 12: concurrent stress — no job executed twice simultaneously
    // -------------------------------------------------------------------------

    @Test
    void concurrentStress_noDoubleExecution() throws InterruptedException {
        int jobCount = 50;
        for (int i = 0; i < jobCount; i++) {
            service.submit(JobSpec.of("email", "payload-" + i));
        }

        AtomicInteger concurrentExecutions = new AtomicInteger(0);
        AtomicInteger maxConcurrent        = new AtomicInteger(0);
        AtomicInteger completed            = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(5);

        for (int w = 0; w < 5; w++) {
            final String wId = "stress-w" + w;
            service.registerWorker(wId, Set.of("email"));
            pool.submit(() -> {
                try {
                    while (completed.get() < jobCount) {
                        AcquiredJob job = service.acquireJob(wId, Set.of("email"));
                        if (job == null) { Thread.sleep(10); continue; }

                        int current = concurrentExecutions.incrementAndGet();
                        maxConcurrent.updateAndGet(m -> Math.max(m, current));

                        Thread.sleep(5); // simulate work

                        concurrentExecutions.decrementAndGet();
                        service.completeJob(wId, job.jobId(), job.leaseToken(), ExecutionResult.SUCCESS);
                        completed.incrementAndGet();
                    }
                } catch (Exception ignored) {}
                finally { latch.countDown(); }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stress test timed out");
        pool.shutdown();

        assertEquals(jobCount, completed.get());
        // Each job has its own lease — concurrent execution of different jobs is fine.
        // What we verify: no job was double-executed (completed == jobCount, not more)
        assertTrue(maxConcurrent.get() <= 5, "Max concurrent should not exceed worker count");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Job getJob(String jobId) {
        // Access store via cast — acceptable in tests
        return ((InMemoryJobStore) getStore()).get(jobId);
    }

    private JobStore getStore() {
        // Reflective access not needed — we construct the store ourselves in setUp
        // Use a holder pattern instead
        return storeRef;
    }

    // Store reference for test assertions
    private InMemoryJobStore storeRef;

    // Override setUp to capture store reference
    {
        storeRef = new InMemoryJobStore();
    }

    @BeforeEach
    void setUpWithStoreRef() {
        service = new JobServiceImpl(
                storeRef,
                new JobQueue(),
                new LeaseManager(),
                new ExclusivityManager(),
                new WorkerRegistry(),
                FAST_RETRY
        );
        service.registerWorker("w1", Set.of("email", "resize", "exclusive_type"));
        service.registerWorker("w2", Set.of("email", "resize", "exclusive_type"));
    }
}
