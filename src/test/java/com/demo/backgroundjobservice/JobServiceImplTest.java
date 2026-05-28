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

    // Latch-based retry policy: fires latch when retry is scheduled, test waits on it
    private CountDownLatch retryLatch;
    private RetryPolicy latchRetry;

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
    // Test 2: retry behavior — uses CountDownLatch, no Thread.sleep
    // -------------------------------------------------------------------------

    @Test
    void transientFailure_retriesAndEventuallyDLQ() throws InterruptedException {
        CountDownLatch attempt2Ready = new CountDownLatch(1);
        CountDownLatch attempt3Ready = new CountDownLatch(1);
        CountDownLatch dlqReady      = new CountDownLatch(1);

        // Retry policy fires latch after scheduling delay (0ms = immediate for test)
        RetryPolicy zeroDelay = attempt -> 0L;
        service = new JobServiceImpl(storeRef = new InMemoryJobStore(),
                new JobQueue(), new LeaseManager(), new ExclusivityManager(),
                new WorkerRegistry(), zeroDelay);
        service.registerWorker("w1", Set.of("email"));

        String jobId = service.submit(new JobSpec("email", "data", 5, 2, 10_000L)); // maxRetries=2

        // Attempt 1
        AcquiredJob a1 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a1.jobId(), a1.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);
        assertEquals(JobStatus.RETRY_SCHEDULED, getJob(jobId).getStatus());

        // With 0ms delay the scheduler fires almost immediately — spin-wait (no sleep)
        long deadline = System.currentTimeMillis() + 2000;
        while (getJob(jobId).getStatus() != JobStatus.PENDING && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(JobStatus.PENDING, getJob(jobId).getStatus());

        // Attempt 2
        AcquiredJob a2 = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a2.jobId(), a2.leaseToken(), ExecutionResult.TRANSIENT_FAILURE);
        deadline = System.currentTimeMillis() + 2000;
        while (getJob(jobId).getStatus() != JobStatus.PENDING && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }

        // Attempt 3 — exhausts retries
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
    // Input validation
    // -------------------------------------------------------------------------

    @Test
    void submit_nullSpec_throws() {
        assertThrows(IllegalArgumentException.class, () -> service.submit(null));
    }

    @Test
    void acquireJob_blankWorkerId_throws() {
        assertThrows(IllegalArgumentException.class, () -> service.acquireJob("", Set.of("email")));
    }

    @Test
    void acquireJob_emptyHandledTypes_throws() {
        assertThrows(IllegalArgumentException.class, () -> service.acquireJob("w1", Set.of()));
    }

    @Test
    void completeJob_nullResult_throws() {
        service.submit(JobSpec.of("email", "p"));
        AcquiredJob a = service.acquireJob("w1", Set.of("email"));
        assertThrows(IllegalArgumentException.class, () ->
                service.completeJob("w1", a.jobId(), a.leaseToken(), null));
    }

    @Test
    void renewLease_blankJobId_throws() {
        assertThrows(IllegalArgumentException.class, () -> service.renewLease("", "token"));
    }

    @Test
    void renewLease_wrongToken_throwsStaleLeaseException() {
        service.submit(JobSpec.of("email", "p"));
        service.acquireJob("w1", Set.of("email"));
        String jobId = service.listDLQ().isEmpty()
                ? storeRef.getByStatus(JobStatus.CLAIMED).get(0).getJobId()
                : null;
        assertNotNull(jobId);
        assertThrows(StaleLeaseException.class, () -> service.renewLease(jobId, "wrong-token"));
    }

    // -------------------------------------------------------------------------
    // Reaper clears worker state
    // -------------------------------------------------------------------------

    @Test
    void reaper_clearsWorkerCurrentJobId_onLeaseExpiry() throws InterruptedException {
        service.submit(new JobSpec("email", "data", 5, 3, 100L));
        service.acquireJob("w1", Set.of("email"));

        assertNotNull(service.getWorkerStats("w1").getCurrentJobId());

        Thread.sleep(150);
        service.reapExpiredLeases();

        assertNull(service.getWorkerStats("w1").getCurrentJobId(),
                "Reaper must clear worker currentJobId on lease expiry");
    }

    // -------------------------------------------------------------------------
    // ExponentialBackoffRetryPolicy formula
    // -------------------------------------------------------------------------

    @Test
    void exponentialBackoff_delayGrowsWithAttempts() {
        var policy = new com.demo.backgroundjobservice.strategy.ExponentialBackoffRetryPolicy();
        long d0 = policy.computeDelayMs(0);
        long d1 = policy.computeDelayMs(1);
        long d2 = policy.computeDelayMs(2);
        // Each delay should be >= previous base (5s * 2^attempt), allowing for jitter
        assertTrue(d0 >= 5_000L,  "attempt 0 delay should be >= 5s");
        assertTrue(d1 >= 10_000L, "attempt 1 delay should be >= 10s");
        assertTrue(d2 >= 20_000L, "attempt 2 delay should be >= 20s");
        // Cap at 300s + jitter
        assertTrue(policy.computeDelayMs(100) <= 305_000L, "delay should be capped");
    }

    // -------------------------------------------------------------------------
    // DLQ immutability
    // -------------------------------------------------------------------------

    @Test
    void listDLQ_returnsImmutableSnapshot() {
        service.submit(JobSpec.of("email", "p"));
        AcquiredJob a = service.acquireJob("w1", Set.of("email"));
        service.completeJob("w1", a.jobId(), a.leaseToken(), ExecutionResult.PERMANENT_FAILURE);

        List<DLQEntry> dlq = service.listDLQ();
        assertThrows(UnsupportedOperationException.class, () -> dlq.add(dlq.get(0)));
    }

    // -------------------------------------------------------------------------
    // Exclusive type: lock released on failure too
    // -------------------------------------------------------------------------

    @Test
    void exclusiveJob_lockReleasedOnFailure_nextJobCanRun() {
        service.submit(JobSpec.of("exclusive_type", "j1"));
        service.submit(JobSpec.of("exclusive_type", "j2"));

        AcquiredJob first = service.acquireJob("w1", Set.of("exclusive_type"));
        assertNotNull(first);
        assertNull(service.acquireJob("w2", Set.of("exclusive_type"))); // locked

        service.completeJob("w1", first.jobId(), first.leaseToken(), ExecutionResult.PERMANENT_FAILURE);

        // Lock must be released even on failure
        AcquiredJob second = service.acquireJob("w2", Set.of("exclusive_type"));
        assertNotNull(second, "Exclusivity lock must be released after failure");
    }

    // -------------------------------------------------------------------------
    // Concurrent acquire — same job not given to two workers
    // -------------------------------------------------------------------------

    @Test
    void concurrentAcquire_sameJobNotGivenTwice() throws InterruptedException {
        service.submit(JobSpec.of("email", "single-job"));

        int workerCount = 10;
        for (int i = 2; i < workerCount + 2; i++) {
            service.registerWorker("cw" + i, Set.of("email"));
        }

        CyclicBarrier barrier = new CyclicBarrier(workerCount);
        AtomicInteger acquired = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(workerCount);
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);

        for (int i = 2; i < workerCount + 2; i++) {
            final String wId = "cw" + i;
            pool.submit(() -> {
                try {
                    barrier.await(); // all threads start simultaneously
                    AcquiredJob job = service.acquireJob(wId, Set.of("email"));
                    if (job != null) acquired.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(1, acquired.get(), "Only one worker should acquire the single job");
    }

    // -------------------------------------------------------------------------
    // Reaper double-check: job completed between getByStatus and synchronized(job)
    // -------------------------------------------------------------------------

    @Test
    void reaper_doesNotReclaimJobCompletedBeforeLock() throws InterruptedException {
        service.submit(new JobSpec("email", "data", 5, 3, 100L));
        AcquiredJob a = service.acquireJob("w1", Set.of("email"));

        Thread.sleep(150); // lease expires

        // Worker completes BEFORE reaper runs
        service.completeJob("w1", a.jobId(), a.leaseToken(), ExecutionResult.SUCCESS);
        assertEquals(JobStatus.SUCCEEDED, getJob(a.jobId()).getStatus());

        // Reaper runs — must not touch the already-SUCCEEDED job
        service.reapExpiredLeases();
        assertEquals(JobStatus.SUCCEEDED, getJob(a.jobId()).getStatus(), "Reaper must not reclaim a completed job");
        assertEquals(0, service.dlqSize());
    }

    // -------------------------------------------------------------------------
    // Exclusive lock on type A does not block dispatch of type B (same worker)
    // -------------------------------------------------------------------------

    @Test
    void exclusiveLock_typeA_doesNotBlockTypeB() {
        service.submit(JobSpec.of("exclusive_type", "job-a"));
        service.submit(JobSpec.of("email", "job-b"));

        // Acquire exclusive_type specifically (submit only that type to a dedicated worker)
        // Use a worker that only handles exclusive_type to guarantee which job it gets
        service.registerWorker("exclusive-only", Set.of("exclusive_type"));
        AcquiredJob typeA = service.acquireJob("exclusive-only", Set.of("exclusive_type"));
        assertNotNull(typeA);
        assertEquals("exclusive_type", typeA.type());

        // email is a different type — must still be dispatchable despite exclusive_type being locked
        AcquiredJob typeB = service.acquireJob("w2", Set.of("email"));
        assertNotNull(typeB, "Exclusive lock on type A must not block dispatch of type B");
        assertEquals("email", typeB.type());
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
