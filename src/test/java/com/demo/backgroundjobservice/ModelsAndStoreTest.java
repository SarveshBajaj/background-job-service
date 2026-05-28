package com.demo.backgroundjobservice;

import com.demo.backgroundjobservice.exception.JobNotFoundException;
import com.demo.backgroundjobservice.model.*;
import com.demo.backgroundjobservice.service.InMemoryJobStore;
import com.demo.backgroundjobservice.service.JobQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ModelsAndStoreTest {

    private InMemoryJobStore store;

    @BeforeEach
    void setUp() { store = new InMemoryJobStore(); }

    // --- JobSpec validation ---

    @Test
    void jobSpec_blankType_throws() {
        assertThrows(IllegalArgumentException.class, () -> new JobSpec("", "p", 5, 3, 10_000));
    }

    @Test
    void jobSpec_priorityOutOfRange_throws() {
        assertThrows(IllegalArgumentException.class, () -> new JobSpec("t", "p", 10, 3, 10_000));
        assertThrows(IllegalArgumentException.class, () -> new JobSpec("t", "p", -1, 3, 10_000));
    }

    @Test
    void jobSpec_negativeMaxRetries_throws() {
        assertThrows(IllegalArgumentException.class, () -> new JobSpec("t", "p", 5, -1, 10_000));
    }

    @Test
    void jobSpec_defaults_correct() {
        JobSpec spec = JobSpec.of("send_email", "payload");
        assertEquals(5, spec.priority());
        assertEquals(3, spec.maxRetries());
        assertEquals(10_000L, spec.leaseDurationMs());
    }

    // --- JobStatus transitions ---

    @Test
    void jobStatus_validTransitions_succeed() {
        assertDoesNotThrow(() -> JobStatus.PENDING.validateTransition(JobStatus.CLAIMED));
        assertDoesNotThrow(() -> JobStatus.CLAIMED.validateTransition(JobStatus.SUCCEEDED));
        assertDoesNotThrow(() -> JobStatus.CLAIMED.validateTransition(JobStatus.RETRY_SCHEDULED));
        assertDoesNotThrow(() -> JobStatus.CLAIMED.validateTransition(JobStatus.PENDING));
        assertDoesNotThrow(() -> JobStatus.RETRY_SCHEDULED.validateTransition(JobStatus.PENDING));
    }

    @Test
    void jobStatus_invalidTransitions_throw() {
        assertThrows(IllegalStateException.class, () -> JobStatus.PENDING.validateTransition(JobStatus.SUCCEEDED));
        assertThrows(IllegalStateException.class, () -> JobStatus.SUCCEEDED.validateTransition(JobStatus.PENDING));
        assertThrows(IllegalStateException.class, () -> JobStatus.FAILED.validateTransition(JobStatus.PENDING));
    }

    // --- InMemoryJobStore ---

    @Test
    void store_saveAndGet_roundtrip() {
        Job job = newJob("send_email");
        store.save(job);
        assertSame(job, store.get(job.getJobId()));
    }

    @Test
    void store_duplicateSave_throws() {
        Job job = newJob("send_email");
        store.save(job);
        assertThrows(IllegalArgumentException.class, () -> store.save(job));
    }

    @Test
    void store_getUnknownId_throws() {
        assertThrows(JobNotFoundException.class, () -> store.get("nonexistent"));
    }

    @Test
    void store_getByStatus_filtersCorrectly() {
        Job j1 = newJob("a");
        Job j2 = newJob("b");
        j2.setStatus(JobStatus.CLAIMED);
        store.save(j1);
        store.save(j2);

        List<Job> pending = store.getByStatus(JobStatus.PENDING);
        assertEquals(1, pending.size());
        assertEquals(j1.getJobId(), pending.get(0).getJobId());
    }

    @Test
    void store_dlq_addAndList() {
        Job job = newJob("t");
        store.save(job);
        store.addToDLQ(job, DLQEntry.REASON_MAX_RETRIES);

        assertEquals(1, store.dlqSize());
        List<DLQEntry> entries = store.listDLQ();
        assertEquals(1, entries.size());
        assertEquals(DLQEntry.REASON_MAX_RETRIES, entries.get(0).reason());
    }

    // --- JobQueue ---

    @Test
    void queue_enqueueAndPoll_returnsJob() {
        JobQueue queue = new JobQueue();
        Job job = newJob("t");
        queue.enqueue(job);

        Job polled = queue.poll(Set.of("t"), Set.of());
        assertSame(job, polled);
    }

    @Test
    void queue_poll_skipsExcludedTypes() {
        JobQueue queue = new JobQueue();
        queue.enqueue(newJob("exclusive_type"));

        Job result = queue.poll(Set.of("exclusive_type"), Set.of("exclusive_type"));
        assertNull(result);
        assertEquals(1, queue.size()); // re-enqueued
    }

    @Test
    void queue_poll_skipsUnhandledTypes() {
        JobQueue queue = new JobQueue();
        queue.enqueue(newJob("resize_image"));

        Job result = queue.poll(Set.of("send_email"), Set.of());
        assertNull(result);
        assertEquals(1, queue.size()); // re-enqueued
    }

    @Test
    void queue_weightedDispatch_highPriorityFavored() {
        JobQueue queue = new JobQueue();
        // 1 high-priority job (p9, weight=10) vs 1 low-priority job (p0, weight=1)
        Job highPri = new Job(UUID.randomUUID().toString(), new JobSpec("t", "p", 9, 3, 10_000));
        Job lowPri  = new Job(UUID.randomUUID().toString(), new JobSpec("t", "p", 0, 3, 10_000));

        int highCount = 0;
        int trials = 1000;
        for (int i = 0; i < trials; i++) {
            queue.enqueue(highPri);
            queue.enqueue(lowPri);
            Job selected = queue.poll(Set.of("t"), Set.of());
            if (selected == highPri) highCount++;
        }

        // High priority has weight 10/(10+1) ≈ 90.9% — expect well above 50%
        assertTrue(highCount > 700, "Expected high-priority to win >70% of trials, got: " + highCount);
    }

    @Test
    void queue_emptyPoll_returnsNull() {
        JobQueue queue = new JobQueue();
        assertNull(queue.poll(Set.of("t"), Set.of()));
    }

    // --- WorkerStats ---

    @Test
    void workerStats_markBusyAndIdle() {
        WorkerStats stats = new WorkerStats("w1", Set.of("t"));
        stats.markBusy("job-1", "token-abc");
        assertEquals("job-1", stats.getCurrentJobId());
        assertEquals("token-abc", stats.getCurrentLeaseToken());

        stats.markIdle();
        assertNull(stats.getCurrentJobId());
        assertNull(stats.getCurrentLeaseToken());
    }

    @Test
    void workerStats_counters_threadSafe() throws InterruptedException {
        WorkerStats stats = new WorkerStats("w1", Set.of("t"));
        Thread t1 = new Thread(() -> { for (int i = 0; i < 500; i++) stats.incrementSuccess(); });
        Thread t2 = new Thread(() -> { for (int i = 0; i < 500; i++) stats.incrementSuccess(); });
        t1.start(); t2.start();
        t1.join();  t2.join();
        assertEquals(1000, stats.getSuccessCount());
    }

    // --- Helpers ---

    private Job newJob(String type) {
        return new Job(UUID.randomUUID().toString(), JobSpec.of(type, "payload"));
    }
}
