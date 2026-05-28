package com.demo.backgroundjobservice;

import com.demo.backgroundjobservice.exception.StaleLeaseException;
import com.demo.backgroundjobservice.exception.WorkerNotFoundException;
import com.demo.backgroundjobservice.model.Job;
import com.demo.backgroundjobservice.model.JobSpec;
import com.demo.backgroundjobservice.model.WorkerStats;
import com.demo.backgroundjobservice.service.ExclusivityManager;
import com.demo.backgroundjobservice.service.LeaseManager;
import com.demo.backgroundjobservice.service.WorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LeaseExclusivityRegistryTest {

    private LeaseManager leaseManager;
    private ExclusivityManager exclusivityManager;
    private WorkerRegistry registry;

    @BeforeEach
    void setUp() {
        leaseManager       = new LeaseManager();
        exclusivityManager = new ExclusivityManager();
        registry           = new WorkerRegistry();
    }

    // --- LeaseManager ---

    @Test
    void issueLease_setsTokenAndExpiry() {
        Job job = newJob("t");
        String token = leaseManager.issueLease(job);

        assertNotNull(token);
        assertEquals(token, job.getCurrentLeaseToken());
        assertTrue(job.getLeaseExpiresAt() > System.currentTimeMillis());
    }

    @Test
    void validate_correctToken_passes() {
        Job job = newJob("t");
        String token = leaseManager.issueLease(job);
        assertDoesNotThrow(() -> leaseManager.validate(job, token));
    }

    @Test
    void validate_wrongToken_throwsStaleLeaseException() {
        Job job = newJob("t");
        leaseManager.issueLease(job);
        assertThrows(StaleLeaseException.class, () -> leaseManager.validate(job, "wrong-token"));
    }

    @Test
    void extend_increasesExpiry() throws InterruptedException {
        Job job = newJob("t");
        leaseManager.issueLease(job);
        long firstExpiry = job.getLeaseExpiresAt();

        Thread.sleep(5); // ensure time advances
        leaseManager.extend(job);

        assertTrue(job.getLeaseExpiresAt() > firstExpiry);
    }

    @Test
    void clearLease_nullsTokenAndExpiry() {
        Job job = newJob("t");
        leaseManager.issueLease(job);
        leaseManager.clearLease(job);

        assertNull(job.getCurrentLeaseToken());
        assertEquals(0, job.getLeaseExpiresAt());
    }

    @Test
    void validate_afterClear_throwsStaleLeaseException() {
        Job job = newJob("t");
        String token = leaseManager.issueLease(job);
        leaseManager.clearLease(job);
        // token is now stale — currentLeaseToken is null
        assertThrows(StaleLeaseException.class, () -> leaseManager.validate(job, token));
    }

    // --- ExclusivityManager ---

    @Test
    void tryAcquire_firstCall_succeeds() {
        assertTrue(exclusivityManager.tryAcquire("resize_image"));
    }

    @Test
    void tryAcquire_whileLocked_fails() {
        exclusivityManager.tryAcquire("resize_image");
        assertFalse(exclusivityManager.tryAcquire("resize_image"));
    }

    @Test
    void release_allowsReacquire() {
        exclusivityManager.tryAcquire("resize_image");
        exclusivityManager.release("resize_image");
        assertTrue(exclusivityManager.tryAcquire("resize_image"));
    }

    @Test
    void isLocked_reflectsState() {
        assertFalse(exclusivityManager.isLocked("t"));
        exclusivityManager.tryAcquire("t");
        assertTrue(exclusivityManager.isLocked("t"));
        exclusivityManager.release("t");
        assertFalse(exclusivityManager.isLocked("t"));
    }

    @Test
    void exclusivity_differentTypes_independent() {
        assertTrue(exclusivityManager.tryAcquire("type_a"));
        assertTrue(exclusivityManager.tryAcquire("type_b")); // different type — unaffected
    }

    @Test
    void exclusivity_concurrentAcquire_onlyOneSucceeds() throws InterruptedException {
        int[] acquired = {0};
        Runnable task = () -> {
            if (exclusivityManager.tryAcquire("shared_type")) {
                synchronized (acquired) { acquired[0]++; }
            }
        };

        Thread[] threads = new Thread[20];
        for (int i = 0; i < 20; i++) threads[i] = new Thread(task);
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        assertEquals(1, acquired[0], "Exactly one thread should acquire the lock");
    }

    // --- WorkerRegistry ---

    @Test
    void register_and_getStats_roundtrip() {
        registry.register("w1", Set.of("send_email"));
        WorkerStats stats = registry.getStats("w1");
        assertEquals("w1", stats.getWorkerId());
        assertTrue(stats.getHandledTypes().contains("send_email"));
    }

    @Test
    void register_duplicate_throws() {
        registry.register("w1", Set.of("t"));
        assertThrows(IllegalArgumentException.class, () -> registry.register("w1", Set.of("t")));
    }

    @Test
    void getStats_unknownWorker_throws() {
        assertThrows(WorkerNotFoundException.class, () -> registry.getStats("ghost"));
    }

    @Test
    void markBusy_and_markIdle_updatesStats() {
        registry.register("w1", Set.of("t"));
        registry.markBusy("w1", "job-1", "token-abc");

        WorkerStats stats = registry.getStats("w1");
        assertEquals("job-1", stats.getCurrentJobId());
        assertEquals("token-abc", stats.getCurrentLeaseToken());

        registry.markIdle("w1");
        assertNull(stats.getCurrentJobId());
        assertNull(stats.getCurrentLeaseToken());
    }

    @Test
    void recordSuccess_and_recordFailure_incrementCounters() {
        registry.register("w1", Set.of("t"));
        registry.recordSuccess("w1");
        registry.recordSuccess("w1");
        registry.recordFailure("w1");

        WorkerStats stats = registry.getStats("w1");
        assertEquals(2, stats.getSuccessCount());
        assertEquals(1, stats.getFailureCount());
    }

    @Test
    void findWorkerByLeaseToken_returnsCorrectWorker() {
        registry.register("w1", Set.of("t"));
        registry.register("w2", Set.of("t"));
        registry.markBusy("w1", "job-1", "token-xyz");

        assertEquals("w1", registry.findWorkerByLeaseToken("token-xyz"));
        assertNull(registry.findWorkerByLeaseToken("nonexistent-token"));
    }

    @Test
    void allStats_returnsAllWorkers() {
        registry.register("w1", Set.of("t"));
        registry.register("w2", Set.of("t"));
        assertEquals(2, registry.allStats().size());
    }

    // --- helper ---

    private Job newJob(String type) {
        return new Job(UUID.randomUUID().toString(), JobSpec.of(type, "payload"));
    }
}
