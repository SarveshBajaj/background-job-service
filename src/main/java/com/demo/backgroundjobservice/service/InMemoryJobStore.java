package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.exception.JobNotFoundException;
import com.demo.backgroundjobservice.model.DLQEntry;
import com.demo.backgroundjobservice.model.Job;
import com.demo.backgroundjobservice.model.JobStatus;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory implementation of {@link JobStore}.
 * ConcurrentHashMap for O(1) job lookup; CopyOnWriteArrayList for DLQ (low write, read-heavy).
 *
 * <p>Persistence boundary: replace this class with a DB-backed implementation to add durability.
 */
public class InMemoryJobStore implements JobStore {

    // jobId → Job; ConcurrentHashMap for thread-safe O(1) access
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();

    // Low write frequency — CopyOnWriteArrayList avoids locking on reads
    private final CopyOnWriteArrayList<DLQEntry> dlq = new CopyOnWriteArrayList<>();

    @Override
    public void save(Job job) {
        if (job == null) throw new IllegalArgumentException("job must not be null");
        if (jobs.putIfAbsent(job.getJobId(), job) != null) {
            throw new IllegalArgumentException("Job already exists: " + job.getJobId());
        }
    }

    @Override
    public Job get(String jobId) {
        if (jobId == null || jobId.isBlank()) throw new IllegalArgumentException("jobId must not be blank");
        Job job = jobs.get(jobId);
        if (job == null) throw new JobNotFoundException(jobId);
        return job;
    }

    @Override
    public List<Job> getByStatus(JobStatus status) {
        if (status == null) throw new IllegalArgumentException("status must not be null");
        // snapshot — callers must not mutate the returned list
        return jobs.values().stream()
                .filter(j -> j.getStatus() == status)
                .toList();
    }

    @Override
    public void addToDLQ(Job job, String reason) {
        if (job == null)                       throw new IllegalArgumentException("job must not be null");
        if (reason == null || reason.isBlank()) throw new IllegalArgumentException("reason must not be blank");
        dlq.add(new DLQEntry(job, reason, Instant.now()));
    }

    @Override
    public long dlqSize() {
        return dlq.size();
    }

    @Override
    public List<DLQEntry> listDLQ() {
        return List.copyOf(dlq);
    }
}
