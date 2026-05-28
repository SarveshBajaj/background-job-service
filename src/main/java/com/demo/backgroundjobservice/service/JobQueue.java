package com.demo.backgroundjobservice.service;

import com.demo.backgroundjobservice.model.Job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Priority-aware job queue backed by a {@link PriorityBlockingQueue}.
 *
 * <p>Ordering: higher priority dispatched more often via weighted random selection.
 * Same-priority jobs are ordered FIFO by submission time (createdAt).
 *
 * <p>Weighted dispatch: weight = priority + 1 (so p9 = weight 10, p0 = weight 1).
 * No starvation — every job has weight >= 1.
 */
public class JobQueue {

    private static final Logger log = LoggerFactory.getLogger(JobQueue.class);

    // Natural order: higher priority first, then older jobs first (FIFO within same priority)
    private static final Comparator<Job> JOB_COMPARATOR =
            Comparator.comparingInt((Job j) -> j.getSpec().priority()).reversed()
                      .thenComparing(j -> j.getCreatedAt());

    private final PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>(16, JOB_COMPARATOR);

    /**
     * Enqueues a job for dispatch.
     *
     * @param job the job to enqueue; must be in PENDING status
     */
    public void enqueue(Job job) {
        if (job == null) throw new IllegalArgumentException("job must not be null");
        queue.offer(job);
        log.debug("Job enqueued: jobId={}, type={}, priority={}, queueSize={}", job.getJobId(), job.getSpec().type(), job.getSpec().priority(), queue.size());
    }

    /**
     * Selects the next job for a worker using weighted random selection across all
     * jobs whose type is in {@code handledTypes} and not blocked by exclusivity.
     *
     * <p>Non-selected candidates are re-enqueued. This is safe because the caller
     * will immediately attempt to acquire a lease — if another thread races and takes
     * the job first, the caller retries.
     *
     * @param handledTypes  types this worker can handle
     * @param excludedTypes types currently locked by ExclusivityManager (exclusive job running)
     * @return selected Job, or null if no eligible job exists
     */
    public Job poll(Set<String> handledTypes, Set<String> excludedTypes) {
        if (handledTypes == null || handledTypes.isEmpty())
            throw new IllegalArgumentException("handledTypes must not be empty");

        // Drain all candidates matching handledTypes and not excluded
        List<Job> candidates = new ArrayList<>();
        List<Job> skipped    = new ArrayList<>();

        Job job;
        while ((job = queue.poll()) != null) {
            String type = job.getSpec().type();
            if (handledTypes.contains(type) && !excludedTypes.contains(type)) {
                candidates.add(job);
            } else {
                skipped.add(job);
            }
        }

        // Re-enqueue skipped jobs immediately
        queue.addAll(skipped);

        if (candidates.isEmpty()) {
            log.debug("No eligible jobs for handledTypes={}", handledTypes);
            return null;
        }

        // Weighted random selection: weight = priority + 1
        int totalWeight = candidates.stream()
                .mapToInt(j -> j.getSpec().priority() + 1)
                .sum();
        int rand = ThreadLocalRandom.current().nextInt(totalWeight);
        int cumulative = 0;
        Job selected = candidates.getLast(); // fallback to last (shouldn't be needed)
        for (Job c : candidates) {
            cumulative += c.getSpec().priority() + 1;
            if (rand < cumulative) {
                selected = c;
                break;
            }
        }

        // Re-enqueue non-selected candidates
        for (Job c : candidates) {
            if (c != selected) queue.offer(c);
        }

        log.debug("Job dispatched: jobId={}, type={}, priority={}", selected.getJobId(), selected.getSpec().type(), selected.getSpec().priority());
        return selected;
    }

    /** @return current number of jobs waiting in the queue */
    public int size() {
        return queue.size();
    }
}
