package com.demo.backgroundjobservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enforces the exclusive-job invariant: at most one job of a given type runs concurrently.
 *
 * <p>Uses a per-type {@link AtomicBoolean} with CAS — lock-free, O(1) acquire and release.
 * A type not present in the map is treated as unlocked.
 */
public class ExclusivityManager {

    private static final Logger log = LoggerFactory.getLogger(ExclusivityManager.class);

    // type → isRunning flag; computeIfAbsent ensures one AtomicBoolean per type
    private final ConcurrentHashMap<String, AtomicBoolean> locks = new ConcurrentHashMap<>();

    /**
     * Attempts to acquire the exclusive lock for the given job type.
     *
     * @param type job type string
     * @return {@code true} if the lock was acquired (safe to dispatch), {@code false} if already running
     */
    public boolean tryAcquire(String type) {
        AtomicBoolean lock = locks.computeIfAbsent(type, k -> new AtomicBoolean(false));
        boolean acquired = lock.compareAndSet(false, true);
        if (acquired) {
            log.debug("Exclusivity lock acquired: type={}", type);
        } else {
            log.debug("Exclusivity lock busy — skipping dispatch: type={}", type);
        }
        return acquired;
    }

    /**
     * Releases the exclusive lock for the given job type.
     * Must be called on ALL exit paths: success, failure, DLQ, and lease expiry reclaim.
     *
     * @param type job type string
     */
    public void release(String type) {
        AtomicBoolean lock = locks.get(type);
        if (lock != null) {
            lock.set(false);
            log.debug("Exclusivity lock released: type={}", type);
        }
    }

    /**
     * Returns whether a given type is currently locked (a job of that type is running).
     *
     * @param type job type string
     * @return {@code true} if locked
     */
    public boolean isLocked(String type) {
        AtomicBoolean lock = locks.get(type);
        return lock != null && lock.get();
    }
}
