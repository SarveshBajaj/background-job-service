package com.demo.backgroundjobservice.exception;

/** Thrown when a workerId is not found in the worker registry. */
public class WorkerNotFoundException extends RuntimeException {
    public WorkerNotFoundException(String workerId) {
        super("Worker not found: " + workerId);
    }
}
