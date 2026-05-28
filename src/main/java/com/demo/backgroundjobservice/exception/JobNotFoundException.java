package com.demo.backgroundjobservice.exception;

/** Thrown when a jobId is not found in the job store. */
public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("Job not found: " + jobId);
    }
}
