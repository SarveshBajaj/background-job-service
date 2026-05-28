package com.demo.backgroundjobservice.strategy;

/**
 * Handler interface for a specific job type.
 * Implementations are registered per type and invoked by workers.
 *
 * <p>Handlers MUST be idempotent — the service guarantees at-least-once execution.
 * Use {@code jobId} as an idempotency key to detect duplicate executions.
 */
public interface JobHandler {

    /**
     * Executes the job.
     *
     * @param jobId   unique job identifier — use as idempotency key
     * @param payload opaque data submitted with the job
     * @return execution result indicating success or failure type
     */
    com.demo.backgroundjobservice.model.ExecutionResult run(String jobId, Object payload);
}
