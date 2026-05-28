# Background Job Service

An in-process background job service — a minimal Sidekiq/Celery/SQS-with-workers, all in one JVM process.

## Features

- **Submit jobs** with type, payload, priority (0-9), and max retries
- **Worker model** — workers register for job types, acquire jobs via lease (visibility timeout)
- **At-least-once execution** — expired leases are reclaimed by a background reaper and re-dispatched
- **Exclusive jobs** — at most one job of a given type runs concurrently; others queue behind it
- **Retry with exponential backoff + jitter** — `min(5s * 2^attempt, 300s) + random(0, 5s)`
- **Dead Letter Queue** — jobs moved to DLQ after max retries exhausted or `PermanentFailure`
- **Stale lease rejection** — Worker A's commit is rejected if its lease expired and Worker B re-acquired
- **Weighted fair dispatch** — priority 9 gets 10x more dispatch weight than priority 0; no starvation
- **Observability** — pending counts by type/priority, in-flight count, DLQ, per-worker stats

## Architecture

```
model/          — Job, JobSpec, JobStatus, AcquiredJob, DLQEntry, WorkerStats (dumb data)
service/        — JobServiceImpl (orchestrator), InMemoryJobStore, JobQueue,
                  LeaseManager, ExclusivityManager, WorkerRegistry
strategy/       — RetryPolicy, ExponentialBackoffRetryPolicy, JobHandler
exception/      — StaleLeaseException, JobNotFoundException, WorkerNotFoundException
```

**Key design decisions:**
- `JobService` and `JobStore` are interfaces — network transport and persistence are swappable without touching business logic
- State transitions validated in `JobStatus` enum — invalid transition throws `IllegalStateException`
- Per-job `synchronized(job)` locking — no global lock
- `ExclusivityManager` uses `AtomicBoolean.compareAndSet` — lock-free exclusivity

## Running Tests

```bash
mvn test
```

63 tests covering: at-least-once, retry backoff, lease renewal, priority dispatch, DLQ, permanent failure short-circuit, stale lease rejection, exclusive jobs, observability, concurrency stress.

## State Machine

```
pending → claimed → succeeded
                 → failed (→ DLQ)
                 → retry_scheduled → pending
         ↑
    lease expiry (reaper)
```
