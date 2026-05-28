# Background Job Service — Design Plan

## 0. Feature Requirements

### Core (must-have)
- Submit job with type, payload, priority (0-9, default 5), max_retries (default 3)
- Worker registration with list of handled types
- Lease-based job acquisition with configurable timeout (default 10s, overridable per job)
- Lease renewal API; expiry returns job to pending (reaper thread)
- Full state machine: PENDING → CLAIMED → SUCCEEDED | FAILED | RETRY_SCHEDULED → PENDING
- Exponential backoff + jitter retry scheduling
- DLQ on max_retries exhausted or PermanentFailure
- Exclusive job type: at most 1 running per type concurrently
- Observability API: pending counts, in-flight count, DLQ, per-worker stats
- 7 test scenarios from spec

### Extension (if time permits)
- Manual DLQ requeue
- Graceful shutdown (drain in-flight jobs)
- Per-type lease duration override

### Core Invariant
**A claimed job must never be executed by two workers simultaneously.** The leaseToken is the enforcement mechanism — stale commits are rejected.

### Constraints
- Java 17, no external libs beyond JUnit 5
- In-memory only; persistence boundary must be explicit
- Thread-safe: multiple workers run concurrently as threads
- No network transport; interfaces must allow it to be added later

---

## 1. Entities & Relationships

```
JobSpec (record, immutable)
  - String type
  - Object payload
  - int priority          // 0-9, default 5
  - int maxRetries        // default 3
  - long leaseDurationMs  // default 10_000ms, overridable

Job (mutable, owns lifecycle state)
  - String jobId          // UUID
  - JobSpec spec
  - JobStatus status      // enum
  - int attemptCount
  - String currentLeaseToken   // null if not claimed
  - long leaseExpiresAt        // epoch ms, 0 if not claimed
  - long scheduledRunAt        // epoch ms, for RETRY_SCHEDULED
  - Instant createdAt
  - Instant updatedAt

JobStatus (enum)
  - PENDING, CLAIMED, SUCCEEDED, RETRY_SCHEDULED, FAILED

ExecutionResult (sealed interface / enum)
  - SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE

WorkerStats (mutable, per worker)
  - String workerId
  - Set<String> handledTypes
  - String currentJobId       // null if idle
  - String currentLeaseToken
  - long successCount
  - long failureCount

DLQEntry (record, immutable)
  - Job job
  - String reason             // "MAX_RETRIES_EXHAUSTED" | "PERMANENT_FAILURE"
  - Instant failedAt
```

**Ownership:**
- `JobService` owns `JobStore`, `LeaseManager`, `RetryScheduler`, `Reaper`, `ExclusivityManager`, `WorkerRegistry`
- `JobStore` owns all `Job` instances and the DLQ list
- Workers are registered in `WorkerRegistry` and reference `JobService` to acquire/complete jobs

---

## 2. Flow

### Submit
```
Producer → JobService.submit(spec)
  → JobStore.save(new Job(PENDING))
  → ExclusivityManager.register(type, jobId)   // track exclusive types
  → return jobId
```

### Dispatch (worker polling loop)
```
Worker thread → JobService.acquireJob(workerId, handledTypes)
  → JobQueue.poll(handledTypes)                 // weighted random from PriorityBlockingQueue
  → ExclusivityManager.tryLock(type)            // if type is running → skip, re-enqueue
  → LeaseManager.issueLease(jobId)              // generate leaseToken, set leaseExpiresAt
  → JobStore.transition(jobId, PENDING→CLAIMED)
  → WorkerRegistry.markBusy(workerId, jobId, leaseToken)
  → return AcquiredJob(jobId, payload, leaseToken)
```

### Execute
```
Worker → handler.run(payload, jobId)            // jobId exposed for idempotency key
  → SUCCESS   → JobService.completeJob(jobId, leaseToken, SUCCESS)
  → TRANSIENT → JobService.completeJob(jobId, leaseToken, TRANSIENT_FAILURE)
  → PERMANENT → JobService.completeJob(jobId, leaseToken, PERMANENT_FAILURE)
```

### Complete
```
JobService.completeJob(jobId, leaseToken, result)
  → LeaseManager.validate(jobId, leaseToken)    // mismatch → throw StaleLeaseException
  → if SUCCESS        → transition CLAIMED→SUCCEEDED, ExclusivityManager.release(type)
  → if PERMANENT      → transition CLAIMED→FAILED, JobStore.addToDLQ, ExclusivityManager.release(type)
  → if TRANSIENT      →
      if attemptCount < maxRetries → transition CLAIMED→RETRY_SCHEDULED, RetryScheduler.schedule(jobId, delay)
      else             → transition CLAIMED→FAILED, JobStore.addToDLQ
  → WorkerRegistry.markIdle(workerId)
```

### Reaper (background thread, runs every 2s)
```
Reaper → JobStore.getClaimedJobs()
  → for each job where leaseExpiresAt < now:
      → transition CLAIMED→PENDING
      → LeaseManager.clearLease(jobId)
      → ExclusivityManager.release(type)
      → JobQueue.reEnqueue(job)
      → WorkerRegistry.markIdle(workerId holding this lease)
```

### RetryScheduler (ScheduledExecutorService)
```
RetryScheduler.schedule(jobId, delayMs)
  → after delay: JobStore.transition(RETRY_SCHEDULED→PENDING), JobQueue.enqueue(job)
```

### Lease Renewal
```
Worker → JobService.renewLease(jobId, leaseToken)
  → LeaseManager.validate(jobId, leaseToken)    // mismatch → StaleLeaseException
  → LeaseManager.extend(jobId, leaseDurationMs)
```

---

## 3. Design Patterns & Extensibility

| Pattern | Where | Why |
|---|---|---|
| Strategy | `RetryPolicy` interface | Swap exponential backoff for fixed/custom per job |
| Strategy | `JobHandler` interface | Each job type = new class, no modification |
| Repository | `JobStore` interface | Swap in-memory for DB without touching service layer |
| Observer (future) | completion events | DAG orchestration, notifications |

**Interfaces and implementations:**
- `JobStore` → `InMemoryJobStore` (persistence boundary: replace with `PostgresJobStore`)
- `RetryPolicy` → `ExponentialBackoffRetryPolicy` (default), overridable per job
- `JobHandler` → user-provided per type (e.g., `SendEmailHandler`)
- `JobQueue` → wraps `PriorityBlockingQueue<Job>` with weighted dispatch logic

**Network transport extensibility:** `JobService` is a plain interface. A `RemoteJobServiceClient` can implement it over HTTP/gRPC without changing any handler or worker code.

---

## 4. Public API

```java
// Submission
String submit(JobSpec spec);

// Worker lifecycle
AcquiredJob acquireJob(String workerId, Set<String> handledTypes);  // blocks until job available
void completeJob(String jobId, String leaseToken, ExecutionResult result) throws StaleLeaseException;
void renewLease(String jobId, String leaseToken) throws StaleLeaseException;

// Worker registration
void registerWorker(String workerId, Set<String> handledTypes);

// Observability
Map<String, Long> pendingCountByType();
Map<Integer, Long> pendingCountByPriority();
long inFlightCount();
long dlqSize();
List<DLQEntry> listDLQ();
WorkerStats getWorkerStats(String workerId);
Map<String, WorkerStats> allWorkerStats();
```

**Exceptions:**
- `StaleLeaseException` — leaseToken mismatch on complete/renew
- `JobNotFoundException` — jobId not found
- `WorkerNotFoundException` — workerId not registered
- `IllegalArgumentException` — null/invalid inputs

---

## 5. Package Structure

```
model/
  Job.java               — mutable entity
  JobSpec.java           — immutable record (submission input)
  JobStatus.java         — enum with valid transitions
  ExecutionResult.java   — enum: SUCCESS, TRANSIENT_FAILURE, PERMANENT_FAILURE
  AcquiredJob.java       — record returned to worker (jobId, payload, leaseToken)
  DLQEntry.java          — immutable record
  WorkerStats.java       — mutable, per-worker counters

service/
  JobService.java        — interface (network-transport boundary)
  JobServiceImpl.java    — orchestrator, constructor-injected deps
  JobStore.java          — interface (persistence boundary)
  InMemoryJobStore.java  — ConcurrentHashMap + DLQ list
  JobQueue.java          — PriorityBlockingQueue wrapper + weighted dispatch
  LeaseManager.java      — issues/validates/renews leaseTokens
  ExclusivityManager.java — per-type mutex (one running at a time)
  WorkerRegistry.java    — worker registration + stats
  Reaper.java            — background thread, lease expiry scanner
  RetryScheduler.java    — ScheduledExecutorService wrapper

strategy/
  RetryPolicy.java       — interface: long computeDelay(int attemptCount)
  ExponentialBackoffRetryPolicy.java — default impl

exception/
  StaleLeaseException.java
  JobNotFoundException.java
  WorkerNotFoundException.java
```

---

## 6. Data Structures & Complexity

| Component | Structure | Complexity | Notes |
|---|---|---|---|
| `JobQueue` | `PriorityBlockingQueue<Job>` | O(log n) enqueue/dequeue | Comparator: priority DESC, then submittedAt ASC (FIFO within same priority) |
| `InMemoryJobStore` | `ConcurrentHashMap<String, Job>` | O(1) get/put | jobId → Job |
| DLQ | `CopyOnWriteArrayList<DLQEntry>` | O(1) add, O(n) list | Low write frequency, reads dominate |
| `LeaseManager` | `ConcurrentHashMap<String, LeaseInfo>` | O(1) | jobId → (leaseToken, expiresAt) |
| `ExclusivityManager` | `ConcurrentHashMap<String, AtomicBoolean>` | O(1) | type → isRunning flag |
| `WorkerRegistry` | `ConcurrentHashMap<String, WorkerStats>` | O(1) | workerId → stats |
| Reaper scan | iterate claimed jobs | O(n) | n = in-flight count, runs every 2s |

**Shared mutable state and locking:**
- `Job` state transitions: `synchronized` on the `Job` instance (per-entity lock, not global)
- `ExclusivityManager.tryLock`: `AtomicBoolean.compareAndSet(false, true)` — lock-free
- `LeaseManager`: `ConcurrentHashMap` + atomic update inside `synchronized(job)`
- `WorkerStats` counters: `AtomicLong` for success/failure counts

---

## 7. Core Logic Snippets

### Weighted random dispatch
```java
// Build candidate list weighted by priority (weight = priority + 1)
List<Job> candidates = queue.drainAvailable(handledTypes, exclusivityManager);
int totalWeight = candidates.stream().mapToInt(j -> j.getSpec().getPriority() + 1).sum();
int rand = ThreadLocalRandom.current().nextInt(totalWeight);
int cumulative = 0;
for (Job job : candidates) {
    cumulative += job.getSpec().getPriority() + 1;
    if (rand < cumulative) return job;
}
```

### Retry delay formula
```java
// base=5s, cap=300s, jitter=0..base
long delay = Math.min(BASE_MS * (1L << attemptCount), MAX_MS)
           + ThreadLocalRandom.current().nextLong(BASE_MS);
```

### Lease token validation
```java
synchronized (job) {
    if (!leaseToken.equals(job.getCurrentLeaseToken()))
        throw new StaleLeaseException(jobId);
    // safe to transition
}
```

### State transition guard (in JobStatus enum)
```java
public void validateTransition(JobStatus next) {
    if (!VALID_TRANSITIONS.get(this).contains(next))
        throw new IllegalStateException(this + " → " + next + " is invalid");
}
```

### Exclusivity tryLock
```java
AtomicBoolean lock = exclusivityLocks.computeIfAbsent(type, k -> new AtomicBoolean(false));
return lock.compareAndSet(false, true); // true = acquired, false = already running
```

---

## 8. Implementation Order

| Step | Component | Time |
|---|---|---|
| 1 | Models + enums + exceptions | 5 min |
| 2 | `InMemoryJobStore` + `JobQueue` | 8 min |
| 3 | `LeaseManager` + `ExclusivityManager` | 5 min |
| 4 | `WorkerRegistry` | 3 min |
| 5 | `RetryPolicy` + `ExponentialBackoffRetryPolicy` | 3 min |
| 6 | `JobServiceImpl` (submit, acquire, complete, renew) | 10 min |
| 7 | `Reaper` + `RetryScheduler` | 5 min |
| 8 | `ObservabilityService` methods | 3 min |
| 9 | Tests | remaining |

---

## 9. State Machine

```java
public enum JobStatus {
    PENDING, CLAIMED, SUCCEEDED, RETRY_SCHEDULED, FAILED;

    private static final Map<JobStatus, Set<JobStatus>> VALID_TRANSITIONS = Map.of(
        PENDING,           Set.of(CLAIMED),
        CLAIMED,           Set.of(SUCCEEDED, FAILED, RETRY_SCHEDULED, PENDING), // PENDING = lease expiry
        RETRY_SCHEDULED,   Set.of(PENDING),
        SUCCEEDED,         Set.of(),
        FAILED,            Set.of()
    );

    public void validateTransition(JobStatus next) {
        if (!VALID_TRANSITIONS.get(this).contains(next))
            throw new IllegalStateException(this + " → " + next + " invalid");
    }
}
```

**Triggers:**
- `PENDING → CLAIMED`: worker acquires lease
- `CLAIMED → SUCCEEDED`: handler returns SUCCESS + valid leaseToken
- `CLAIMED → RETRY_SCHEDULED`: handler returns TRANSIENT_FAILURE, retries remain
- `CLAIMED → FAILED`: handler returns PERMANENT_FAILURE, OR retries exhausted
- `CLAIMED → PENDING`: reaper detects lease expiry
- `RETRY_SCHEDULED → PENDING`: RetryScheduler fires after backoff delay

---

## 10. Edge Cases

- **Null/empty inputs** → `IllegalArgumentException` with message
- **Unknown jobId on complete/renew** → `JobNotFoundException`
- **Stale leaseToken** → `StaleLeaseException` (Worker A finishes after lease expired and Worker B has taken over)
- **Worker A completes after lease expiry but before Worker B picks up** → still rejected; leaseToken was cleared by reaper
- **Duplicate submit of same logical job** → allowed (different jobId each time); idempotency is handler's responsibility using jobId as key
- **PermanentFailure on first attempt** → straight to DLQ, attemptCount=1, maxRetries not checked
- **Exclusive type: second job submitted while first is running** → queued in PriorityBlockingQueue, ExclusivityManager blocks dispatch until first completes/fails
- **Worker crashes mid-execution** → no `completeJob` call → lease expires → reaper returns to PENDING → re-dispatched (at-least-once guarantee)
- **renewLease after lease already expired** → `StaleLeaseException` (leaseToken cleared by reaper)
- **priority=0 job** → weight=1, still gets dispatched (no starvation)
- **maxRetries=0** → first failure goes straight to DLQ

---

## 11. Extensions

| Extension | Design approach | What changes |
|---|---|---|
| Network transport | `JobService` is an interface → `RemoteJobServiceClient` implements it | New class only |
| Persistent storage | `JobStore` is an interface → `PostgresJobStore` implements it | New class only |
| Cron/recurring jobs | `RetryScheduler` uses `ScheduledExecutorService` → add `CronScheduler` using same executor | New class only |
| Job DAGs | Add completion event listener on `JobServiceImpl.completeJob` → DAG orchestrator submits dependents | Add observer hook in `completeJob` |
| Custom retry policy per job | `JobSpec` accepts optional `RetryPolicy` → `JobServiceImpl` uses it if present, else default | `JobSpec` field + null-check in service |
| DLQ requeue | `JobService.requeueFromDLQ(jobId)` → remove from DLQ, reset attemptCount, re-submit | New method on `JobService` + `JobStore` |
| Graceful shutdown | `JobServiceImpl.shutdown()` → stop accepting new jobs, wait for in-flight to complete or timeout | Add shutdown flag + `CountDownLatch` |

---

## 12. What AI Will Likely Miss

- **Reaper must clear `WorkerStats.currentJobId`** when it reclaims a lease — easy to forget
- **ExclusivityManager.release must be called on ALL exit paths** (success, failure, DLQ, lease expiry) — missing one causes permanent type lockout
- **`synchronized(job)` scope** — state transition + leaseToken update must be atomic; AI may split them
- **Weighted dispatch drains candidates from queue** — must re-enqueue non-selected candidates atomically or use a peek-then-remove pattern
- **RetryScheduler delay uses `attemptCount` before increment** — off-by-one on backoff calculation
- **`PermanentFailure` still sets `attemptCount++`** before going to DLQ — spec says retries are skipped but attempt should be recorded
- **Thread visibility** — `leaseExpiresAt` and `currentLeaseToken` on `Job` must be read inside `synchronized(job)` block, not cached

---

## Resolved Ambiguities (from spec §1.2)

| Question | Decision | Justification |
|---|---|---|
| Lease duration | 10s default, overridable per `JobSpec` | 10s is enough for most fast jobs; long-running jobs set their own |
| Lease overrun (Worker A finishes after B re-leased) | Reject Worker A's commit (`StaleLeaseException`) | At-least-once is guaranteed; Worker B will complete it. Accepting A's commit risks double-success |
| Priority semantics | Weighted random (weight = priority+1) | High priority gets proportionally more dispatch slots; priority-0 jobs still get weight=1, no starvation |
| Same-priority ordering | FIFO (submittedAt ASC as tiebreaker in comparator) | Predictable, fair, easy to reason about |
| At-least-once / idempotency | Service guarantees at-least-once; handlers must be idempotent. `jobId` is passed to `handler.run()` as the idempotency key | The service cannot enforce exactly-once without distributed transactions; this is the standard contract (SQS, Celery, Sidekiq all do the same) |
