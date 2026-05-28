# Background Job Service — MC3 AI-Assisted

**Round:** MC3 (AI-assisted)
**Date:** 2026-05-28
**Time budget:** 60 min

## Problem Statement

Build an in-process Background Job Service. Producers submit jobs; workers pull jobs and execute them. The service guarantees at-least-once execution, retries failed jobs with backoff, surfaces stuck jobs, and exposes observability into queue health.

### Core Functional Requirements

1. `submit(job_spec) -> job_id` — spec contains `type`, `payload`, `priority` (0-9, default 5), `max_retries` (default 3)
2. Workers register with a list of types they handle. Jobs are acquired via lease (visibility timeout). Lease must be renewed for long-running jobs; expiry returns job to pending.
3. State machine: `pending → claimed → succeeded | failed(DLQ) | retry_scheduled → pending`
4. Retry with exponential backoff + jitter. After `max_retries` exhausted → DLQ.
5. `JobHandler` interface: `run(payload) → Success | TransientFailure | PermanentFailure`. `PermanentFailure` → DLQ immediately.
6. Observability: pending count by type/priority, in-flight count, DLQ size + list, per-worker stats.
7. Exclusive jobs: at most one job of a given `type` runs concurrently; others queue behind it.
