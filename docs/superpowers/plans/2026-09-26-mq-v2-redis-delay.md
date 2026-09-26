# MQ v2 Redis Durable Delay Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task by task in the existing user-designated `v2` branch. This plan is approved by the architecture spec; no new product decision is needed.

**Goal:** Provide durable delayed publication as a separate Go module usable with Redis Streams or Kafka publishers, without adding Redis work or dependencies to normal publish paths.

**Architecture:** `delay/redis/v2` stores versioned message envelopes in a Redis hash and due times in a sorted set. An atomic Lua script inserts both. Workers atomically move due entries into a leased sorted set, publish through any core `mq.Publisher`, then atomically remove a task only if their lease token still matches. Expired leases are reclaimed. A crash after target confirmation and before task completion may redeliver, matching at-least-once semantics.

**Tech stack:** Go 1.23, `go-redis/v9` v9.17.0, Redis 7.2 and 8.0 with AOF appendfsync always in integration tests. Root remains standard-library-only. All scheduling keys for a lane share one Redis Cluster hash tag. Shard count is fixed for each deployed scheduler prefix and is documented as immutable while tasks remain.

**Spec:** [MQ v2 architecture](../specs/2026-09-25-mq-v2-architecture-design.md)

## Guarantees and limits

- `PublishAt` returns nil only after Redis confirms the atomic durable insert. A post-dispatch transport error is `mq.OutcomeUnknown`; retry with identical message ID and due time. A changed payload or due time for a live ID returns a conflict error.
- `Run` owns the scheduler loop and returns errors. The adapter owns neither Redis client nor target publisher. Target publication is confirmed before task deletion. Unknown target outcomes remain scheduled for retry after lease expiry.
- Worker cancellation stops new claims. A task already publishing may finish within a configurable drain deadline; expired work remains leased until recovery. No per-message goroutine is used.
- At-least-once, possibly late and duplicate delivery; no exactly-once or cross-backend transaction claim. Redis Sentinel/Cluster asynchronous failover is not zero-loss. Reliable deployment uses AOF with `appendfsync always`.
- Publish/claim/complete scripts use only explicitly provided keys and one hash slot per lane. Claims fetch at most one task to avoid a lease expiring while a worker processes an earlier claimed task.

## Task 1: Isolated module, codec, and atomic schedule insert

- [ ] Add `delay/redis/go.mod`, workspace entry, module README scaffold; test `GOWORK=off go list -m all` remains root only.
- [ ] RED tests for options validation, binary/key/header/time round-trip, duplicate identical insertion, conflicting duplicate, and failed Redis insert outcome classification.
- [ ] Implement `New(client redis.UniversalClient, target mq.Publisher, Options)`, `PublishAt`, versioned codec, and atomic insert script. Validate due time, message, and configured prefix/shard count. Choose lane deterministically from topic and ID; keep all lane keys in one hash slot.
- [ ] GREEN unit and Redis 7.2 integration tests; commit.

## Task 2: Atomic claim, lease recovery, target publication, completion

- [ ] RED integration tests for due-not-before behavior, successful target confirmation then task removal, transient/unknown target failure leaving a task recoverable, two workers not publishing the same unexpired lease, expired lease recovery, and stale token unable to delete a newer claim.
- [ ] Implement bounded one-task claim/reclaim script, completion script with lease-token check, worker polling and round-robin lane scanning. Use Redis server time for due/lease comparison. Surface errors from Redis and target; retain tasks on every failure path.
- [ ] GREEN Redis 7.2 integration tests and race tests; commit.

## Task 3: Cancellation, restart, fault injection, and operations

- [ ] RED tests for cancel while idle, cancel while target publication is in progress with drain success/expiry, worker restart after Redis lease expiry, and crash-window duplicate after target confirmation before completion.
- [ ] Implement configurable drain deadline and `Close(ctx)` for scheduling operations while preserving caller-owned clients. Document AOF settings, shard/prefix stability, lag metrics, capacity, idempotency, and Kafka optional wiring. Keep scheduler out of Redis and Kafka normal modules.
- [ ] GREEN Redis 7.2/8.0 integration and race suites; root and Memory tests; isolated root dependency graph; commit.

## Task 4: Review and release evidence

- [ ] Run a fresh read-only review against this plan and spec; fix Critical/Important findings with focused regressions.
- [ ] Record Redis version matrix and failures in `.superpowers/sdd/2026-09-26-mq-v2-redis-delay/progress.md`.
- [ ] Keep normal Redis `Publish` and future Kafka `Publish` free of scheduler calls. Benchmark scheduling separately; normal adapter throughput comparison remains a v2 release gate.

## Exit checks

- A confirmed scheduled task survives scheduler process restart under configured Redis persistence and eventually reaches a confirmed target publisher.
- Crash and timeout windows yield explainable duplicates, never silent task deletion before target confirmation.
- Root module still imports no Redis package, and applications add `delay/redis/v2` only when they need scheduling.
