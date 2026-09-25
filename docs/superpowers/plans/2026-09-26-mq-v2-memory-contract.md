# MQ v2 Memory and Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish executable transport semantics with a standard-library contract suite and an isolated, nonpersistent Memory adapter.

**Architecture:** The root module owns deterministic message sizing and adapter-agnostic contract tests. `adapters/memory` is a separate Go module whose broker stores bounded topic logs, independent subscription cursors, retry state, and dead letters. The Memory broker is only a test replacement, never a durability claim.

**Tech Stack:** Go 1.23 standard library for root and Memory modules; Go 1.25 workspace toolchain.

**Spec:** [MQ v2 architecture](../specs/2026-09-25-mq-v2-architecture-design.md)

## Global Constraints

- Keep `github.com/goairix/mq/v2` free of third-party requirements and imports from adapter modules.
- Memory uses module path `github.com/goairix/mq/adapters/memory/v2` and declares `go 1.23.0`.
- A successful handler is acknowledged only after it returns; transient failures are retried, permanent failures enter a dead-letter collection before advancing.
- Different subscription names receive independent copies; equal names compete without concurrent delivery of one record.
- All queues and in-flight counts are bounded; no message is silently dropped when the limit is reached.
- Memory scheduled messages are test-only and disappear on process restart.

## Review Focus

1. A caller mutating the payload or headers after `Publish` must not alter stored data; test a defensive copy.
2. A handler failure followed by cancellation must leave the message available for a later `Run`; test redelivery.
3. Two groups on the same topic must each receive a message, while two workers in one group must handle it once; test fanout and competition.
4. Batch partial success with an earlier failure must not repeat later successful records; test per-index completion and contiguous advancement.
5. An oversized record must not hang a batch subscriber forever; test standalone delivery when within `MaxInFlightBytes`, and an explicit error above it.

---

## Scope and files

| File | Responsibility |
| --- | --- |
| `message.go`, `message_test.go` | `SizeBytes` for consistent logical byte accounting. |
| `contract.go`, `contract_test.go` | Document batch limits and test every publish-result state. |
| `contracttest/contract.go` | Root-module reusable test suite accepting a fresh adapter factory. |
| `adapters/memory/go.mod`, `go.work` | Independent module and local workspace resolution. |
| `adapters/memory/broker.go` | Bounded topic logs, copy-on-publish, subscription state, retry and dead letters. |
| `adapters/memory/batch.go` | Batched delivery and partial-result accounting. |
| `adapters/memory/schedule.go` | Test-only scheduled publishing with cancel-safe timer ownership. |
| `adapters/memory/broker_test.go` | Contract suite and Memory-specific boundary cases. |

Use the existing `v2` branch as requested. Create no release tag or push. Verify root isolation with `GOWORK=off`; verify Memory with `GOWORK=$PWD/go.work`.

### Task 1: Freeze batch byte semantics

**Interfaces:** Produce `Message.SizeBytes() int` and unchanged public transport interfaces.

- [ ] **Step 1:** Extend `message_test.go` with `TestMessageSizeBytes`: for ID `a`, topic `b`, key `[]byte{1,2}`, payload `[]byte{3,4,5}`, header `x:y`, and a timestamp, expect `1+1+2+3+1+1+8 == 17` bytes. Extend `contract_test.go` table cases to reject an unknown `PublishState`, an accepted result with an error, and rejected/unknown results without an error; accept rejected/unknown results with an error.
- [ ] **Step 2:** Run `GOWORK=off go test ./... -run 'Test(MessageSizeBytes|BatchResultValidation)$' -count=1` and observe RED because `SizeBytes` is absent and the added assertions are now compiled.
- [ ] **Step 3:** Add `SizeBytes` in `message.go` by summing `len(ID)+len(Topic)+len(Key)+len(Payload)+8` plus lengths of all header keys and values. Document in `BatchOptions` that `MaxBytes` uses this size, `MaxWait=0` sends available messages immediately, a single message over `MaxBytes` is delivered alone, and a message over `MaxInFlightBytes` returns a subscriber error before handler invocation. Document that `MaxInFlightBatches` and `MaxInFlightBytes` are hard concurrency bounds.
- [ ] **Step 4:** Run `gofmt -w message.go message_test.go contract.go contract_test.go`, `GOWORK=off go test ./... -count=1`, and `GOWORK=off go vet ./...`.
- [ ] **Step 5:** Commit as `docs: fix v2 batch sizing and result semantics`.

### Task 2: Independent Memory module and single-message semantics

**Interfaces:** Consume `mq.Message`, `mq.Subscription`, `mq.Handler`; produce `memory.New(capacity int) (*Broker,error)`, `(*Broker).Publish`, `Run`, `Close`, `DeadLetters`.

- [ ] **Step 1:** Add `adapters/memory/go.mod` with module path above, `go 1.23.0`, and `require github.com/goairix/mq/v2 v2.0.0`; add repository `go.work` with `go 1.25.0` and `use ( . ./adapters/memory )`. This workspace is local development wiring; published child module has no `replace` directive.
- [ ] **Step 2:** Add `broker_test.go` with table-driven tests for invalid capacity, publish validation, payload/header copying, two subscription names, equal-name worker competition, retry after first handler error, permanent-error dead letter, capacity backpressure, and close. Run `GOWORK=$PWD/go.work go test ./adapters/memory/... -count=1` and observe RED from undefined `New`/`Broker`.
- [ ] **Step 3:** Implement `Broker` with one mutex, topic logs, per-topic/name cursors, one active delivery per group, notification channel, and max retained-record capacity. `Publish` validates then deep-copies message data under the lock, appends or returns `mq.ErrBackpressure`, and wakes waiters. `Run` registers a group at the earliest retained record, claims a record, invokes handler outside the lock, advances only on success or after appending a permanent failure to dead letters, and on transient error releases the claim and retries with capped exponential backoff. `Close` prevents future publish; `Run` returns on context cancellation. Reclaim log records only after every existing group passes them. Provide `DeadLetters` as copied snapshots for tests.
- [ ] **Step 4:** Run `gofmt -w adapters/memory/*.go`, `GOWORK=$PWD/go.work go test ./adapters/memory/... -count=1`, `GOWORK=$PWD/go.work go test -race ./adapters/memory/...`, and `GOWORK=off go list -m all`; last output must contain only root module.
- [ ] **Step 5:** Commit as `feat: add isolated memory transport`.

### Task 3: Batch delivery and shared contract suite

**Interfaces:** Consume `mq.BatchOptions`, `mq.BatchHandler`, `mq.PublishResult`; produce `(*Broker).PublishBatch`, `RunBatch`, and `contracttest.Run(t, Factory)`.

- [ ] **Step 1:** Write `contracttest/contract.go` with a `Factory` taking `*testing.T` and returning a fresh object satisfying `mq.Publisher`, `mq.Subscriber`, and `mq.BatchSubscriber`. Its subtests must prove publish confirmation, two-name fanout, transient redelivery, permanent-message isolation, batch success, batch partial results, invalid batch result length, cancellation, and independent capacity behavior. Use bounded timeouts and unique topic names; assert only portable guarantees.
- [ ] **Step 2:** Invoke `contracttest.Run` from `broker_test.go` and add Memory-specific tests for `PublishBatch` per-item statuses, `MaxBytes` standalone oversize, `MaxInFlightBytes` failure, and later-success preservation after an earlier failed batch member. Run Memory tests and observe RED from missing methods.
- [ ] **Step 3:** Implement `PublishBatch` by calling `Publish` per item in order and mapping errors to accepted/rejected/unknown. Implement `RunBatch` with bounded selection from one group, `SizeBytes` accounting, one active batch per group, and per-record completion flags; advance the cursor only through a contiguous completed prefix. For handler-level error, retry all; for invalid result length, preserve all records and return an error; for permanent errors, append dead letters before completion. Respect context cancellation and release claimed records.
- [ ] **Step 4:** Run `GOWORK=$PWD/go.work go test -race ./adapters/memory/... -count=1`, `GOWORK=off go test -race ./... -count=1`, `GOWORK=off go vet ./...`, and `git diff --check`.
- [ ] **Step 5:** Commit as `feat: add batch memory contract tests`.

### Task 4: Test-only scheduled publication and usage docs

**Interfaces:** Produce `(*Broker).PublishAt(context.Context, mq.Message, time.Time) error` with same-process, nonpersistent semantics.

- [ ] **Step 1:** Add tests that publishing in the past delivers promptly, a future due time does not deliver early, a closed broker rejects scheduling, and canceling a caller after a successful schedule does not discard it. Run Memory tests and observe RED because `PublishAt` is undefined.
- [ ] **Step 2:** Add `schedule.go` with one broker-owned scheduler goroutine and a min-heap of due records, bounded by the broker capacity. Store a defensive message copy at schedule time; wake the scheduler for earlier deadlines; on due, append to the topic log atomically under broker lock; stop it on `Close`. A scheduled record counts toward capacity until it is delivered or closed. Document that Close/restart discards pending schedule records.
- [ ] **Step 3:** Update README files with a test-only Memory example, `go.work` instructions, and correct historical tag wording (`v0.x`, not `v1`).
- [ ] **Step 4:** Run `GOWORK=$PWD/go.work go test -race ./adapters/memory/... -count=1`, `GOWORK=off go test -race ./... -count=1`, `GOWORK=off go list -m all`, and `git diff --check`.
- [ ] **Step 5:** Commit as `feat: add test-only memory scheduling`.

## Exit checks

- Memory passes its own test suite and the portable contract suite under the race detector.
- Root `go.mod` remains dependency-free; `go.work` resolves the two local modules.
- Memory docs say explicitly that scheduling and queued messages do not survive process restart.
- No production broker adapter or durable-delay claim is made by this plan.
