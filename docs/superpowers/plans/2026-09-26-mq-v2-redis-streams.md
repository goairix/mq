# MQ v2 Redis Streams Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a separate production Redis Streams module with confirmed publication, at-least-once consumption, pending recovery, dead letters, bounded batch delivery, and portable contract coverage.

**Architecture:** The Redis child module accepts a caller-owned `redis.UniversalClient`; root interfaces remain unaware of the client. It stores versioned message envelopes as Stream fields, uses consumer groups and manual `XACK`, and recovers abandoned entries with `XAUTOCLAIM`. Delay scheduling is a separate plan so normal Redis traffic never executes scheduling work.

**Tech Stack:** Go 1.23, `github.com/redis/go-redis/v9` v9.17.0, Redis 7.2/8.0 for integration (Redis 6.2 command floor for local smoke tests), Docker.

**Spec:** [MQ v2 architecture](../specs/2026-09-25-mq-v2-architecture-design.md)

## Global Constraints

- Redis module path is `github.com/goairix/mq/adapters/redis/v2`; root `go.mod` remains standard-library-only.
- Do not use `XADD MAXLEN` or `XTRIM`; a stream entry may be pending in another subscription group.
- A successful `Publish` follows Redis's `XADD` confirmation; transport errors after dispatch are classified as outcome unknown.
- A handler is `XACK`ed only after success or a separately confirmed dead-letter `XADD`.
- Redis Sentinel/Cluster asynchronous failover cannot guarantee zero lost acknowledged writes; document this limit.
- Batch result length mismatch is an error and leaves records pending.
- The adapter owns no caller Redis client lifecycle; `Close` stops new operations and waits for synchronous in-flight calls.

## Review Focus

1. Binary key/payload and Unicode headers must round-trip without string corruption.
2. A handler failure must remain pending and be claimed by a different consumer after idle expiry.
3. A dead-letter `XADD` failure must leave the source pending; the source is acknowledged only after DLQ success.
4. A partial batch must acknowledge successes while retaining failures, including a later success after an earlier failure.
5. Cancellation during blocked reads must end promptly without acknowledging an in-flight failed handler.

---

## Scope and files

| File | Responsibility |
| --- | --- |
| `contracttest/contract.go` | Strengthen invalid-result and `(nil,nil)` batch checks from Memory review. |
| `adapters/redis/go.mod`, `go.work` | Independent module and workspace graph. |
| `adapters/redis/config.go` | Client interface, options, stream/DLQ key mapping, constructor validation. |
| `adapters/redis/wire.go` | Stable `mq.v` wire version and byte-safe envelope fields. |
| `adapters/redis/publish.go` | Synchronous XADD and pipelined batch publication; close state. |
| `adapters/redis/consume.go` | Group creation, XREADGROUP, XAUTOCLAIM, retry and dead-letter ordering. |
| `adapters/redis/batch.go` | Bounded batch handler outcomes and grouped ACKs. |
| `adapters/redis/*_test.go` | Codec tests, integration tests, fault injection, portable suite. |
| `adapters/redis/README.md` | Minimal use, topology/reliability and tuning boundaries. |

Work on long-lived `v2`, no push/tag. Use `GOWORK=off` for root isolation. `go-redis` v9.17.0 declares Go 1.18 and is compatible with this module's Go 1.23 baseline; the latest v9 requires Go 1.24 and would violate the approved Redis baseline. The adapter must never depend on `go-redis` from root.

### Task 1: Portable contract corrections and module skeleton

**Interfaces:** `contracttest.Run(t, Factory)` stays unchanged; Redis module exports `New(redis.UniversalClient, Options) (*Adapter,error)`.

- [ ] **Step 1:** Add a contracttest fixture that returns `context.DeadlineExceeded` without invoking a batch handler; verify the invalid-result subtest rejects it. Add a `(nil,nil)` batch success subtest and verify a fixture that does not acknowledge it fails. Run root tests and observe RED.
- [ ] **Step 2:** Change the invalid-result subtest to record handler invocation and reject `context.Canceled`/`context.DeadlineExceeded` as proof of validation. Add a separate all-success subtest using `(nil,nil)` with two messages. Run root race tests GREEN.
- [ ] **Step 3:** Add `adapters/redis/go.mod` requiring root v2.0.0 and go-redis v9.17.0; add `./adapters/redis` to `go.work` and a version-specific workspace replacement only for the unpublished root v2.0.0. Create `config.go` with validated `Options{Prefix,Consumer,StartLatest,ReadCount,Block,ClaimIdle,RetryMin,RetryMax}`; defaults must be explicit, `ClaimIdle>0`, `RetryMin>0`, `RetryMax>=RetryMin`, `ReadCount>0`, `Block>0`. `New` rejects nil client and invalid options. `Close(ctx)` marks closed, does not close caller client.
- [ ] **Step 4:** Test constructor failures and verify `GOWORK=$PWD/go.work go list -m all` includes Redis client only via Redis module; `GOWORK=off go list -m all` remains root-only.
- [ ] **Step 5:** Commit as `feat: scaffold isolated Redis Streams adapter`.

### Task 2: Versioned wire format and confirmed publication

**Interfaces:** `encode(mq.Message) map[string]any`, `decode(redis.XMessage) (mq.Message,error)`, `(*Adapter).Publish`, `PublishBatch`.

- [ ] **Step 1:** Write codec tests for binary payload/key, empty payload, Unicode header, malformed version, missing ID, and invalid timestamp. Write Redis integration tests for `Publish` XADD confirmation and ordered per-item `PublishBatch` results, including an invalid middle message. Observe RED from undefined methods.
- [ ] **Step 2:** Encode fields `v=1`, `id`, `topic`, `key`, `payload`, `created_ns`, and `headers` (JSON only for the header map); decode and `Message.Validate`. `Publish` validates before XADD, stores no MAXLEN/TTL, maps Redis transport failures to `mq.OutcomeUnknown`, and uses a fixed prefix plus escaped topic. `PublishBatch` uses one go-redis pipeline per configured chunk, returns exactly one `PublishResult` per input in order, and does not treat the entire batch as atomic.
- [ ] **Step 3:** Verify tests against a local Redis 7.2 container; run codec race tests and `go vet` for child. Stop the container after tests.
- [ ] **Step 4:** Commit as `feat: publish versioned messages to Redis Streams`.

### Task 3: Group consumption, pending recovery, and dead letters

**Interfaces:** `(*Adapter).Prepare(ctx, mq.Subscription) error`, `Run(ctx,sub,handler) error`, and `(*Adapter).DeadLetterStream(sub) string`.

- [ ] **Step 1:** Add integration tests for two groups receiving the same record, two workers in one group competing, a transient failure remaining pending, and a second consumer reclaiming it after `ClaimIdle`. Add a failure-injected Redis client test proving DLQ XADD failure leaves source pending. Observe RED.
- [ ] **Step 2:** `Prepare` performs `XGROUP CREATE ... 0 MKSTREAM` by default and `... $` only when `StartLatest` is explicitly set; ignore only Redis `BUSYGROUP`. `Run` calls `Prepare`, periodically attempts `XAUTOCLAIM` with a stable consumer name, then `XREADGROUP` with `>` and bounded `COUNT/BLOCK`. Call handlers outside internal locks. `XACK` successful messages; transient errors retain pending and wait capped exponential backoff; permanent errors `XADD` a dead-letter stream with original ID, headers, source stream/entry, and error category, wait for confirmation, then `XACK`. A DLQ error returns from `Run` with source still pending.
- [ ] **Step 3:** Run portable `contracttest.Run` against Redis 7.2 using `Prepare`; test cancellation, restart recovery, and injected XACK failure causing explainable duplicate delivery. Verify no silent loss.
- [ ] **Step 4:** Commit as `feat: consume Redis Streams with pending recovery`.

### Task 4: Bounded batch consumption, docs, and module matrix

**Interfaces:** `(*Adapter).RunBatch(ctx,sub,options,handler) error`.

- [ ] **Step 1:** Add integration tests for `(nil,nil)` all-success, whole-batch transient failure, per-item partial success, permanent dead letter, invalid result length, `MaxBytes`/`MaxInFlightBytes`, and `MaxWait` sparse arrivals. Observe RED.
- [ ] **Step 2:** Read up to `MaxMessages` and logical `MaxBytes`, allowing one record over soft MaxBytes and returning an explicit error over hard MaxInFlightBytes. Coalesce until `MaxWait` or a bound. Apply handler outcomes per item: pipeline successful `XACK`s, confirm each DLQ XADD before its source ACK, leave transient failures pending, and return an error for invalid result length without ACK. Never launch one goroutine per message.
- [ ] **Step 3:** Add README with basic publisher/subscriber example, caller-owned client lifetime, pre-created subscription topology, AOF/replication reliability limits, no implicit stream truncation, and tuning guidance for accumulated pending entries.
- [ ] **Step 4:** Run Redis 7.2 and 8.0 integration suites, child race tests, root race tests, isolated root graph, and `git diff --check`.
- [ ] **Step 5:** Commit as `feat: add bounded Redis batch consumer`.

## Exit checks

- Redis module passes portable tests and fault-injection tests without adding go-redis to root `go.mod`.
- Redis 7.2 and 8.0 run the same integration suite; tests skip only when explicitly configured without Docker.
- Publication, failure, DLQ, pending recovery, and batch behavior match the approved at-least-once contract.
- Durable Redis delay is still a separate work package; no claim of complete Redis delay support yet.
