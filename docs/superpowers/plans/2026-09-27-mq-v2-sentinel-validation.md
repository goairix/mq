# MQ v2 Redis Sentinel Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate ordinary Redis Streams traffic, Redis-backed delayed traffic, and confirmed publishing against disposable Redis Sentinel deployments on Redis 7.2 and 8.0.

**Architecture:** Launch one master, two replicas, and three Sentinel processes in a labeled Docker network. Tests use the caller-supplied `redis.NewFailoverClient` accepted by the existing `redis.UniversalClient` API. Before killing the master, verify Sentinel quorum and that both replicas contain the confirmed record; then wait for election, continue publishing through the same client, and consume or dispatch every expected ID. Run ordinary and delayed scenarios in fresh deployments. Redis replication is asynchronous, so this verifies replicated records, not zero loss for all acknowledged writes.

**Tech Stack:** Go 1.25, go-redis v9, Redis 7.2/8.0, Docker CLI, Bash.

---

### Task 1: Sentinel fixture and topology

**Files:** `tests/cluster/sentinel.sh`

- [x] Create a labeled network and three Redis data containers on host ports 7101–7103. Start node 1 as master and nodes 2–3 as replicas, with AOF `appendfsync always` and node-specific ports that the Go test Dialer can remap to localhost.
- [x] Start three Sentinel containers on ports 27101–27103, each monitoring the same named master with quorum 2. The fixture waits for master agreement and `SENTINEL CKQUORUM`; the Go tests additionally require both replicas and two peer Sentinels.
- [x] Pass owned container run ID, Sentinel addresses, and master name to a selected Go test. In the EXIT trap remove only containers/network bearing this run's label; remove anonymous volumes with `docker rm -fv`.

### Task 2: Ordinary and delayed failover

**Files:** `adapters/redis/sentinel_test.go`, `delay/redis/sentinel_test.go`

- [x] Add opt-in tests that construct `redis.NewFailoverClient` with a test-only Dialer mapping advertised Docker ports to host loopback. Reject an incomplete Sentinel topology before fault injection.
- [x] Publish a uniquely identified record and wait until both replicas can read it. Verify ownership label and kill the data master, wait for Sentinel election and the same FailoverClient to connect to the promoted master, then confirm a second publish.
- [x] Consume both ordinary message IDs or dispatch both delayed message IDs, allowing duplicates and supervising transient worker errors within a bounded timeout. Run the negative topology test against one Sentinel and a standalone Redis master; it must fail before Docker kill.

### Task 3: Benchmark and integration

**Files:** `adapters/redis/publish_bench_test.go`, `tests/cluster/run.sh`, `.github/workflows/v2-cluster.yml`, `tests/cluster/README.md`, `tests/cluster/results-2026-09-27.md`, `adapters/redis/README.md`

- [x] Extend the existing Redis benchmark client selection to accept Sentinel addresses/master via `NewFailoverClient`, preserving standalone and Cluster modes.
- [x] Add `sentinel [7.2|8.0]` and `bench sentinel [7.2|8.0]` selectors; include both versions in `all` and manual CI.
- [x] Run each failover scenario and one short benchmark on 7.2 and 8.0; record only measured results and the asynchronous-replication limit. Re-run existing Redis Cluster scenarios and the race suite.
- [x] Check syntax, formatting, root dependency isolation, labeled resource cleanup, and the final diff. Commit on the existing `v2` branch without pushing or tagging.
