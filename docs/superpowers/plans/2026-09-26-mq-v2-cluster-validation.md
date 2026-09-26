# MQ v2 Cluster Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add repeatable, opt-in failover tests for confirmed ordinary traffic and delayed traffic against real Kafka, RabbitMQ, and Redis clusters.

**Architecture:** A Docker-only runner creates one disposable broker cluster at a time and passes its addresses and owned container names to Go integration tests in the corresponding optional modules. Tests check cluster topology before fault injection, publish confirmed messages, stop a data-owning node, then verify recovery through surviving nodes. Redis tests wait for replica observation before stopping a primary and explicitly make no zero-loss claim for asynchronous failover.

**Tech Stack:** Go 1.25 tests, Docker CLI, Apache Kafka 4.0.2/4.1.2, RabbitMQ 3.13.3, Redis 7.2/8.0.

---

### Task 1: Kafka leader failover

**Files:** `adapters/kafka/cluster_test.go`, `tests/cluster/kafka.sh`, `tests/cluster/README.md`

- [x] Write an opt-in `TestClusterLeaderFailover` that requires three seed brokers and a disposable container prefix. Create a unique one-partition topic with replication factor 3 and `min.insync.replicas=2`; verify three ISR before injecting a fault.
- [x] Publish identified messages with the adapter. Stop the current partition leader container using its broker ID. Reuse the adapter to publish more messages and consume all expected IDs through the surviving brokers. A duplicate ID may be delivered; a missing ID fails.
- [x] Observe the test fail against a single-node broker because it cannot create the required topology.
- [x] Implement the three-node KRaft Docker setup, readiness checks, bounded cleanup and test command. Run the cluster test on both 4.0.2 and 4.1.2, then run all module suites with the race detector and broker opt-in tests disabled.

### Task 2: RabbitMQ quorum leader failover

**Files:** `adapters/rabbitmq/cluster_test.go`, `delay/rabbitmq/cluster_test.go`, `tests/cluster/rabbitmq.sh`, `tests/cluster/README.md`

- [x] Write opt-in ordinary and delayed-message tests. Use management API to require a three-member quorum queue and identify its leader. Publish confirmed messages, stop that leader's disposable container, reconnect to a surviving node and verify delivery. Require another confirmed publish after failover.
- [x] Observe the tests reject a single-node quorum topology.
- [x] Implement the three-node RabbitMQ Docker cluster with shared Erlang cookie, stream_queue feature flag, readiness checks and bounded cleanup. Run both tests against 3.13.3.

### Task 3: Redis Cluster primary failover

**Files:** `adapters/redis/cluster_test.go`, `delay/redis/cluster_test.go`, `tests/cluster/redis.sh`, `tests/cluster/README.md`

- [x] Write opt-in ordinary and delayed-message tests against `redis.ClusterClient`. Find the key's slot owner and replica, wait until the replica can read the confirmed record, stop the owner, wait for promotion and verify consumption or delayed publication. After promotion require a new confirmed write.
- [x] Observe the tests reject a standalone Redis instance.
- [x] Implement a disposable three-primary, three-replica Redis Cluster with AOF `appendfsync always`, Docker-network addresses mapped by the test Dialer to host ports, readiness checks and bounded cleanup. Run both tests against Redis 7.2 and 8.0.

### Task 4: Integration and release evidence

**Files:** `tests/cluster/run.sh`, `tests/cluster/README.md`, `.github/workflows/v2-cluster.yml`, root `README.md`

- [x] Add a selector to run one backend/version at a time and a manual CI workflow that invokes the same runner.
- [x] Document topology, exact commands, fault boundaries, duplicate semantics and Redis asynchronous-replication limitation.
- [x] Run `gofmt`, relevant `go test -race`, `bash -n`, every cluster scenario, and dependency isolation checks. Record failures honestly and fix only verified defects.
- [x] Review the diff, ensure disposable resources were removed, and commit the completed test harness on `v2`.

### Task 5: Optional healthy-cluster publish benchmark

**Files:** `adapters/redis/publish_bench_test.go`, `tests/cluster/*.sh`, `tests/cluster/README.md`, `.github/workflows/v2-cluster.yml`

- [x] Reuse the existing confirmed-batch adapter versus direct-client benchmarks on a healthy Kafka and RabbitMQ cluster, with Kafka topic RF3 and `min.insync.replicas=2`.
- [x] Extend the Redis benchmark to accept a `ClusterClient` through the same address mapping as the failover tests, preserving the current standalone benchmark path.
- [x] Add an opt-in `bench` runner and manual CI selector; document that the numbers measure publish confirmation only, not end-to-end consumption or failover under load.
- [x] Run one sample on each backend, check output, and ensure Docker resources are removed.
