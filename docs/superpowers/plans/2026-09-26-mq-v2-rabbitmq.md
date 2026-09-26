# MQ v2 RabbitMQ 3.13 Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task by task on the existing user-designated `v2` branch. Apply TDD and perform one fresh review after the normal adapter and one after the delay module.

**Goal:** Deliver a separate RabbitMQ module with confirmed, routable ordinary publication, quorum consumer groups, explicit ACK and dead-letter ordering, bounded batch operation, and reliable delay on RabbitMQ 3.13.3 without a plugin.

**Architecture:** The normal module `adapters/rabbitmq/v2` uses caller-owned `amqp091-go` connections, a bounded pool of long-lived confirm-mode publishing channels, a durable topic exchange, and one durable quorum queue per subscription. `Prepare` creates/binds topology before publication. The optional `delay/rabbitmq/v2` module uses a fixed set of quorum TTL bucket queues, each configured for at-least-once dead-lettering into one durable release queue. A worker reads release messages, republishes to the appropriate bucket if still early, or to the target `mq.Publisher` when due; it confirms the next hop before ACKing release. Ordinary publication never enters the delay path.

**Tech stack:** Go 1.23, `github.com/rabbitmq/amqp091-go` v1.15.0 (module Go 1.20), RabbitMQ `3.13.3-management` integration image. Root module remains standard-library-only.

**Spec:** [MQ v2 architecture](../specs/2026-09-25-mq-v2-architecture-design.md)

## Confirmed design constraints

- Quorum queues in RabbitMQ 3.13 support queue TTL and per-message TTL. Expired messages reach the DLX only at the queue head; fixed queue TTL buckets avoid arbitrary short-TTL messages sitting behind longer-TTL messages in the same queue. The release worker checks the absolute due time before final delivery.
- At-least-once dead lettering requires `dead-letter-strategy=at-least-once`, `overflow=reject-publish`, a DLX, and `stream_queue` feature flag. The RabbitMQ 3.13.3 probe accepted `x-dead-letter-strategy` and other required queue arguments; with a missing DLX, an expired message remained in the source quorum queue (`messages_dlx=1`) instead of disappearing. Integration must cover transfer after the target becomes routable. [RabbitMQ 3.13 quorum queues](https://www.rabbitmq.com/docs/3.13/quorum-queues), [TTL behavior](https://www.rabbitmq.com/docs/3.13/ttl).
- A successful ordinary `Publish` requires a publisher confirm and no `basic.return` from a mandatory publish. Confirm timeout or channel loss yields `mq.OutcomeUnknown`. NACK and unroutable return are explicit failures. A channel with an uncertain outstanding publish is retired before reuse.
- The caller owns the AMQP connection and reconstructs the adapter after connection failure; `Run` returns the failure. Channel pooling, bounded prefetch, and batching prevent this library from adding a per-message connection or goroutine.
- A single-node RabbitMQ deployment can recover persisted data after process restart, but cannot remain available when its only node is down. Cluster HA requires a quorum of RabbitMQ nodes and matching connection endpoint management.

## Task 1: Module, topology, and wire format

- [ ] Add `adapters/rabbitmq/go.mod`, workspace entry, constructor/options validation, and close state. Root graph stays isolated.
- [ ] RED tests for durable topic exchange + quorum queue binding, fanout across subscription names, stable queue names, binary key/payload, headers, original ID/time, and unknown wire version.
- [ ] Implement caller-owned connection constructor and `Prepare`; configure durable topic exchange, durable quorum source queue, durable quorum DLQ, and bindings. Use versioned AMQP headers for envelope metadata. Explicitly reject conflicting topology.
- [ ] GREEN RabbitMQ 3.13.3 integration; commit.

## Task 2: Confirmed publishing

- [ ] RED tests for successful publish, missing route `basic.return`, invalid message rejected before dispatch, ordered batch results with one invalid item, confirm timeout/connection failure outcome unknown, and bounded channel reuse.
- [ ] Implement confirm channel pool with `mandatory=true` and persistent messages. One outstanding ordinary `Publish` per pooled channel; `PublishBatch` pipelines bounded chunks with correlated returns and deferred confirms. Track active operations so `Close(ctx)` drains or reports deadline. No per-message channel creation.
- [ ] GREEN RabbitMQ 3.13.3 tests and race tests; commit.

## Task 3: Manual ACK consumption and dead letters

- [ ] RED portable tests plus Rabbit-specific tests for same-group competition, separate-group fanout, transient retry, permanent failure to confirmed DLQ before source ACK, DLQ publish failure leaving source unacked, broker restart redelivery, cancel and drain completion/expiry.
- [ ] Implement `Run` with per-consumer channel, finite QoS prefetch, manual ACK, capped retry delay, and confirmed explicit DLQ publish. Stop reading on cancellation, allow an in-flight handler to finish within configurable `DrainTimeout`, then ACK only completed work.
- [ ] GREEN integration/race tests; commit.

## Task 4: Bounded batch, performance, and review

- [ ] RED tests for `(nil,nil)` success, whole-batch retry, partial results, invalid result length, `MaxMessages`, `MaxBytes`, `MaxWait`, and hard `MaxInFlightBytes`. Observe the portable Outstanding probe through AMQP queue inspection.
- [ ] Implement one bounded batch at a time with manual per-message ACK and confirmed dead-letter publication. Keep source prefetch within the configured in-flight bound; account for one unavoidable oversize delivery before rejecting it.
- [ ] Run RabbitMQ 3.13.3 restart/fault suite, direct-client throughput and allocation comparison, root/child race tests, isolated module graph. Fresh read-only review and Critical/Important fixes; commit.

## Task 5: Optional RabbitMQ delay module

- [ ] Create a separate implementation plan for `delay/rabbitmq/v2` after normal adapter API is stable. It must be independent of normal publish hot path and use fixed quorum TTL buckets plus one release queue. Every bucket must declare `x-queue-type=quorum`, `x-overflow=reject-publish`, `x-dead-letter-strategy=at-least-once`, `x-dead-letter-exchange`, and a persistent route to the release queue.
- [ ] Prove no early target publication, broker restart recovery, missing release route retains source, crash after next-hop confirm before ACK duplicates safely, and worker restart recovery. Disallow deletion/expiration of delay topology while messages remain.
- [ ] Document delay precision, bucket count, limits, backlog, and required feature flags. Review before declaring RabbitMQ delay complete.

## Exit checks

- Ordinary RabbitMQ traffic meets the core contract with publisher confirms, mandatory returns, and manual ACK, without a Redis dependency.
- Delay tasks are never silently discarded in TTL/DLX transfer or release-worker confirmation windows under the documented RabbitMQ quorum deployment.
- Root module still imports no AMQP library; users include RabbitMQ modules only when needed.
