# MQ v2 RabbitMQ delayed publication plan

> Execute on the user-designated `v2` branch. Apply TDD and request a fresh review before completion.

**Goal:** Optional `github.com/goairix/mq/delay/rabbitmq/v2` module for RabbitMQ 3.13.3 durable delayed publication without a plugin or Redis.

**Topology:** One durable direct release exchange, a quorum release queue and a quorum failed queue; six durable quorum bucket queues with fixed `x-message-ttl` (100 ms, 1 s, 10 s, 1 min, 10 min, 1 h), `x-overflow=reject-publish`, `x-dead-letter-strategy=at-least-once`, and dead-letter route to the release queue. The `stream_queue` feature flag must be enabled. Bucket queues cannot have max-length drop policies, queue expiration, or deletion while messages remain. At-least-once DLX retains expired messages when the release route is unavailable.

**Scheduling:** Encode the full validated v2 message, its absolute due time, and a wire version. `PublishAt` selects the largest bucket at most the remaining delay (or the 100 ms bucket for shorter delays), publishes persistently with `mandatory=true`, and waits for a publisher confirm. A due time in the past publishes directly to the target. Unknown confirmation outcome returns `mq.OutcomeUnknown` and may require retry with the same message ID. RabbitMQ has no ID-based schedule deduplication.

**Release:** A worker reads the release queue with manual ACK. It decodes and validates the scheduled message. If early, it confirms publication to the next bucket before ACK. If due, it calls the caller-owned target `mq.Publisher` and ACKs only after target success. Target failure confirms re-publication to a retry bucket before ACK, keeping the release queue flowing. Corrupt records are confirmed into a durable failed quorum queue before ACK for manual replay. A failed next-hop confirmation leaves the release delivery unacknowledged and retries with bounded backoff. Crash after next-hop confirm and before ACK may duplicate; consumers must be idempotent by message ID. Worker cancellation drains current work for a bounded time; late success after deadline is left unacknowledged.

## Steps

1. Create nested module, workspace entry, options and topology. RED/GREEN tests for queue arguments, routable release exchange, fixed buckets, invalid options, and module isolation.
2. Implement `PublishAt` with full wire round trip, bucket selection, persistent mandatory confirm and unknown outcome. RED/GREEN tests for no early target delivery, confirmed schedules, and missing route.
3. Implement `Run`, retry, ACK ordering, cancellation/close. RED/GREEN tests for target failure, early reschedule, worker restart, crash window duplicate, and corrupt envelope retention.
4. Run RabbitMQ 3.13.3 fault tests: broker restart, missing release route retains bucket message, restart worker, delay precision. Document feature flags and single-node limits. Run race tests, root module isolation, and fresh read-only review.
