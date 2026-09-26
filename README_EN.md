# MQ v2

MQ v2 is a breaking redesign for domain events, jobs, and continuous data ingestion. It is being developed on the long-lived `v2` branch; v2 module tags have not been published. The core module, `github.com/goairix/mq/v2`, has only standard-library dependencies and requires Go 1.23. Kafka and the optional OpenTelemetry module require Go 1.25.

| Purpose | Module |
| --- | --- |
| Core contracts | `github.com/goairix/mq/v2` |
| Redis Streams | `github.com/goairix/mq/adapters/redis/v2` |
| RabbitMQ 3.13 quorum queues | `github.com/goairix/mq/adapters/rabbitmq/v2` |
| Kafka 4.x | `github.com/goairix/mq/adapters/kafka/v2` |
| Durable Redis delay for any `mq.Publisher` | `github.com/goairix/mq/delay/redis/v2` |
| RabbitMQ quorum TTL/DLX delay | `github.com/goairix/mq/delay/rabbitmq/v2` |
| Nonpersistent test double | `github.com/goairix/mq/adapters/memory/v2` |
| Optional tracing and metrics | `github.com/goairix/mq/observability/otel/v2` |

Applications import only the adapters and optional capabilities they use. Ordinary Kafka publishing and consumption do not use Redis. The local `go.work` connects the modules for repository development and does not change the dependency graph of an application importing only the core.

Each production adapter provides at-least-once consumption: the handler returns successfully before the message is acknowledged or its offset is committed. Consumers must be idempotent by message ID. Publishing waits for broker acknowledgement; an unknown outcome can mean the message was accepted, so retry with the same ID. Distinct subscription names receive separate copies, while instances using one name compete for work. `Run` and `RunBatch` block and return operational errors for the application to supervise.

```go
message, err := mq.NewMessage("order.created", []byte(`{"id":"123"}`))
if err != nil { return err }
return publisher.Publish(ctx, message)
```

See the [Chinese README](README.md) for a migration table, adapter construction, domain-event fanout, trace batching, durable delay composition, and high-availability limits. Backend-specific details are in the [Redis](adapters/redis/README.md), [RabbitMQ](adapters/rabbitmq/README.md), [Kafka](adapters/kafka/README.md), [Redis delay](delay/redis/README.md), [RabbitMQ delay](delay/rabbitmq/README.md), and [OpenTelemetry](observability/otel/README.md) guides. The [architecture design](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md) records the v2 contract. The previous API remains on `main` and v0.x tags.
