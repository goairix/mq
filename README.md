# MQ v2

MQ v2 是面向领域事件、任务和持续数据上报的 Go 消息库。它提供小型公共契约，由接入方选择 broker 和可选能力。v2 是破坏性重构；目前在长期 `v2` 分支开发，尚未发布 v2 模块标签。

根模块 `github.com/goairix/mq/v2` 仅依赖 Go 标准库，最低 Go 1.23。Kafka 和 OpenTelemetry 子模块要求 Go 1.25。仓库的 `go.work` 用于本地开发，不会让只使用根模块的应用引入 broker 客户端。

## 先理解它怎么工作

日常使用只需认识三个对象：

1. `mq.Message` 是一条消息，包含 topic、消息 ID、payload 等数据。
2. `mq.Publisher` 负责把消息写入 broker。Redis、RabbitMQ 或 Kafka adapter 是它的具体实现。
3. `mq.Subscriber` 负责持续读取消息并调用你的 handler。handler 返回 `nil` 后 adapter 才确认消息；返回普通错误会重试。

```text
业务代码 ── Publish(Message) ──> 所选 adapter ──> Redis / RabbitMQ / Kafka
业务 handler <── Run(Subscription, handler) <── 所选 adapter <── broker
```

根模块只规定调用方式，不负责连接 broker。程序启动时由你创建所选 adapter，再把它作为 `mq.Publisher` 或 `mq.Subscriber` 传给业务代码。v1 的统一工厂已经移除，因此无需配置或编译未选择的后端。批量、延时和 OTel 都是按需要额外使用的能力；初次接入可以先忽略它们。

先运行不需要外部服务的 [完整 Memory 示例](adapters/memory/example/main.go)：

```bash
go run ./adapters/memory/example
```

它创建一个 `order.created` 消息，用 Memory adapter 发布，再以订阅名 `billing` 读取。输出是 `order.created: {"order_id":"123"}`。Memory 只为演示和测试保留消息；理解这条调用链后，把 `memory.New(16)` 换成所需生产 adapter 的构造过程即可。下面的模块表和示例说明各后端的连接与部署要求。

## 模块选择

| 用途 | 模块路径 | 能力与条件 |
| --- | --- | --- |
| 公共契约 | `github.com/goairix/mq/v2` | 消息、发布、订阅、批量、错误类别、契约测试 |
| Redis Streams | `github.com/goairix/mq/adapters/redis/v2` | 普通消息；需要 Redis 持久化配置 |
| RabbitMQ 3.13 | `github.com/goairix/mq/adapters/rabbitmq/v2` | 普通消息；durable quorum 队列 |
| Kafka 4.x | `github.com/goairix/mq/adapters/kafka/v2` | 普通消息；Go 1.25，需预建 topic 与 DLQ |
| Redis 延时 | `github.com/goairix/mq/delay/redis/v2` | 可将到期消息交给任意 `mq.Publisher`，包括 Kafka |
| RabbitMQ 延时 | `github.com/goairix/mq/delay/rabbitmq/v2` | quorum TTL/DLX + release worker；不依赖延时插件 |
| Memory | `github.com/goairix/mq/adapters/memory/v2` | 非持久化测试替身 |
| OpenTelemetry | `github.com/goairix/mq/observability/otel/v2` | 可选的接口装饰器；Go 1.25 |

应用只需在自己的 `go.mod` 中引入所用模块。普通 Redis/RabbitMQ/Kafka 收发均不初始化延时调度器；Kafka 普通路径也不访问 Redis。各模块的部署和错误边界见各自 [Redis](adapters/redis/README.md)、[RabbitMQ](adapters/rabbitmq/README.md)、[Kafka](adapters/kafka/README.md)、[Redis 延时](delay/redis/README.md)、[RabbitMQ 延时](delay/rabbitmq/README.md) 和 [OTel](observability/otel/README.md) 说明。

## 快速接入

下面展示各后端的构造入口。导入别名对应模块路径；连接由应用创建，`Close(ctx)` 和 `Run` / `RunBatch` 的返回错误由应用管理。代码片段使用已有的 `ctx`、`sub`、`client`、`conn` 等变量。

```go
// Redis：client 是 *redis.Client 或其他 redis.UniversalClient。
redisMQ, err := redisadapter.New(client, redisadapter.Options{})

// RabbitMQ：conn 是 *amqp.Connection。发布前为每个目标订阅调用 Prepare。
rabbitMQ, err := rabbitadapter.New(conn, rabbitadapter.Options{})
if err == nil { err = rabbitMQ.Prepare(ctx, sub) }

// Kafka 4.x：topic 和 <topic>.dlq 需由部署流程预建。
kafkaMQ, err := kafkaadapter.New([]string{"kafka-1:9092", "kafka-2:9092"}, kafkaadapter.Options{})

// 单元测试：内存数据随进程退出消失。
testMQ, err := memory.New(128)
```

每个 adapter 都实现 `mq.Publisher`、`mq.BatchPublisher`、`mq.Subscriber` 和 `mq.BatchSubscriber`；Memory 还实现仅供测试的 `PublishAt`。生产代码通常只依赖需要的核心接口：

```go
func EmitOrder(ctx context.Context, out mq.Publisher, orderID string, payload []byte) error {
    event, err := mq.NewMessage("order.created", payload)
    if err != nil { return err }
    event.Key = []byte(orderID)
    event.Headers = map[string]string{"schema": "order.created.v2"}
    return out.Publish(ctx, event)
}
```

`Publish` 返回 nil 只表示 broker 按其确认配置接收消息，不表示消费者处理完成。`mq.IsOutcomeUnknown(err)` 为 true 时，原消息可能已经进入 broker；重试须沿用原 `Message.ID`，业务处理须按 ID 幂等。

## 领域事件与批量上报

同一 topic 的不同订阅名各自消费一份消息；同一订阅名的多个实例竞争处理。RabbitMQ 需在发布前为两个订阅都调用 `Prepare`。消费者的 `Run` 会阻塞，应由进程监督其返回值，在断线后重建连接与 adapter。

```go
eventSub := mq.Subscription{Topic: "order.created", Name: "billing"}
auditSub := mq.Subscription{Topic: "order.created", Name: "audit"}
go func() { runErr <- subscriber.Run(ctx, eventSub, billOrder) }()
go func() { runErr <- subscriber.Run(ctx, auditSub, auditOrder) }()
```

持续上报 trace 等场景可按消息数、逻辑字节数和等待时间组批。下面的 handler 在持久化成功后确认整批；如果只有个别记录不可处理，可返回与 `messages` 等长的逐条错误列表，`mq.Permanent(err)` 会将该条送往死信。Kafka 同一分区只有连续完成的 offset 才会提交。

```go
opts := mq.BatchOptions{
    MaxMessages: 256, MaxBytes: 1 << 20, MaxWait: 50 * time.Millisecond,
    MaxInFlightBatches: 1, MaxInFlightBytes: 16 << 20,
}
err := batchSubscriber.RunBatch(ctx,
    mq.Subscription{Topic: "agent.trace", Name: "analytics"}, opts,
    func(ctx context.Context, messages []mq.Message) ([]error, error) {
        if err := writeBatch(ctx, messages); err != nil { return nil, err }
        return nil, nil
    })
```

实际吞吐取决于下游写入能力。持续输入速率高于处理速率时，消息仍会积压；需要监控处理速率、broker lag 或 ready/unacked、最老待处理年龄、死信和容量。订阅端控制并发与批大小，业务侧应测试积压追赶速度。库不内置 ClickHouse 驱动或业务序列化。

## 可靠延时

`mq.ScheduledPublisher.PublishAt(ctx, message, absoluteTime)` 返回成功表示调度记录已确认。到期可能迟到或重复，消费者仍需幂等。RabbitMQ 3.13 使用独立 TTL/DLX 模块；Redis 调度器可把到期消息发布到 Redis Streams 或 Kafka：

```go
// target 可以是 redisMQ 或 kafkaMQ；client 是配置 AOF 的 Redis client。
scheduler, err := redisdelay.New(client, target, redisdelay.Options{})
if err != nil { return err }
err = scheduler.PublishAt(ctx, message, time.Now().Add(time.Hour))

// 在少量独立 worker 实例中，用相同 Prefix/Shards 配置运行：
// workerErr <- scheduler.Run(workerCtx)
```

RabbitMQ 使用 `rabbitdelay.New(conn, rabbitMQ, rabbitdelay.Options{})`，先调用 `scheduler.Prepare(ctx)`，再运行至少一个 `scheduler.Run(workerCtx)`。Kafka 仅在使用延时时才需引入和运行 Redis 调度模块；不要让每个 API 副本都启动调度 worker。延时调度数据的持久化、租约、轮询压力和高可用限制见对应子模块文档。

## 从 v1 迁移

| v1 | v2 |
| --- | --- |
| 根模块 `github.com/goairix/mq`、`NewFactory(config.Config).CreateMQ()` | `github.com/goairix/mq/v2` + 单独导入所需 adapter，直接调用其 `New` |
| `Producer().Send(ctx, *message.Message)` | `mq.NewMessage` 创建带稳定 ID 的值；`mq.Publisher.Publish(ctx, mq.Message)` |
| `Producer().SendBatch` 只返回整批错误 | `mq.BatchPublisher.PublishBatch` 返回按输入顺序逐条对应的 `PublishResult` |
| `Consumer().Subscribe(ctx, topic, handler)` | `mq.Subscriber.Run(ctx, Subscription{Topic, Name}, handler)`；调用阻塞并返回运行错误 |
| `DelayQueue().Push/Pop/Remove/Size`、进程内时间轮 | 单独的 `ScheduledPublisher.PublishAt`；生产延时使用 Redis 或 RabbitMQ 调度模块并运行 worker |
| 内置序列化、统一后端配置、隐式观测 | 业务选择 Payload 编码；后端各管配置；OTel 单独引入 |

迁移时需重新选择订阅名、预建 RabbitMQ 订阅拓扑或 Kafka topic/DLQ，并检查旧消息格式。v2 不提供兼容 shim，也不自动转换旧消息。旧 API 在 `main` 分支和 v0.x 标签中。

## 可靠性与开发

所有生产 adapter 都是至少一次语义：成功处理后确认，死信目标确认后再确认源消息；确认窗口允许重复。Redis 单节点可靠性依赖 AOF，异步复制切换可能丢失已确认写入；RabbitMQ 单节点 quorum 队列可恢复重启但停机期间不可用，集群需要多数副本；Kafka 的副本、`min.insync.replicas` 和保留期由部署方配置。可靠延时表示应用进程重启后调度记录可恢复，不保证精确到期或单节点故障期间仍可服务。

仓库 `go.work` 连接所有模块。本地测试示例：`go test ./...`、`go test ./adapters/redis/... ./delay/redis/...`；根模块依赖隔离可用 `GOWORK=off go list -m all` 验证。CI 配置覆盖 Redis 7.2/8.0、RabbitMQ 3.13.3、Kafka 4.0.2/4.1.2、Memory 和 OTel。详细设计见 [v2 架构文档](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md)，标签顺序见 [发布清单](docs/RELEASING_V2.md)。
