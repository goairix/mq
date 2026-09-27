# MQ v2

[![v2 CI](https://github.com/goairix/mq/actions/workflows/v2.yml/badge.svg?branch=v2)](https://github.com/goairix/mq/actions/workflows/v2.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

MQ 是一个 Go 普通消息队列库，提供统一的发布、消费和批量接口。Redis Streams、RabbitMQ、Kafka 及延时能力分别位于独立模块，应用只引入实际使用的实现。

## 特性

- **按需依赖：** 核心模块只依赖 Go 标准库；接入一个 adapter 不会引入其他 broker 客户端。
- **至少一次投递：** handler 成功后确认，失败重试，永久失败进入死信队列；消费端应按消息 ID 幂等。
- **批量与背压：** 支持逐条确认结果，以及按消息数、字节数和在途量限制批量消费。
- **延时能力独立：** 普通收发不启动调度器；Kafka 仅在需要延时时才使用 Redis 调度模块。

## 模块

| 模块 | 用途 |
| --- | --- |
| [`github.com/goairix/mq/v2`](go.mod) | 消息、发布与消费接口；Go 1.23 |
| [`github.com/goairix/mq/adapters/redis/v2`](adapters/redis/README.md) | Redis Streams |
| [`github.com/goairix/mq/adapters/rabbitmq/v2`](adapters/rabbitmq/README.md) | RabbitMQ 3.13 quorum queue |
| [`github.com/goairix/mq/adapters/kafka/v2`](adapters/kafka/README.md) | Kafka 4.x；Go 1.25 |
| [`github.com/goairix/mq/delay/redis/v2`](delay/redis/README.md) | Redis 持久化延时调度，可投递到任意 `mq.Publisher` |
| [`github.com/goairix/mq/delay/rabbitmq/v2`](delay/rabbitmq/README.md) | RabbitMQ quorum TTL/DLX 延时，无需延时插件 |
| [`github.com/goairix/mq/adapters/memory/v2`](adapters/memory/example/main.go) | 非持久化测试实现 |
| [`github.com/goairix/mq/observability/otel/v2`](observability/otel/README.md) | 可选的 OpenTelemetry 集成；Go 1.25 |

## 快速开始

运行 [Memory 示例](adapters/memory/example/main.go) 不需要 broker；Memory 仅供测试：

```sh
git clone --branch v2 https://github.com/goairix/mq.git
cd mq
go run ./adapters/memory/example
```

生产接入时创建所选 adapter，并将其作为 `mq.Publisher` 或 `mq.Subscriber` 交给业务代码。以下是 Redis Streams 的基本用法（假设已有 `ctx` 和 `handle`）：

```go
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer client.Close()

queue, err := redisadapter.New(client, redisadapter.Options{})
if err != nil { return err }
defer queue.Close(context.Background())

sub := mq.Subscription{Topic: "jobs.email", Name: "workers"}
if err := queue.Prepare(ctx, sub); err != nil { return err }

job, err := mq.NewMessage(sub.Topic, []byte(`{"to":"user@example.com"}`))
if err != nil { return err }
if err := queue.Publish(ctx, job); err != nil { return err }

return queue.Run(ctx, sub, handle) // 阻塞运行；handle 成功返回后确认
```

同一订阅名的多个实例共同处理队列；不同订阅名各自消费。RabbitMQ 发布前须为目标订阅调用 `Prepare`；Kafka 须预建 topic 和 `<topic>.dlq`。连接、重连和 `Run` 的返回错误由应用管理。

## 性能

测试机器：Apple M5、16 GiB 内存；Docker Desktop 分配 8 vCPU、约 11.7 GiB 内存；Go 1.25.8、Docker 29.8.0。所有 broker 运行在同一 Docker 主机。

测试负载：每批 256 条、每条 1 KiB，等待 broker 确认发布；`-benchtime=2s -count=1`。Kafka 使用单分区、三副本、`acks=all`；RabbitMQ 使用三副本持久化 quorum queue；Redis 使用一个 Stream key 和 AOF `appendfsync always`。

| 后端 | MQ adapter | 约合消息/秒 | 直接客户端 |
| --- | ---: | ---: | ---: |
| Kafka 4.1.2 | 294.43 MB/s | 28.8 万 | 281.84 MB/s |
| RabbitMQ 3.13.3 | 29.60 MB/s | 2.9 万 | 26.77 MB/s |
| Redis Cluster 7.2 | 28.02 MB/s | 2.7 万 | 26.62 MB/s |
| Redis Sentinel 7.2 | 14.86 MB/s | 1.5 万 | 13.68 MB/s |

以上是短时间、单分区/队列/key 的**确认发布**采样，不代表消费吞吐或生产容量；各后端的持久化机制也不同。测试拓扑、更多版本与故障结果见 [集群测试报告](tests/cluster/results-2026-09-27.md)。

## 使用边界

`Publish` 返回成功表示 broker 已确认接收，不表示消费者处理完成；`mq.IsOutcomeUnknown(err)` 表示结果可能已成功，重试应保留原 `Message.ID`。普通 handler 错误会重试，`mq.Permanent(err)` 会进入死信。Redis 的异步复制切换可能丢失尚未复制的写入；持续积压需要监控消费速率、待处理数量和存储容量。详细行为见各模块文档。

仓库使用 [`go.work`](go.work) 连接独立模块，仅供开发；外部应用的依赖由自己引入的模块决定。测试命令与集群拓扑见 [测试说明](tests/cluster/README.md)，发布步骤见 [发布清单](docs/RELEASING_V2.md)。

## 许可证

[MIT](LICENSE)
