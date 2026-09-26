# RabbitMQ 3.13 quorum adapter（v2）

模块路径：`github.com/goairix/mq/adapters/rabbitmq/v2`，单独依赖官方 `amqp091-go`。根模块和其他 adapter 不会因此引入 AMQP 客户端。

```go
conn, err := amqp.Dial("amqp://user:password@rabbitmq:5672/")
if err != nil { panic(err) }
defer conn.Close()

adapter, err := rabbitadapter.New(conn, rabbitadapter.Options{})
if err != nil { panic(err) }
defer adapter.Close(context.Background())

sub := mq.Subscription{Topic: "order.created", Name: "billing"}
if err := adapter.Prepare(context.Background(), sub); err != nil { panic(err) }
msg, err := mq.NewMessage(sub.Topic, []byte(`{"id":"123"}`))
if err != nil { panic(err) }
if err := adapter.Publish(context.Background(), msg); err != nil { panic(err) }

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go func() {
    runErr <- adapter.Run(ctx, sub, func(ctx context.Context, message mq.Message) error {
        // nil：确认；普通错误：退避重试；mq.Permanent(err)：确认写入死信后确认原消息。
        return nil
    })
}()
```

示例需导入标准库 `context`、根模块别名 `mq`、本模块别名 `rabbitadapter` 和 `github.com/rabbitmq/amqp091-go`。生产进程应处理 `Run`/`RunBatch` 的返回错误，连接断开后重建连接与 adapter。

## 发布与消费语义

- 先为所有目标订阅组调用 `Prepare`，再发布。`Prepare` 创建 durable topic exchange、每个订阅组的 durable quorum 队列及对应的 durable quorum 死信队列。Topic 映射成固定长度的路由键，原 Topic 保留在消息封套里，避免 `*`/`#` 被解释为 RabbitMQ 通配符。
- `Publish` 使用持久化消息、`mandatory=true` 和 publisher confirm。只有确认且可路由时返回 nil。无法路由返回 `mq.ErrNoRoute`；发送或确认中断返回 `mq.OutcomeUnknown`，重试应保留原消息 ID，消费者按 ID 幂等。发布 channel 池有固定大小，批量发布在同一 channel 上流水线发送并按各条确认结果返回。
- 源队列和死信队列都设置 `x-overflow=reject-publish`，避免队列长度策略丢弃旧消息。`Run` 使用有限预取与手动 ACK。暂时错误在当前消息上有上限地退避重试；永久错误先确认发布到 `DeadLetterQueue(sub)`，再 ACK 源消息。死信确认后、源消息 ACK 前崩溃可能产生重复死信。
- `RunBatch` 使用 `basic.get` 逐条获取并组成批次，以便在 handler 阻塞时不保留超过 `MaxInFlightBytes` 的客户端预取内容。单条超过硬上限会重新入队并返回错误。这个严格字节边界带来额外往返；普通高吞吐单条消费可使用 `Run`，批量消费的性能应在自己的消息大小、批次和网络条件下测量。
- 批量发布同时受 `PublishBatchSize`（默认 256 条）和 `MaxPublishBytes`（默认 8 MiB 逻辑消息字节）限制；单条超限会被拒绝。取消消费 context 后停止取新消息；已进入的 handler 最多有 `DrainTimeout`（默认 30 秒）完成并确认。handler 应尊重传入的 context。`Close(ctx)` 等待已开始的操作，不关闭调用方的连接。

## 部署

目前验证基线为 RabbitMQ 3.13.3。单节点的 durable quorum 队列可在进程重启后恢复已持久化的消息，但唯一节点停机期间不可用；集群高可用需要保持多数副本可用。监控发布确认耗时和未知结果、队列 ready/unacked、死信数量、消费者处理速率及磁盘告警。

可靠延时投递由独立的 [`delay/rabbitmq/v2`](../../delay/rabbitmq/README.md) 模块提供；普通收发不依赖延时拓扑。RabbitMQ 3.13 quorum TTL/DLX 延时使用 at-least-once dead-lettering、`reject-publish` overflow 和有效的目标路由，并由独立 worker 确认投递到目标队列。

故障验证可运行 `MQ_TEST_RABBIT_DOCKER_RESTART=1 go test ./adapters/rabbitmq -run TestBrokerRestartPreservesConfirmedMessage`；吞吐对照可运行 `MQ_TEST_RABBIT_URL=... go test ./adapters/rabbitmq -run '^$' -bench BenchmarkConfirmedBatch -benchmem`。在本地 RabbitMQ 3.13.3 单节点、1 KiB × 256 条、确认持久化 quorum 队列的同一进程对照中，adapter 为 44.68 MB/s，直接 AMQP 客户端为 46.02 MB/s；此数据只代表该环境的发布路径。
