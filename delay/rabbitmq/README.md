# RabbitMQ 3.13 可靠延时模块（v2）

模块路径 `github.com/goairix/mq/delay/rabbitmq/v2`。它只在需要 RabbitMQ 延时时引入，普通 RabbitMQ adapter 不依赖本模块。

```go
conn, err := amqp.Dial("amqp://user:password@rabbitmq:5672/")
if err != nil { panic(err) }
defer conn.Close()

target, err := rabbitadapter.New(conn, rabbitadapter.Options{})
if err != nil { panic(err) }
defer target.Close(context.Background())
sub := mq.Subscription{Topic: "order.timeout", Name: "billing"}
if err := target.Prepare(context.Background(), sub); err != nil { panic(err) }

scheduler, err := rabbitdelay.New(conn, target, rabbitdelay.Options{})
if err != nil { panic(err) }
defer scheduler.Close(context.Background())
if err := scheduler.Prepare(context.Background()); err != nil { panic(err) }

msg, err := mq.NewMessage(sub.Topic, []byte(`{"order_id":"123"}`))
if err != nil { panic(err) }
if err := scheduler.PublishAt(context.Background(), msg, time.Now().Add(time.Hour)); err != nil { panic(err) }

// 单独运行至少一个 worker；生产应用要监控返回错误并在重连后重建实例。
go func() { runErr <- scheduler.Run(workerCtx) }()
```

示例还需导入 `context`、`time`、根模块别名 `mq`、RabbitMQ 普通模块别名 `rabbitadapter`、本模块别名 `rabbitdelay` 和 `amqp091-go`。`target` 可以是任何 `mq.Publisher`，包括需要额外部署 Redis 调度器的 Kafka 路径之外的后端；目标队列拓扑仍须由调用方预先创建。

## 语义与部署条件

- 使用固定 100 ms、1 s、10 s、1 min、10 min、1 h 的 quorum TTL 桶。大于 1 小时的延时会多次转桶；不足 100 ms 的延时至少等待一个 100 ms 桶。到期只保证**不早于**指定时间，实际延迟还受 broker、DLX 重试、worker 积压和目标发布耗时影响。
- 每个桶设置 `x-overflow=reject-publish` 与 `x-dead-letter-strategy=at-least-once`；持久化的消息经 DLX 到持久化 release quorum 队列。worker 用手动 ACK，确认发布到下一个桶或目标后才 ACK release 消息。目标暂时失败会确认写入重试桶后 ACK release，让其他到期消息继续流动；损坏封套确认写入持久化 `Prefix.failed` 隔离队列后 ACK，需人工检查和重放。确认与 ACK 间崩溃可造成重复；业务按消息 ID 幂等。
- `PublishAt` 使用 publisher confirm 与强制路由检查。`PublishChannels`（默认 8）限制并发确认 channel 数，`MaxMessageBytes`（默认 8 MiB）限制单条逻辑消息。结果未知会返回 `mq.OutcomeUnknown`；调用方可保留原 ID 重试，但 RabbitMQ 不提供按 ID 去重，因此可能重复。过去时间的消息直接交给目标 `Publish`。
- 发布者和 worker 应使用稳定的相同 `Prefix`；不要删除、过期、清空延时桶和 release 队列，也不要把它们的 `overflow` 改成 `drop-head`。RabbitMQ 3.13 要启用 `stream_queue` feature flag。不要给 release 队列配置有限 `delivery-limit`，否则多次进程故障可能导致消息被丢弃。单节点可恢复进程重启，不能在唯一节点停机时保持服务；高可用需多数 quorum 副本存活。
- 监控各桶的 `messages`、`messages_dlx`、release ready/unacked、worker 错误和目标确认耗时。DLX 路由故障会让到期消息滞留在源桶；修复路由后 RabbitMQ 内部会周期性重试，可能显著晚于到期时间。

集成测试使用 `MQ_TEST_RABBIT_URL`；节点重启测试还需 `MQ_TEST_RABBIT_DOCKER_RESTART=1` 和可选 `MQ_TEST_RABBIT_IMAGE`。路由故障的长时间重试测试需显式设置 `MQ_TEST_RABBIT_ROUTE_FAULT=1`。
