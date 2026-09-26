# Kafka 4.x adapter（v2）

模块路径：`github.com/goairix/mq/adapters/kafka/v2`，只依赖 Kafka 客户端 `franz-go`，需要 Go 1.25。根模块仍支持 Go 1.23，且不引入 Kafka。此模块提供普通消息；需要可靠延时时，可额外引入 `github.com/goairix/mq/delay/redis/v2`，将本 adapter 作为其目标 `mq.Publisher`。普通发布、消费不访问 Redis。

```go
adapter, err := kafkaadapter.New([]string{"kafka-1:9092", "kafka-2:9092"}, kafkaadapter.Options{})
if err != nil { panic(err) }
defer adapter.Close(context.Background())

sub := mq.Subscription{Topic: "order.created", Name: "billing"}
msg, err := mq.NewMessage(sub.Topic, []byte(`{"order_id":"123"}`))
if err != nil { panic(err) }
msg.Key = []byte("123") // 同一 key 通常落到同一分区
if err := adapter.Publish(context.Background(), msg); err != nil { panic(err) }

// Run 阻塞；生产进程应监督其返回值，并在连接故障后重启消费者。
err = adapter.Run(ctx, sub, func(ctx context.Context, m mq.Message) error {
    // 成功才提交 offset；普通错误重试；mq.Permanent(err) 先写 DLQ。
    return handle(ctx, m)
})
```

示例需导入 `context`、根模块别名 `mq`、本模块别名 `kafkaadapter`。应用应先创建源 topic 和 `<topic>.dlq`，配置分区、副本、`min.insync.replicas`、保留期与消息大小。`Prepare` 只校验订阅参数，不创建 topic。适配器拒绝通过 `ClientOptions` 关闭幂等写入或开启自动建主题。生产者使用 `acks=all` 与 franz-go 幂等写入；调用方仍须按消息 ID 处理结果未知时的重复。

## 语义和容量

- `Publish` 复用生产者并等待 broker 确认；`PublishBatch` 按消息数和逻辑字节数切批，逐条返回与输入顺序对应的成功、明确拒绝或结果未知。上下文取消时已经发出的消息可能成功，结果未知不能当作未投递。
- 每次 `Run` / `RunBatch` 启动一个 consumer group member。默认从最早保留 offset 开始；同名订阅竞争处理，不同名称的组分别处理。手动提交只发生在 handler 成功或死信写入确认之后。同一分区里的前一条失败时不会越过它提交后一条；已处理但提交失败或重平衡时可能重复。
- 普通错误会在当前记录上指数退避重试。永久错误会把原始 record、原 ID 和业务头写入 `<topic>.dlq`，加上失败类别、截断为 256 字节的原因和来源 topic，再提交源 offset。死信主题缺失、不可用或大小限制不够时源 offset 保持未提交，消费停止并返回错误，需先修复目标。DLQ 的 `max.message.bytes` 应高于源 topic 的允许记录大小，容纳额外头字段；外部生产者发送的超大记录也可能卡在这条边界。
- `BatchOptions.MaxBytes` 和 `MaxInFlightBytes` 使用 `mq.Message.SizeBytes()` 约束交给 handler 的逻辑消息。单条超过软上限时独立交付；超过硬上限时返回错误且不调用 handler。Kafka fetch 会在客户端解压，且允许第一批超过 fetch 限额，所以这两个选项**不能严格限制进程总内存**；还应在 broker/topic 设置消息大小上限并监控消费者内存。`RunBatch` 在途批次数目前为 1，较大的 `MaxInFlightBatches` 不会增加并行度。
- `RebalanceTimeout` 默认 5 分钟，单次 poll 的 `MaxPollWork` 默认其 80%。超过期限的合作式 handler 会收到取消的 context，记录不提交，返回错误供监督器重启。handler 必须尊重 context；Go 无法强行中止一个不返回的 handler，它可能继续占用重平衡门闩。取消订阅时不再取新记录，已进入的 handler 最多有 `DrainTimeout`（默认 30 秒）完成并提交。`Close(ctx)` 等待活动操作，即使等待超时，也会在活动操作结束后释放生产者。
- 普通生产者缓冲受 `MaxBufferedBytes`（默认 64 MiB）和 `MaxBufferedRecords`（默认 10,000）限制，底层 record batch 上限与 `BatchMaxBytes`（默认 1 MiB）一致，避免并发发布合并成过大的 Kafka 批次。首次写死信时会按需建立独立的生产者，其底层批次与缓冲上限为 `BatchMaxBytes + 4 KiB`，给死信元数据留余量；配置的 `MaxBufferedBytes` 至少要比 `BatchMaxBytes` 多 4 KiB。应用层 `PublishBatch` 还受 `BatchMaxMessages`（默认 256）限制。`MaxMessageBytes` 默认 1 MiB；还需给 Kafka record framing、死信头字段和 broker/topic 上限留余量。消费者 `ConsumerFetchBytes` 默认 8 MiB，`ConsumerPollRecords` 默认 256。持续输入高于处理能力时，Kafka 会积压，须按保留期和磁盘容量规划并监控 group lag、最老未处理消息年龄、处理速率与发布速率。

## 验证

集成测试设置 `MQ_TEST_KAFKA_BROKERS` 与 `MQ_TEST_KAFKA_TOPIC`，后者需是预建的普通 topic，且 `<topic>.dlq` 已建。契约测试会另外创建独立 topic，需要测试账号有建主题权限。broker 重启测试还需 `MQ_TEST_KAFKA_DOCKER_RESTART` 指向可丢弃的容器名，不能与其他测试并发执行。

本地已在 Apache Kafka 4.0.2 和 4.1.2 的单节点 KRaft 容器运行集成测试；单节点测试证明进程重启恢复，不能证明集群故障切换高可用。吞吐对照可运行 `go test -run '^$' -bench BenchmarkConfirmedBatch -benchmem`。4.0.2 本地 Apple M5、1 KiB × 256 条、同一 producer/主题与 `acks=all` 的 3 次采样中，adapter 中位发布吞吐 383.55 MB/s，直接 franz-go 为 399.08 MB/s（约 96%）；P95 分别约 0.92 ms 和 0.87 ms。这个数字仅代表该环境的发布路径，下游处理和真实集群需另测。
