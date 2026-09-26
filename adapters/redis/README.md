# Redis Streams adapter (v2)

模块：`github.com/goairix/mq/adapters/redis/v2`。它单独依赖 `github.com/redis/go-redis/v9`；根模块 `github.com/goairix/mq/v2` 不会因此引入 Redis 客户端。

```go
client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer client.Close()

adapter, err := redisadapter.New(client, redisadapter.Options{})
if err != nil { panic(err) }
defer adapter.Close(context.Background())

sub := mq.Subscription{Topic: "order.created", Name: "billing"}
if err := adapter.Prepare(context.Background(), sub); err != nil { panic(err) }

msg, err := mq.NewMessage(sub.Topic, []byte(`{"order_id":"123"}`))
if err != nil { panic(err) }
if err := adapter.Publish(context.Background(), msg); err != nil { panic(err) }

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go func() {
    _ = adapter.Run(ctx, sub, func(ctx context.Context, msg mq.Message) error {
        // 返回 nil 后 XACK；普通错误重试；mq.Permanent(err) 进入死信。
        return nil
    })
}()
```

示例需要导入 `context`、根模块别名 `mq`、本模块别名 `redisadapter` 和 `github.com/redis/go-redis/v9`。`Run` 和 `RunBatch` 是阻塞调用，生产代码应接收并处理它们的返回错误。

## 语义

- `Publish` 的成功是 Redis `XADD` 确认，不代表消费者已处理。连接故障后的结果可能未知；保留原消息 ID 重试，并在业务处理端按 ID 幂等。
- 默认新建组从最早保留的消息开始；只有显式设置 `StartLatest` 才从当前末尾开始。可在发布前用 `Prepare` 建好订阅组。
- 同一订阅名称的实例竞争处理，不同名称的订阅组分别消费。成功处理后 `XACK`。失败记录保留在 pending，失联实例的记录由 `XAUTOCLAIM` 接管；`ClaimIdle` 应大于消息从预取到确认的最长正常停留时间。顺序处理时，这可能包含前面整个读取批次与重试的时间。
- 永久失败先写入 `DeadLetterStream(sub)` 并等待 `XADD` 确认，再 `XACK` 原记录。死信写入失败时原记录仍 pending。死信写入成功与原记录 ACK 之间崩溃可能导致重复死信，处理方需按原 ID 去重。
- 批量结果按每条处理，成功记录使用一次 `XACK` 提交。`MaxBytes` 按 `Message.SizeBytes()` 计算；单条超出软限制时单独交付，超出 `MaxInFlightBytes` 时返回错误且不调用 handler。Redis 没有按字节限制 `XREADGROUP` 响应的参数，因此批量消费者逐条读取并按字节组成批次；已经被 Redis 标记为 pending、但放不进当前批次的下一条消息只暂存 ID，处理完当前批次后再读取完整内容。`MaxWait` 收集稀疏到达的记录。
- 取消 `Run` 或 `RunBatch` 的 context 后停止取新消息。已进入的 handler 最多使用 `DrainTimeout`（默认 30 秒）完成并确认；超时后 handler context 取消，未确认记录由 Redis 后续接管。handler 应尊重传入的 context。阻塞读取最多约 250 毫秒检查一次取消，即使 `Block` 设置得更长。
- adapter 不关闭传入的 Redis client；调用方在停止消费者并关闭 adapter 后关闭 client。`Close` 会等待已开始的发布与消费确认操作完成，超过调用方提供的 context 期限则返回对应错误。

## 部署边界

Redis 需要按业务可靠性要求配置 AOF、复制、Sentinel/Cluster 和内存上限。单节点进程重启恢复依赖已持久化的数据；异步复制故障切换仍可能丢失刚被确认的写入。不要对仍可能被任一订阅组读取或待确认的 Stream 使用 `MAXLEN`、`XTRIM` 或过期时间；本 adapter 不做自动截断。容量不足时 Redis 返回错误或阻塞，应监控 Stream 长度、组 lag、pending 数量、最老 pending 年龄，以及成功处理速率与发布速率。

当前模块只实现普通消息。需要持久化延时投递时，单独引入 [`delay/redis/v2`](../../delay/redis/README.md)，以本 adapter 作为其目标 `mq.Publisher`。不要把 Memory adapter 的 `PublishAt` 当成生产保障。

发布吞吐对照可在 AOF `appendfsync always` 的 Redis 上运行 `MQ_TEST_REDIS_ADDR=... go test ./adapters/redis -run '^$' -bench '^BenchmarkConfirmedBatch$' -benchmem`。基准使用同一 go-redis client、Stream、256 条 × 1 KiB 消息和确认策略；`paired` 子基准交替运行 adapter 与直接 pipeline，以减少服务端状态随时间变化带来的顺序偏差。Apple M5 本地 Redis 7.2 的三次 paired 采样中，adapter 相对直接客户端为 103.0%、96.76%、100.4%；Redis 8.0 为 113.0%、95.05%、99.73%。这些数字只代表本地确认发布路径，不包含消费者和下游处理。
