# Redis 可靠延时调度模块（v2）

模块路径：`github.com/goairix/mq/delay/redis/v2`。这是可选模块，可将到期消息发布到任意实现 `mq.Publisher` 的目标，包括 Redis Streams 或 Kafka。普通消息直接调用目标 adapter 的 `Publish`，不经过调度器，也不会引入这个模块。

```go
// client 是调用方创建的 go-redis UniversalClient；target 是已创建的 mq.Publisher。
scheduler, err := redisdelay.New(client, target, redisdelay.Options{
    Prefix: "orders:delay:v2",
    Shards: 16,
})
if err != nil { panic(err) }

msg, err := mq.NewMessage("order.expired", []byte(`{"order_id":"123"}`))
if err != nil { panic(err) }
if err := scheduler.PublishAt(ctx, msg, time.Now().Add(time.Hour)); err != nil { panic(err) }

// 在一个或多个 worker 实例中运行；接收 Run 的返回错误并交给进程监督器处理。
go func() { workerErr <- scheduler.Run(workerCtx) }()
```

示例需导入标准库 `time`、根模块 `mq` 和本模块 `redisdelay`。停止时先取消 `workerCtx`，等待 `Run` 返回，再调用 `scheduler.Close(ctx)`；最后由调用方关闭 Redis client 和目标 publisher。

## 确认与恢复

- `PublishAt` 用 Redis Lua 脚本一次写入消息体和到期索引，收到 Redis 确认才返回成功。传输错误可能表示结果未知，按原消息 ID、内容和到期时间重试。仍在调度中的同 ID 同内容任务重复提交是幂等的；内容或到期时间不同返回 `ErrScheduleConflict`。已完成任务重新提交可能再次投递，需要业务消费者按消息 ID 幂等。
- worker 使用 Redis 服务器时间判断到期时间，以租约领取任务。目标 `Publish` 确认成功后才删除调度记录。目标失败时 `Run` 返回错误，任务保留在租约集合，租约到期后其他 worker 可接管。
- 目标确认后、删除调度记录前进程崩溃会重复投递。目标发布结果未知时同样可能重复。租约短于目标发布耗时也可能使两个 worker 同时投递；应将 `LeaseDuration` 设得高于正常发布最长耗时，并让消费者幂等。
- 取消 `Run` 后不再领取新任务；已经领取的发布最多使用 `DrainTimeout`（默认 30 秒）完成并删除记录。目标 publisher 应尊重传入的 context。`Close(ctx)` 等待已开始的调度写入和目标发布，不关闭调用方传入的 client 或 publisher。

## 部署与容量

可靠的单节点进程重启恢复要求调度 Redis 启用 AOF，且设置 `appendfsync always`。`everysec` 可丢失最近约一秒的已确认写入，不满足本模块的可靠延时配置。Sentinel/Cluster 的异步复制故障切换仍可能丢失刚确认的写入，不能承诺零丢失。

同一个 `Prefix` 的 `Shards` 数量必须在仍有未完成任务时保持一致，否则旧 lane 可能无人扫描。每条 lane 的索引、租约、消息体和 token key 使用同一个 Redis Cluster hash slot；多 lane 可分散负载。`Run` 单实例顺序处理任务，可运行多个相同配置的 worker 来提高调度吞吐。到期精度为毫秒，`PollInterval` 默认 100 毫秒，实际投递允许迟到。

监控各 lane 的待到期与租约集合大小、最老到期时间、消息体数量、目标发布错误、确认速率、Redis 内存和 AOF 状态。调度记录不设置自动过期；若目标长期故障，应先恢复目标或人工处置，不能直接删除租约和消息体。
