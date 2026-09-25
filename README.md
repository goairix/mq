# MQ v2

Go 消息库的 v2 重构正在 `v2` 分支进行。根模块 `github.com/goairix/mq/v2` 只有标准库依赖，提供消息模型、发布/订阅接口与可复用契约测试。生产 adapter 尚未发布。

设计见 [v2 架构文档](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md)。旧版代码可在 `main` 分支和 v0.x 标签中获取。

## 本地开发

仓库的 `go.work` 连接根模块与已实现的子模块。运行 `go test ./...` 测试根模块，运行 `go test ./adapters/memory/...` 测试 Memory；检查根模块独立依赖图时使用 `GOWORK=off go list -m all`。

## Memory 测试替身

Memory 是独立模块 `github.com/goairix/mq/adapters/memory/v2`，可在测试中直接构造：

```go
broker, err := memory.New(128)
if err != nil { panic(err) }
defer broker.Close(context.Background())

msg, err := mq.NewMessage("order.created", []byte(`{"id":"123"}`))
if err != nil { panic(err) }
if err := broker.Publish(context.Background(), msg); err != nil { panic(err) }

ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go func() {
    _ = broker.Run(ctx, mq.Subscription{Topic: "order.created", Name: "billing"},
        func(_ context.Context, msg mq.Message) error {
            // 成功后确认；返回错误将重试。
            return nil
        })
}()
```

示例需导入标准库 `context`、根模块别名 `mq` 和 Memory 模块别名 `memory`。Memory 的普通消息和 `PublishAt` 延时记录都只存在于当前进程，关闭或重启后会丢失；不能用作生产队列。生产应用将在后续版本按需单独引入 Redis、RabbitMQ 或 Kafka adapter。
