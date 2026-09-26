# 可选 OpenTelemetry 桥接（v2）

模块路径：`github.com/goairix/mq/observability/otel/v2`，单独依赖 OpenTelemetry Go API。根模块、Redis、RabbitMQ、Kafka 和延时模块不会因此引入 OTel。此模块使用调用方配置的 tracer/meter provider；不创建 exporter，也不改变业务消息体。

```go
instrumentation, err := otelmq.New(otelmq.Options{
    TracerProvider: tracerProvider,
    MeterProvider:  meterProvider,
})
if err != nil { panic(err) }

publisher := instrumentation.Publisher(adapter)
batchPublisher := instrumentation.BatchPublisher(adapter)
subscriber := instrumentation.Subscriber(adapter)
batchSubscriber := instrumentation.BatchSubscriber(adapter)

// 已包装的发布者仍实现核心接口，可交给领域服务。
var _ mq.Publisher = publisher
_ = batchPublisher
_ = subscriber
_ = batchSubscriber
```

示例需导入根模块别名 `mq` 与本模块别名 `otelmq`。`Publisher.Close` 会委托给底层 adapter；其他包装接口没有新增关闭方法，调用方仍管理底层资源。需要延时时，使用 `instrumentation.ScheduledPublisher(scheduler)` 包装调度入口。

发布包装器复制 `Message.Headers` 后注入 W3C `traceparent`/`tracestate`，不会修改调用方的 map。单条消费从消息头提取父上下文并传给 handler；批量 handler 只有一个 context，因此批量 span 使用各消息的 trace context 作为 links。超过 64 条关联时，额外 links 放在批量 span 的 `mq.consume.batch.links` 子 span 上，避开 Go SDK 默认每个 span 128 条 links 的截断。若调用方把 SDK 的 link limit 配得低于 64，应同步调高。返回错误、逐条批量结果、到期时间和取消语义均由底层实现决定，包装器原样传递。

指标为 `mq.publish.messages`、`mq.publish.duration`、`mq.consume.messages`、`mq.consume.duration`。标签只包含操作、topic、订阅名与有限的结果类别，不包含消息 ID、trace ID、payload 或错误文本。发布结果类别区分 accepted、rejected、unknown；消费 handler 结果区分 success、retry、permanent。指标反映包装边界处的调用与 handler 尝试，不等于 broker 最终积压量；内部重试、死信队列 ready/unacked、Kafka group lag 和最老待处理年龄仍应从 broker/客户端原生监控采集。对可能由用户动态生成的 topic 或订阅名，应在部署端限制其指标基数。
