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

指标为：

| 指标 | 含义 |
| --- | --- |
| `mq.publish.messages`、`mq.publish.payload` | 按发布结果统计消息数和 payload 字节数；`schedule` 表示调度请求，不表示到期投递 |
| `mq.publish.duration` | 每次发布调用耗时；批量发布按调用记录一次 |
| `mq.consume.messages`、`mq.consume.payload` | 按 handler 结果统计处理尝试的消息数和 payload 字节数；重试可能重复计数 |
| `mq.consume.duration` | 每次 handler 调用耗时；批量处理按调用记录一次 |
| `mq.consume.inflight` | 当前处于 handler 中的消息数，批量调用按消息数增减 |
| `mq.consume.oldest_age` | handler 开始时批次中最老消息距 `CreatedAt` 的时间，每次调用记录一次；时钟超前的消息跳过 |

耗时与消息年龄的直方图采用显式秒级桶，覆盖毫秒操作和较长积压。指标标签只包含操作、topic、订阅名与有限的结果类别，不包含消息 ID、trace ID、payload 或错误文本。发布结果类别区分 accepted、rejected、unknown；消费 handler 结果区分 success、retry、permanent、invalid。`oldest_age` 包含延时消息的计划等待时间，只在开始消费时产生样本；它不能替代 broker 积压量或消费组 lag。内部重试、死信队列 ready/unacked、Kafka group lag 和最老待处理年龄仍应从 broker/客户端原生监控采集。对可能由用户动态生成的 topic 或订阅名，应在部署端限制其指标基数。

[Grafana dashboard](grafana/mq-v2.json) 使用 PromQL，导入 JSON 后选择 Prometheus 兼容数据源。它依赖 Collector 将 OTLP metrics 写入 Prometheus 兼容存储，并采用默认 Prometheus 指标名转换（例如 `mq.publish.duration` → `mq_publish_duration_seconds_bucket`）。若 Collector 更改了命名策略，需要同步修改面板查询。

## 导出与性能测试

调用方自行创建 OTel SDK provider 与 exporter。使用 OTLP/HTTP 时，trace 和 metric exporter 分别指向 Collector 的 `/v1/traces` 与 `/v1/metrics`。批量 trace 在一个 Collector 验收环境中曾出现未压缩请求超时；启用 trace exporter 的 gzip 压缩后，该环境的导出成功。可使用 `otlptracehttp.WithCompression(otlptracehttp.GzipCompression)`，并监控 SDK 的导出错误与队列丢弃；`ForceFlush` 成功不能证明之前所有后台导出都成功。

仅测包装器及 SDK 记录成本的基准：

```sh
cd observability/otel
go test -run '^$' -bench '^BenchmarkInstrumentationOverhead$' -benchmem
```

批量基准每次处理 256 条 1 KiB 消息，不包含 broker、网络或 exporter。真实吞吐应在实际 broker、Collector 和消费者负载下测量。

2026-09-27 在 Apple M5 / 16 GiB、Docker Desktop 8 vCPU / 约 11.7 GiB 环境，对单节点 Kafka 4.1.2、RabbitMQ 3.13.3、Redis 7.2 进行了三轮真实 OTLP/HTTP Collector 测试。每轮在同一 broker 交替发布无 OTel 与有 OTel 的请求，每种模式发布 400 批 × 256 条 × 1 KiB；下表的比值是每轮有效载荷 MB/s 比值的中位数：

| Adapter | OTel / 无 OTel | 三轮范围 |
| --- | ---: | ---: |
| Kafka | 85.5% | 71.9%–87.0% |
| RabbitMQ | 96.9% | 96.0%–97.8% |
| Redis | 94.7% | 89.4%–99.2% |

该测试采用单节点、单个发布进程、短时间负载；不代表集群容量或生产 SLO。Kafka 批量场景的开销值得在目标负载下单独评估。

加入 payload、inflight、最老消息年龄指标后，用同一负载补跑一轮，批量发布的有 OTel / 无 OTel 比值分别为 Kafka 87.3%、RabbitMQ 93.9%、Redis 94.3%；20,000 条 Kafka→ClickHouse 消息均完整入库，OTLP trace 和 metric 导出成功。这一轮是回归检查，不与上表三轮混算。
