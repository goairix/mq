# MQ v2 架构设计

日期：2026-09-25
状态：设计已在对话中确认；本文等待最终审阅

## 目标与范围

v2 是一次破坏性重构。它面向领域事件、任务、持续上报的 trace 等不同消息场景，统一提供至少一次投递、明确的确认与背压语义，同时允许调用方只引入需要的后端实现。普通消息的吞吐和可用性优先；延时消息走独立路径。

本设计覆盖公共契约、模块边界、各后端的语义要求、故障边界与验收标准。Redis、RabbitMQ、Kafka 和延时调度器的内部实现各自作为后续工作包设计和实施。

### 已确定的约束

- Go 模块使用 v2 路径；v1 API 不保留兼容层。
- 主模块不依赖任何 Redis、RabbitMQ、Kafka、MsgPack、Zap 或 OpenTelemetry 包，也不通过工厂隐式引入 adapter。
- 生产后端提供至少一次消费：处理成功之后才确认；重复投递是正常结果，接入方应能幂等处理。
- 所有生产后端都提供可在应用进程重启后恢复的延时投递能力；Memory 仅作测试用非持久化实现。
- Kafka broker 只支持 Apache Kafka 4.x，最低验收版本为 4.0；不维护 3.x 或 ZooKeeper 兼容逻辑。Kafka 4.x 使用 KRaft。
- RabbitMQ 基线为用户当前的 3.13.3；延时路径默认使用 quorum queue + 固定 TTL 分层 + DLX，不依赖 delayed-message-exchange 插件。
- Kafka 延时使用单独的 Redis 调度模块；普通 Kafka 发布和消费不依赖 Redis。
- 当前没有确定的消息大小、峰值吞吐或消费者并发目标；采用相同可靠性配置下的底层客户端对照基准，并在真实负载出现后补充绝对容量目标。
- 主模块维持 Go 1.23 最低版本；Kafka 子模块因所选 `franz-go` 版本单独要求 Go 1.25。接入 Redis、RabbitMQ 或只使用核心的项目无需因此升级 Go。仓库级 `go.work` 和全模块 CI 使用 Go 1.25。

## 方案选择

| 方案 | 取舍 |
| --- | --- |
| **小核心 + 独立 adapter + 可选能力（采用）** | 公共语义清晰，主模块依赖最少；发布、批量、延时和观测分别装配。代价是多模块发布与测试矩阵更复杂。 |
| 一个统一的 `MQ` 大接口和内置工厂 | 上手调用少，但会把不适用于所有后端的 `DelayQueue.Pop/Remove/Size`、配置和驱动依赖一起带入，容易制造虚假的一致性。 |
| 完全独立的各后端包 | 每个后端可充分利用原生能力，但接入方很难复用领域事件处理代码和契约测试。 |

## 模块与依赖

| 模块路径 | 责任 | 允许的第三方依赖 |
| --- | --- | --- |
| `github.com/goairix/mq/v2` | 消息模型、最小接口、错误分类、通用选项与契约测试工具 | 无 |
| `github.com/goairix/mq/adapters/redis/v2` | Redis Streams 普通队列、Redis 原生可靠延时 | Redis Go 客户端 |
| `github.com/goairix/mq/adapters/rabbitmq/v2` | RabbitMQ 普通队列与 quorum/TTL/DLX 延时 | AMQP 客户端 |
| `github.com/goairix/mq/adapters/kafka/v2` | Kafka 4.x 普通发布与消费 | `franz-go` v1.21.6；仅 Kafka 子模块要求 Go 1.25 |
| `github.com/goairix/mq/delay/redis/v2` | 将 Redis 持久化调度器与普通 `Publisher` 组合 | Redis Go 客户端；不依赖 Kafka adapter |
| `github.com/goairix/mq/adapters/memory/v2` | 非持久化测试替身 | 无 |
| `github.com/goairix/mq/observability/otel/v2` | OpenTelemetry 桥接 | OTel |

各子模块在各自目录维护 `go.mod` 和带目录前缀的版本标签。仓库使用 `go.work` 进行本地开发；CI 单独测试每个模块。主模块的测试不得导入 adapter，以免污染主模块依赖图。导入 adapter 的示例放在相应子模块或独立示例模块内。仓库根目录不再提供选择后端的工厂，调用方直接构造 adapter 并把它赋给核心接口。

## 公共契约

### 消息

消息承载 `ID`、`Topic`、`Key`、`Payload`、`Headers`、`CreatedAt`。`Payload` 是字节序列；业务负责 JSON、Protobuf 等编码及 schema 版本。`ID` 在发布与重试之间保持稳定，用于接入方幂等处理；构造函数用标准库生成 ID，手工构造的消息在缺少 ID 时被拒绝。`Key` 供支持分区的后端使用，不表示跨后端统一的顺序保证。保留内部消息头命名空间和 wire format 版本，避免业务头与适配器元数据冲突。

### 接口分离

- `Publisher.Publish(ctx, msg)`：正常发布。成功返回代表对应后端已经按配置确认接收，不代表业务消费者已处理。失败可能是明确失败，也可能是结果未知；结果未知用可识别的错误类别返回，调用方重试时必须保留原 ID。
- `BatchPublisher.PublishBatch(ctx, msgs)`：批量发布。按输入顺序返回等长的逐条结果，每项为成功、失败或结果未知；不承诺整批原子性。
- `Subscriber.Run(ctx, subscription, handler)`：阻塞运行一个订阅，handler 返回成功后确认。停止或运行故障通过返回值暴露，不把永久错误藏在后台 goroutine 中。
- `BatchSubscriber.RunBatch(ctx, subscription, batchHandler)`：按最大消息数、字节数或等待时间交付批次。handler 返回 `nil` 结果和 `nil` 错误表示全批成功，返回普通错误表示全批重试，也可返回与输入等长的逐条结果标注成功、重试或永久失败；Kafka 只提交各分区连续完成的 offset。无效的结果长度使整批保持未确认并返回订阅错误。
- `ScheduledPublisher.PublishAt(ctx, msg, dueAt)`：可选的延时发布。返回成功表示调度记录已被后端确认；允许迟到与重复，不承诺精确时刻投递。

本文件冻结行为语义；具体 Go 类型与方法签名在第一工作包设计中固定，然后实施。接口没有通用 `Pop`、`Remove`、`Size`、事务或历史重放方法；这些能力若需要，由相应后端单独提供。

### 订阅与路由

订阅由准确的 `Topic` 和稳定的 `Subscription` 名称标识。同名订阅的多个实例竞争处理；不同名称的订阅分别获取发布后的消息。Kafka 映射到 consumer group，Redis Streams 映射到 consumer group，RabbitMQ 映射到持久化队列和 binding。通用接口只支持准确的 topic 名称；通配符路由属于后端扩展。

RabbitMQ 新建的队列无法收到建队前的消息。因此 RabbitMQ 模块提供明确的订阅拓扑预创建操作和部署说明；`Run` 可验证或声明已配置拓扑，但不能把“首次启动消费者”误写为历史重放。Kafka 和 Redis 新建组的起始位置显式配置；可靠性优先的默认值为最早可用消息，选择最新位置需要调用方明确指定。

### 处理结果与重试

- 单条 handler 返回 `nil` 表示成功；普通错误表示暂时失败；显式永久错误表示转入死信路径。
- 暂时失败使用有上限的指数退避并保留原消息未确认。默认持续重试，避免重启后丢失进程内的尝试次数。失败会暂时阻塞该消息所在的顺序范围；其他分区或队列可继续处理。
- 批量 handler 整批失败时整批重试；每条结果可用于隔离坏消息。整批成功不要求每条单独 ACK 往返。
- 死信写入必须先获得目标后端确认，再确认原消息；死信不可用时原消息仍待处理。死信保留原 ID、业务头、失败原因类别和来源位置。后端不承诺跨 broker 的原子事务，因此确认窗口仍允许重复。
- Kafka 在分区内仅提交连续成功的 offset；重平衡或关闭时不得提交尚在处理中的 offset。Redis 使用 `XACK`，失联后的 pending 消息由 `XAUTOCLAIM` 接管。RabbitMQ 使用手动 ACK 和受限 prefetch。

### 关闭与背压

消费实例在 context 取消时停止取新消息，给在途 handler 一个可配置的排空期限，期限内完成的消息才确认；未完成消息由 broker 重投。发布端的内部队列、在途消息数和字节数均有上限；满时等待或返回明确错误。关闭生产者时等待已提交的发布结果，未确认的消息以结果未知返回。任何路径都不允许静默丢弃。

## 各后端设计边界

| 后端 | 普通消息路径 | 顺序、重放及高可用边界 |
| --- | --- | --- |
| Redis | Streams + consumer group；批量 `XADD/XREADGROUP/XACK`；过期 pending 由 `XAUTOCLAIM` 恢复；禁止自动截断尚未被所有订阅组确认的 entry | 单实例重启可恢复已持久化数据；Sentinel/Cluster 的异步复制切换可能丢失已经返回成功的写入，不能宣称零数据丢失。 |
| RabbitMQ 3.13.3 | 持久化 exchange/queue、quorum queue、持久消息、publisher confirm、强制路由检查、手动 ACK、QoS prefetch；队列满时拒绝发布并返回错误 | 当前单节点配置 quorum group size 1，只能承受进程重启；生产 HA 使用至少三节点、group size 3。并发消费者不承诺业务处理完成顺序。 |
| Kafka 4.x | KRaft 集群、topic/consumer group、批量生产、`acks=all`、手动 offset commit；使用稳定的 Kafka 4.x group 协议能力 | 同一个 Key 的记录在单分区内有顺序；跨分区无全序。生产 HA 需副本数与 `min.insync.replicas` 配置配合。保留期结束后的历史消息无法重放。 |
| Memory | 有界内存队列与定时器 | 仅用于测试；进程重启会丢消息，也不承诺 HA。 |

Redis Stream 默认不设置可能删除未确认消息的 `MAXLEN` 截断。若业务需要保留期，先计算各订阅组安全的最小确认位置再清理；主动选择有损截断的部署不在至少一次保证范围内。Kafka adapter 只实现 Kafka 4.x broker 路径，不提供旧 `Version` 配置、3.x 协议降级、ZooKeeper 部署示例，也不使用 Kafka 4.1 预览中的 Queues for Kafka 功能。Kafka 驱动选用 `franz-go` v1.21.6，并针对超时、取消、重平衡和 broker 重启做故障测试；驱动的 API 不进入核心接口。

## 延时消息

- **Redis：**使用持久化的待投递索引与消息体，原子领取到期任务并设置可恢复租约。写入目标 Stream 成功后才删除调度记录；进程在写入后、删除前崩溃会重复投递。单 Redis Cluster 部署中的关联 key 使用相同 hash slot。要求调度 Redis 启用 AOF 且 `appendfsync always`，以保证同一节点进程崩溃后已确认的调度记录可恢复；若使用 `everysec`，必须明确接受最近约一秒写入丢失的风险，不能称为本设计的可靠延时配置。
- **RabbitMQ：**使用固定 TTL 的 quorum queue 层级，消息到期后通过配置为至少一次的 DLX 到达中转队列。源队列须配置 `dead-letter-strategy=at-least-once`、`overflow=reject-publish` 和可路由的 DLX，并满足 RabbitMQ 3.13 对 `stream_queue` feature flag 的要求；目标队列须持久化。中转 worker 检查绝对 `dueAt`，再选择下一层或发布到目标队列；目标 publisher confirm 后才 ACK 中转消息。固定 TTL 层级避免每条消息不同 TTL 造成队头阻塞。延时误差、最长支持时长、层级数量是模块配置并在集成测试中验证。插件作为对比方案记录，不是默认实现。
- **Kafka：**`delay/redis/v2` 接收任意核心 `Publisher`；调度先在 Redis 保存消息与到期时间，worker 到期后发布 Kafka，等 Kafka 确认再完成 Redis 任务。普通 Kafka 发布不接触 Redis；调度器崩溃后的租约恢复可能造成重复，不得丢失已经持久化的调度记录。调度 Redis 使用上述 AOF 配置，可与普通消息后端分开部署以隔离刷盘成本。Redis 故障切换的限制同上。
- **Memory：**仅保证同一进程存活期间的到期处理，文档和类型说明明确其测试用途。

“可靠延时”在本设计中表示：调度成功后，应用进程重启不会使消息消失；到期后最终会尝试投递，允许重复和迟到。它不表示单节点故障下的可用性、绝对准点或 Redis 异步故障切换时的零数据丢失。

## 性能与观测

普通消息热路径不创建每条消息一个 goroutine，不做强制 JSON/MsgPack 往返，不隐式初始化延时调度器或 OTel。各 adapter 保留必要的客户端批量、压缩、预取和连接复用设置；配置项由所属模块负责，不能暴露“设置了但未生效”的字段。默认发布以确认成功为准；若提供异步发布，它必须是独立接口，明确队列上限与结果通知。

公共观测事件包括发布确认耗时、失败与结果未知、处理耗时、重试、死信、在途数、背压等待和队列积压。Kafka 上报 lag 与最老未处理消息年龄；Redis 和 RabbitMQ 在后端支持的范围内上报待处理/未确认量。消息 ID、trace ID、错误文本不得作为指标标签。可观测性桥接在独立模块，核心仅保留轻量的可选钩子。

积压恢复验收同时比较输入速率与成功处理速率。持续输入高于下游处理能力时，系统必须施加背压或在 broker 保留期/容量内积压，不能宣称 MQ 包装层能消除容量缺口。领域事件测试验证多订阅方、重复处理和失败隔离；trace 测试验证持续流量、批量消费和积压追赶，不把 ClickHouse 驱动放进核心。

## 验收与测试

1. **模块依赖：**主 `go.mod` 的依赖图不包含 Redis、AMQP、Kafka、MsgPack、Zap、OTel。接入方只导入一个 adapter 时不拉取另外两个 broker 客户端。
2. **契约测试：**所有生产 adapter 通过发布确认、多个订阅方、同组竞争、处理失败重投、死信先写后确认、批量部分结果、取消、排空和重启恢复测试。Memory 通过适用的非持久化契约。
3. **故障注入：**分别模拟 handler 成功但确认失败、broker 断连、死信目标不可用、Kafka 重平衡、Redis pending 接管、延时 worker 在目标确认前后崩溃；已确认但未处理的消息不静默消失，允许可解释的重复。
4. **版本矩阵：**Kafka 至少在 4.0.x 和一个更新的 4.x 小版本上集成测试，RabbitMQ 在 3.13.3 上集成测试；Redis 在 7.2 和 8.0 上集成测试，并分别覆盖普通消费及可靠延时所需的 AOF 配置。CI 的 broker 配置须与可靠性断言一致。
5. **性能与有界性：**在同等确认、持久化、压缩、批次和并发配置下，与直接使用相同 Go 客户端的基线比较吞吐、P95 延迟与分配量；正常路径目标为达到直接客户端中位吞吐的至少 80%。如达不到，定位并优化包装开销后再发布。下游暂停期间内存须保持在配置的在途字节上限附近，且无消息静默丢弃。
6. **迁移体验：**README 提供 v1→v2 迁移表、每个 adapter 的最小示例、领域事件双订阅示例、批量 trace 示例、延时接入示例、单节点与集群可靠性说明。旧 API 不做兼容 shim。

## 工作包与交付顺序

1. **核心契约和多模块骨架：**固定消息、发布、订阅、批量和延时接口；建立契约测试工具与 Memory 测试实现；验证主模块零 broker 依赖。这一工作包先独立实施。
2. **普通后端：**Redis Streams、RabbitMQ 3.13 quorum、Kafka 4.x 各自独立实现并通过相同契约测试；每个后端单独做故障和性能测试。
3. **延时能力：**Redis 原生调度、RabbitMQ TTL/DLX、可选 Redis 调度器组合 Kafka；各自验证进程重启和确认窗口。
4. **观测、文档与发布：**可选 OTel 模块、迁移指南、示例、CI 矩阵、根模块与子模块分别打 v2 标签。

后续工作包可以各有内部设计文档，但不得改变本文的公共语义；若发现后端能力无法满足公共契约，先修订本文并取得审阅确认。

## 依据

- [Go Modules：主版本路径与多模块发布](https://go.dev/ref/mod)
- [Kafka 4.x 升级说明](https://kafka.apache.org/41/getting-started/upgrade/)：Kafka 4.0 仅支持 KRaft，并移除了旧协议版本。
- [franz-go 项目说明](https://github.com/twmb/franz-go)：客户端声明支持 Kafka 4.x，具备消费者组与幂等生产能力；具体可靠性仍需本项目故障测试验证。
- [franz-go v1.21.6 模块要求](https://raw.githubusercontent.com/twmb/franz-go/v1.21.6/go.mod)：Kafka 子模块需要 Go 1.25。
- [RabbitMQ 3.13 quorum queue 文档](https://www.rabbitmq.com/docs/3.13/quorum-queues)：publisher confirms、quorum 持久性、至少一次 dead-letter 和资源边界。
- [Redis 复制文档](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)：异步复制切换不能提供已确认写入的零丢失保证。
- [Redis 持久化文档](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)：`appendfsync everysec` 与 `always` 的丢失窗口和成本差异。
- [ClickHouse 写入建议](https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics)：批量或可确认的异步写入是 trace 验收场景的下游设置，不是 MQ 核心依赖。
