# MQ v2

Go 消息库的 v2 重构正在 `v2` 分支进行。当前已提供无第三方依赖的核心消息模型与接口；生产 adapter 尚未发布。

设计见 [v2 架构文档](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md)。旧版代码可在 `main` 分支和 v1 标签中获取。

核心模块：`github.com/goairix/mq/v2`。接入方将在后续版本中分别引入需要的 Redis、RabbitMQ 或 Kafka adapter 模块。
