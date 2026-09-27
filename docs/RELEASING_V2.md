# v2 多模块发布清单

从 `v2` 分支上经过验证的同一提交，为根模块和各子模块创建版本标签。

## 发布前

1. 确认发布提交的 `.github/workflows/v2.yml` 全部任务通过，检查根模块零 broker 依赖、全部子模块的 race 测试和 broker 重启测试。
2. 对 Redis 7.2/8.0、RabbitMQ 3.13.3、Kafka 4.0.2/4.1.2 分别记录镜像摘要和验证结果。单节点重启测试只能验证恢复，集群故障切换需要另外验证。
3. 检查根模块及各子模块的 `go.mod`、`go.sum`，确认没有本地 `replace`。仓库 `go.work` 中的本地 `replace` 只用于尚未发布时的开发，不会进入消费者模块。
4. 为所有公共模块同步选择版本，例如 `v2.0.0`。子模块的 `go.mod` 已引用根模块 `v2.0.0`；若改用其他首发版本，先统一更新依赖与文档。

## 标签顺序

先在同一个经验证的提交上发布根模块标签 `v2.0.0`，再发布子模块标签：

```text
adapters/memory/v2.0.0
adapters/redis/v2.0.0
adapters/rabbitmq/v2.0.0
adapters/kafka/v2.0.0
delay/redis/v2.0.0
delay/rabbitmq/v2.0.0
observability/otel/v2.0.0
```

Go 多模块仓库使用**目录前缀 + 版本号**作为子模块标签；模块路径末尾的 `/v2` 不额外加到目录前缀中。各模块可在后续独立递增补丁或次版本，同时遵守它们对根模块的版本要求。

## 消费者验证

从仓库外的临时应用分别只引入根模块、Redis、RabbitMQ、Kafka、延时模块，运行 `go mod tidy`、`go list -m all` 和最小示例。确认只导入核心时没有 broker/OTel 客户端，且只导入一个普通 adapter 时不会拉取其他后端实现。
