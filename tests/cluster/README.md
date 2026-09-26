# v2 集群故障测试

在仓库根目录运行：

```sh
bash tests/cluster/run.sh all
bash tests/cluster/run.sh kafka 4.0.2
bash tests/cluster/run.sh kafka 4.1.2
bash tests/cluster/run.sh rabbitmq
bash tests/cluster/run.sh redis 7.2
bash tests/cluster/run.sh redis 8.0

# 健康集群的短时间确认发布吞吐对照
bash tests/cluster/run.sh bench
bash tests/cluster/run.sh bench kafka 4.0.2
bash tests/cluster/run.sh bench rabbitmq
bash tests/cluster/run.sh bench redis 7.2

# 延长采样并重复 3 次
MQ_CLUSTER_BENCHTIME=10s MQ_CLUSTER_BENCH_COUNT=3 bash tests/cluster/run.sh bench kafka 4.1.2
```

需要 Go 1.25、Docker CLI/daemon 和本地镜像，首次运行可由 Docker 拉取镜像。测试在本机逐个建立一次性集群，使用固定本机端口：Kafka `19092`–`19094`，RabbitMQ AMQP `5672`–`5674` 与管理接口 `15672`–`15674`，Redis `7001`–`7006`。端口被占用时会失败。脚本只删除本次带 `mq.v2.cluster.run` 标签的容器、匿名数据卷及专属 Docker 网络。不要把集群测试环境变量指向长期运行的 broker。

| 后端 | 拓扑与故障 | 通过条件 |
| --- | --- | --- |
| Kafka 4.0.2/4.1.2 | 三节点 KRaft，单分区 topic 的副本数 3、`min.insync.replicas=2`；确认 3 个 ISR 后杀分区 leader | 继续确认发布；消费到故障前后每个消息 ID，允许重复 |
| RabbitMQ 3.13.3 | 三节点 quorum；确认队列有 3 个成员后杀队列 leader | 用存活节点重建连接；普通消息及 TTL/DLX 延时消息继续确认并投递 |
| Redis 7.2/8.0 | 三主三从，AOF `appendfsync always`；先从副本读到消息或调度记录，再杀对应主节点 | 副本晋升后继续确认写入；普通消息和到期任务仍可处理 |

RabbitMQ adapter 使用调用方提供的 AMQP 连接。节点故障导致连接或 `Run` 返回错误时，应用需要重建连接、adapter 和延时 worker，并监督重试。集群测试等待 quorum 队列重新选主后启动新的 worker。

Redis 复制是异步的。这里先确认副本实际持有记录，再注入故障，因此验证的是**已经复制的记录**在切换后的恢复；它不证明刚被主节点确认、尚未复制的写入零丢失。测试中的 go-redis Dialer 将 Docker 网络内的节点地址映射到本机端口，仅用于本机测试。

`bench` 在健康集群运行现有的 256 条 × 1 KiB 确认发布基准，默认目标采样时长 2 秒、重复 1 次，对照 adapter 与直接客户端；Kafka 使用单分区、三个副本和 `min.insync.replicas=2`，RabbitMQ 使用一个三副本 quorum 队列，Redis 使用六节点 ClusterClient 上的一个 Stream key 和 AOF `appendfsync always`。输出的 `MB/s` 以有效载荷计，只覆盖单分区、单队列或单 key 的发布确认；它不能代表多分区并行聚合吞吐，也不是消费吞吐、积压追赶速度、延时调度吞吐或故障中的持续负载结果。基准运行时不要并行运行本机其他重负载程序。

所有容器都运行在同一台 Docker 主机。故障测试覆盖一个 broker 节点进程故障，不覆盖主机宕机、网络分区或同时丢失多数节点。现有单节点版本矩阵在 [v2 工作流](../../.github/workflows/v2.yml)；集群测试和基准通过手动触发 [集群工作流](../../.github/workflows/v2-cluster.yml) 运行。

当前本机运行记录见 [2026-09-27 结果](results-2026-09-27.md)。
