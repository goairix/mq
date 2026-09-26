# Redis durable delay scheduler (v2)

`github.com/goairix/mq/delay/redis/v2` is an optional module. It stores delayed messages in Redis and later publishes them through any MQ v2 `Publisher`. The ordinary Redis and Kafka adapter paths do not import it.

The scheduler is being implemented on the `v2` branch. Use Redis AOF with `appendfsync always` for the stated single-node crash recovery guarantee. Asynchronous replica failover can still lose a recently acknowledged write.
