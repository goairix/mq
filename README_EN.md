# MQ v2

The breaking v2 rewrite is in progress on the `v2` branch. The root module currently provides a dependency-free message model and transport interfaces; production adapters have not been published yet.

See the [v2 architecture design](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md). The previous API remains available from the `main` branch and v1 tags.

Core module: `github.com/goairix/mq/v2`. Applications will import only the Redis, RabbitMQ, or Kafka adapter modules they need when those are released.
