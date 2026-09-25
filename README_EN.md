# MQ v2

The breaking v2 rewrite is in progress on the `v2` branch. The root module `github.com/goairix/mq/v2` uses only the standard library and provides the message model, transport interfaces, and reusable contract tests. Production adapters have not been published yet.

See the [v2 architecture design](docs/superpowers/specs/2026-09-25-mq-v2-architecture-design.md). The previous API remains available from the `main` branch and v0.x tags.

The checked-in `go.work` connects local modules. Run `go test ./...` for the root module and `go test ./adapters/memory/...` for Memory. Use `GOWORK=off go list -m all` to inspect the root module's isolated dependency graph.

The test-only Memory adapter is a separate module at `github.com/goairix/mq/adapters/memory/v2`:

```go
broker, err := memory.New(128)
if err != nil { panic(err) }
defer broker.Close(context.Background())

msg, err := mq.NewMessage("order.created", []byte(`{"id":"123"}`))
if err != nil { panic(err) }
if err := broker.Publish(context.Background(), msg); err != nil { panic(err) }
```

Import `context`, the root module as `mq`, and the Memory module as `memory`. Memory messages and `PublishAt` schedules exist only in the current process and disappear on close or restart. Production applications will import only the Redis, RabbitMQ, or Kafka adapter modules they need when those are released.
