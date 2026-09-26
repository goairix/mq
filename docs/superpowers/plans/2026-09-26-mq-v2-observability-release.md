# MQ v2 observability and release-readiness plan

Work on the user-designated long-lived `v2` branch. Do not push, tag, or publish without explicit authorization.

## Optional OpenTelemetry module

Create `github.com/goairix/mq/observability/otel/v2` as an independent Go module. Keep the root dependency graph standard-library-only. Implement decorators for `Publisher`, `BatchPublisher`, `Subscriber`, `BatchSubscriber`, and `ScheduledPublisher` where supported. Inject/extract W3C trace context through copied message headers; do not mutate caller-owned maps. Use one primary span per publish call and per consume callback. Batch consumption links incoming contexts because it has one handler context for many messages. For a batch with more than 64 valid incoming contexts, create short `mq.consume.batch.links` child spans for the remaining links so the default OTel SDK link limit does not silently truncate a 256-message batch; the primary consume span directly carries only the first 64 links. Emit low-cardinality counters/histograms for outcomes and one duration sample per call; never label by message ID, trace ID, payload, or error text. Tests must verify propagation, result preservation, cancellation, and metrics with the OTel SDK test readers. Internal broker retry and queue lag remain broker-native metrics; the decorator does not pretend to observe them.

## CI and documentation

Add a GitHub Actions matrix for root isolation, Memory, Redis 7.2/8.0 with AOF, RabbitMQ 3.13.3, and Kafka 4.0.2/4.1.2. Run each child module's tests and race checks under Go 1.25. Keep destructive broker restart tests in isolated jobs or opt-in local commands. Document v1-to-v2 migration, per-adapter construction, DDD event fanout, trace batch consumption, Kafka delay composition, HA boundaries, and release tags for child modules. Verify the examples compile where practical.

## Gate

Run all module tests with workspace and `GOWORK=off` root module isolation. Run broker-backed tests, race, vet, diff check, and code review; fix Critical/Important findings before claiming completion. Stop local disposable containers. Do not create v2 tags or push the branch as part of this work.
