// Package otelmq adds optional OpenTelemetry instrumentation around MQ v2
// interfaces. Importing it is never required by the core or broker modules.
package otelmq

import (
	"context"
	"time"

	mq "github.com/goairix/mq/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
}

type Instrumentation struct {
	tracer           trace.Tracer
	propagator       propagation.TextMapPropagator
	publishMessages  metric.Int64Counter
	publishPayload   metric.Int64Counter
	publishSeconds   metric.Float64Histogram
	consumeMessages  metric.Int64Counter
	consumePayload   metric.Int64Counter
	consumeSeconds   metric.Float64Histogram
	consumeInflight  metric.Int64UpDownCounter
	consumeOldestAge metric.Float64Histogram
}

func New(options Options) (*Instrumentation, error) {
	if options.TracerProvider == nil {
		options.TracerProvider = otel.GetTracerProvider()
	}
	if options.MeterProvider == nil {
		options.MeterProvider = otel.GetMeterProvider()
	}
	if options.Propagator == nil {
		options.Propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	meter := options.MeterProvider.Meter("github.com/goairix/mq/observability/otel/v2")
	publishMessages, err := meter.Int64Counter("mq.publish.messages", metric.WithDescription("MQ messages reported by publish calls, grouped by outcome"))
	if err != nil {
		return nil, err
	}
	publishPayload, err := meter.Int64Counter("mq.publish.payload", metric.WithUnit("By"), metric.WithDescription("Payload bytes reported by publish calls, excluding envelope overhead"))
	if err != nil {
		return nil, err
	}
	durationBuckets := metric.WithExplicitBucketBoundaries(0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
	publishSeconds, err := meter.Float64Histogram("mq.publish.duration", metric.WithUnit("s"), metric.WithDescription("Duration of one publish call"), durationBuckets)
	if err != nil {
		return nil, err
	}
	consumeMessages, err := meter.Int64Counter("mq.consume.messages", metric.WithDescription("MQ message processing attempts grouped by handler outcome"))
	if err != nil {
		return nil, err
	}
	consumePayload, err := meter.Int64Counter("mq.consume.payload", metric.WithUnit("By"), metric.WithDescription("Payload bytes processed by handlers, including retries"))
	if err != nil {
		return nil, err
	}
	consumeSeconds, err := meter.Float64Histogram("mq.consume.duration", metric.WithUnit("s"), metric.WithDescription("Duration of one handler call"), durationBuckets)
	if err != nil {
		return nil, err
	}
	consumeInflight, err := meter.Int64UpDownCounter("mq.consume.inflight", metric.WithDescription("MQ messages currently in active handlers"))
	if err != nil {
		return nil, err
	}
	consumeOldestAge, err := meter.Float64Histogram("mq.consume.oldest_age", metric.WithUnit("s"), metric.WithDescription("Age of the oldest message when a handler starts"), metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400))
	if err != nil {
		return nil, err
	}
	return &Instrumentation{
		tracer:           options.TracerProvider.Tracer("github.com/goairix/mq/observability/otel/v2"),
		propagator:       options.Propagator,
		publishMessages:  publishMessages,
		publishPayload:   publishPayload,
		publishSeconds:   publishSeconds,
		consumeMessages:  consumeMessages,
		consumePayload:   consumePayload,
		consumeSeconds:   consumeSeconds,
		consumeInflight:  consumeInflight,
		consumeOldestAge: consumeOldestAge,
	}, nil
}

func (i *Instrumentation) Publisher(target mq.Publisher) mq.Publisher {
	return observedPublisher{instrumentation: i, target: target}
}

func (i *Instrumentation) BatchPublisher(target mq.BatchPublisher) mq.BatchPublisher {
	return observedBatchPublisher{instrumentation: i, target: target}
}

func (i *Instrumentation) Subscriber(target mq.Subscriber) mq.Subscriber {
	return observedSubscriber{instrumentation: i, target: target}
}

func (i *Instrumentation) BatchSubscriber(target mq.BatchSubscriber) mq.BatchSubscriber {
	return observedBatchSubscriber{instrumentation: i, target: target}
}

func (i *Instrumentation) ScheduledPublisher(target mq.ScheduledPublisher) mq.ScheduledPublisher {
	return observedScheduledPublisher{instrumentation: i, target: target}
}

func cloneWithTrace(ctx context.Context, propagator propagation.TextMapPropagator, message mq.Message) mq.Message {
	cp := message
	cp.Headers = make(map[string]string, len(message.Headers)+2)
	for key, value := range message.Headers {
		cp.Headers[key] = value
	}
	propagator.Inject(ctx, propagation.MapCarrier(cp.Headers))
	return cp
}

func (i *Instrumentation) extract(ctx context.Context, message mq.Message) context.Context {
	return i.propagator.Extract(ctx, propagation.MapCarrier(message.Headers))
}

func recordSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func publishResult(err error) string {
	if err == nil {
		return "accepted"
	}
	if mq.IsOutcomeUnknown(err) {
		return "unknown"
	}
	return "rejected"
}

func consumeResult(err error) string {
	if err == nil {
		return "success"
	}
	if mq.IsPermanent(err) {
		return "permanent"
	}
	return "retry"
}

func (i *Instrumentation) recordPublish(ctx context.Context, operation, topic, outcome string, count, payloadBytes int, duration time.Duration) {
	i.recordPublishCount(ctx, operation, topic, outcome, count, payloadBytes)
	i.recordPublishDuration(ctx, operation, topic, outcome, duration)
}

func (i *Instrumentation) recordPublishCount(ctx context.Context, operation, topic, outcome string, count, payloadBytes int) {
	attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("topic", topic), attribute.String("outcome", outcome))
	i.publishMessages.Add(ctx, int64(count), attrs)
	i.publishPayload.Add(ctx, int64(payloadBytes), attrs)
}

func (i *Instrumentation) recordPublishDuration(ctx context.Context, operation, topic, outcome string, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("topic", topic), attribute.String("outcome", outcome))
	i.publishSeconds.Record(ctx, duration.Seconds(), attrs)
}

func (i *Instrumentation) recordConsume(ctx context.Context, sub mq.Subscription, outcome string, count, payloadBytes int, duration time.Duration) {
	i.recordConsumeCount(ctx, sub, outcome, count, payloadBytes)
	i.recordConsumeDuration(ctx, sub, outcome, duration)
}

func (i *Instrumentation) recordConsumeCount(ctx context.Context, sub mq.Subscription, outcome string, count, payloadBytes int) {
	attrs := metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name), attribute.String("outcome", outcome))
	i.consumeMessages.Add(ctx, int64(count), attrs)
	i.consumePayload.Add(ctx, int64(payloadBytes), attrs)
}

func (i *Instrumentation) recordConsumeStart(ctx context.Context, sub mq.Subscription, count int, oldest, start time.Time) {
	if count <= 0 {
		return
	}
	attrs := metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name))
	i.consumeInflight.Add(ctx, int64(count), attrs)
	if !oldest.IsZero() && !oldest.After(start) {
		i.consumeOldestAge.Record(ctx, start.Sub(oldest).Seconds(), attrs)
	}
}

func (i *Instrumentation) recordConsumeFinish(ctx context.Context, sub mq.Subscription, count int) {
	if count > 0 {
		i.consumeInflight.Add(ctx, -int64(count), metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name)))
	}
}

func (i *Instrumentation) recordConsumeDuration(ctx context.Context, sub mq.Subscription, outcome string, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name), attribute.String("outcome", outcome))
	i.consumeSeconds.Record(ctx, duration.Seconds(), attrs)
}
