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
	tracer          trace.Tracer
	propagator      propagation.TextMapPropagator
	publishMessages metric.Int64Counter
	publishSeconds  metric.Float64Histogram
	consumeMessages metric.Int64Counter
	consumeSeconds  metric.Float64Histogram
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
	publishMessages, err := meter.Int64Counter("mq.publish.messages")
	if err != nil {
		return nil, err
	}
	publishSeconds, err := meter.Float64Histogram("mq.publish.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	consumeMessages, err := meter.Int64Counter("mq.consume.messages")
	if err != nil {
		return nil, err
	}
	consumeSeconds, err := meter.Float64Histogram("mq.consume.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &Instrumentation{
		tracer:          options.TracerProvider.Tracer("github.com/goairix/mq/observability/otel/v2"),
		propagator:      options.Propagator,
		publishMessages: publishMessages,
		publishSeconds:  publishSeconds,
		consumeMessages: consumeMessages,
		consumeSeconds:  consumeSeconds,
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

func (i *Instrumentation) recordPublish(ctx context.Context, operation, topic, outcome string, count int, duration time.Duration) {
	i.recordPublishCount(ctx, operation, topic, outcome, count)
	i.recordPublishDuration(ctx, operation, topic, outcome, duration)
}

func (i *Instrumentation) recordPublishCount(ctx context.Context, operation, topic, outcome string, count int) {
	attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("topic", topic), attribute.String("outcome", outcome))
	i.publishMessages.Add(ctx, int64(count), attrs)
}

func (i *Instrumentation) recordPublishDuration(ctx context.Context, operation, topic, outcome string, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("topic", topic), attribute.String("outcome", outcome))
	i.publishSeconds.Record(ctx, duration.Seconds(), attrs)
}

func (i *Instrumentation) recordConsume(ctx context.Context, sub mq.Subscription, outcome string, count int, duration time.Duration) {
	i.recordConsumeCount(ctx, sub, outcome, count)
	i.recordConsumeDuration(ctx, sub, outcome, duration)
}

func (i *Instrumentation) recordConsumeCount(ctx context.Context, sub mq.Subscription, outcome string, count int) {
	attrs := metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name), attribute.String("outcome", outcome))
	i.consumeMessages.Add(ctx, int64(count), attrs)
}

func (i *Instrumentation) recordConsumeDuration(ctx context.Context, sub mq.Subscription, outcome string, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name), attribute.String("outcome", outcome))
	i.consumeSeconds.Record(ctx, duration.Seconds(), attrs)
}
