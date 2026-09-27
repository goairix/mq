package otelmq

import (
	"context"
	"sync/atomic"
	"testing"

	mq "github.com/goairix/mq/v2"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type benchPublisher struct{}

var benchTransportCalls atomic.Uint64

func (benchPublisher) Publish(context.Context, mq.Message) error {
	benchTransportCalls.Add(1)
	return nil
}
func (benchPublisher) Close(context.Context) error { return nil }

type benchBatchPublisher struct{ results []mq.PublishResult }

func (p benchBatchPublisher) PublishBatch(context.Context, []mq.Message) []mq.PublishResult {
	benchTransportCalls.Add(1)
	return p.results
}

type benchSubscriber struct{ message mq.Message }

func (s benchSubscriber) Run(ctx context.Context, _ mq.Subscription, handler mq.Handler) error {
	benchTransportCalls.Add(1)
	return handler(ctx, s.message)
}

type benchBatchSubscriber struct{ messages []mq.Message }

func (s benchBatchSubscriber) RunBatch(ctx context.Context, _ mq.Subscription, _ mq.BatchOptions, handler mq.BatchHandler) error {
	benchTransportCalls.Add(1)
	_, err := handler(ctx, s.messages)
	return err
}

// BenchmarkInstrumentationOverhead isolates the decorator and SDK recording
// cost from broker and exporter latency. It does not measure end-to-end MQ.
func BenchmarkInstrumentationOverhead(b *testing.B) {
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	meter := metric.NewMeterProvider(metric.WithReader(metric.NewManualReader()))
	b.Cleanup(func() { _ = tracer.Shutdown(context.Background()); _ = meter.Shutdown(context.Background()) })
	instrumentation, err := New(Options{TracerProvider: tracer, MeterProvider: meter})
	if err != nil {
		b.Fatal(err)
	}
	ctx, parent := tracer.Tracer("benchmark").Start(context.Background(), "parent")
	defer parent.End()
	const batchSize = 256
	const payloadBytes = 1024
	messages := make([]mq.Message, batchSize)
	for index := range messages {
		messages[index], err = mq.NewMessage("benchmark", make([]byte, payloadBytes))
		if err != nil {
			b.Fatal(err)
		}
		messages[index].Headers = make(map[string]string)
		propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(messages[index].Headers))
	}
	results := make([]mq.PublishResult, batchSize)
	for index := range results {
		results[index].State = mq.PublishAccepted
	}
	sub := mq.Subscription{Topic: "benchmark", Name: "worker"}
	handler := func(context.Context, mq.Message) error { return nil }
	batchHandler := func(context.Context, []mq.Message) ([]error, error) { return nil, nil }
	batchOptions := mq.DefaultBatchOptions()
	singlePublisher := benchPublisher{}
	batchPublisher := benchBatchPublisher{results: results}
	singleSubscriber := benchSubscriber{message: messages[0]}
	batchSubscriber := benchBatchSubscriber{messages: messages}
	bench := func(name string, size int64, call func() error) {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := call(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	bench("publish/plain", payloadBytes, func() error { return singlePublisher.Publish(ctx, messages[0]) })
	observedPublisher := instrumentation.Publisher(singlePublisher)
	bench("publish/otel", payloadBytes, func() error { return observedPublisher.Publish(ctx, messages[0]) })
	bench("publish_batch/plain", batchSize*payloadBytes, func() error { _ = batchPublisher.PublishBatch(ctx, messages); return nil })
	observedBatchPublisher := instrumentation.BatchPublisher(batchPublisher)
	bench("publish_batch/otel", batchSize*payloadBytes, func() error { _ = observedBatchPublisher.PublishBatch(ctx, messages); return nil })
	bench("consume/plain", payloadBytes, func() error { return singleSubscriber.Run(ctx, sub, handler) })
	observedSubscriber := instrumentation.Subscriber(singleSubscriber)
	bench("consume/otel", payloadBytes, func() error { return observedSubscriber.Run(ctx, sub, handler) })
	bench("consume_batch/plain", batchSize*payloadBytes, func() error { return batchSubscriber.RunBatch(ctx, sub, batchOptions, batchHandler) })
	observedBatchSubscriber := instrumentation.BatchSubscriber(batchSubscriber)
	bench("consume_batch/otel", batchSize*payloadBytes, func() error { return observedBatchSubscriber.RunBatch(ctx, sub, batchOptions, batchHandler) })
}
