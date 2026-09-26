package otelmq

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type capturePublisher struct {
	message mq.Message
	err     error
	closed  bool
}

func (p *capturePublisher) Publish(_ context.Context, m mq.Message) error {
	p.message = m
	return p.err
}
func (p *capturePublisher) Close(context.Context) error { p.closed = true; return nil }

type contextPublisher struct{ contextError error }

func (p *contextPublisher) Publish(ctx context.Context, _ mq.Message) error {
	p.contextError = ctx.Err()
	return ctx.Err()
}
func (*contextPublisher) Close(context.Context) error { return nil }

type captureSubscriber struct{ message mq.Message }

func (s captureSubscriber) Run(ctx context.Context, _ mq.Subscription, handler mq.Handler) error {
	return handler(ctx, s.message)
}

type captureBatchPublisher struct {
	messages []mq.Message
	results  []mq.PublishResult
}

func (p *captureBatchPublisher) PublishBatch(_ context.Context, messages []mq.Message) []mq.PublishResult {
	p.messages = messages
	return p.results
}

type captureBatchSubscriber struct {
	messages []mq.Message
	results  []error
}

func (s *captureBatchSubscriber) RunBatch(ctx context.Context, _ mq.Subscription, _ mq.BatchOptions, handler mq.BatchHandler) error {
	results, err := handler(ctx, s.messages)
	s.results = results
	return err
}

type captureScheduler struct {
	message mq.Message
	due     time.Time
}

func (s *captureScheduler) PublishAt(_ context.Context, message mq.Message, due time.Time) error {
	s.message, s.due = message, due
	return nil
}

func testInstrumentation(t *testing.T) (*Instrumentation, *tracetest.InMemoryExporter, *metric.ManualReader, *sdktrace.TracerProvider) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	reader := metric.NewManualReader()
	meter := metric.NewMeterProvider(metric.WithReader(reader))
	i, err := New(Options{TracerProvider: tracer, MeterProvider: meter})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()); _ = meter.Shutdown(context.Background()) })
	return i, exporter, reader, tracer
}

func TestPublisherInjectsTraceWithoutMutatingCallerMessage(t *testing.T) {
	i, exporter, reader, tracer := testInstrumentation(t)
	target := &capturePublisher{}
	wrapped := i.Publisher(target)
	m, _ := mq.NewMessage("order.created", []byte("payload"))
	m.Headers = map[string]string{"schema": "v1"}
	ctx, parent := tracer.Tracer("test").Start(context.Background(), "parent")
	if err := wrapped.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	parent.End()
	if m.Headers["traceparent"] != "" || target.message.Headers["traceparent"] == "" {
		t.Fatalf("trace injection changed caller headers: input=%v output=%v", m.Headers, target.message.Headers)
	}
	if target.message.Headers["schema"] != "v1" {
		t.Fatal("business header lost")
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Name != "mq.publish" || spans[0].Parent.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("unexpected spans: %+v", spans)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.ScopeMetrics) == 0 || len(data.ScopeMetrics[0].Metrics) == 0 {
		t.Fatal("missing publisher metrics")
	}
	if err := wrapped.Close(context.Background()); err != nil || !target.closed {
		t.Fatalf("close delegation: %v, closed=%t", err, target.closed)
	}
}

func TestSubscriberExtractsParentAndPreservesHandlerError(t *testing.T) {
	i, exporter, _, tracer := testInstrumentation(t)
	m, _ := mq.NewMessage("order.created", []byte("payload"))
	publisher := &capturePublisher{}
	ctx, parent := tracer.Tracer("test").Start(context.Background(), "parent")
	if err := i.Publisher(publisher).Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	parent.End()
	want := errors.New("retry")
	var handlerSpan trace.SpanContext
	err := i.Subscriber(captureSubscriber{message: publisher.message}).Run(context.Background(), mq.Subscription{Topic: m.Topic, Name: "billing"}, func(ctx context.Context, got mq.Message) error {
		handlerSpan = trace.SpanFromContext(ctx).SpanContext()
		if got.ID != m.ID {
			t.Fatalf("message changed: %s", got.ID)
		}
		return want
	})
	if !errors.Is(err, want) || !handlerSpan.IsValid() || handlerSpan.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("handler error=%v span=%v", err, handlerSpan)
	}
	spans := exporter.GetSpans()
	if len(spans) != 3 || spans[2].Name != "mq.consume" || spans[2].Parent.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("unexpected consume span: %+v", spans)
	}
}

func TestPublisherPreservesUnknownOutcome(t *testing.T) {
	i, _, _, _ := testInstrumentation(t)
	want := mq.OutcomeUnknown(errors.New("confirm lost"))
	target := &capturePublisher{err: want}
	m, _ := mq.NewMessage("trace", []byte("payload"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := i.Publisher(target).Publish(ctx, m); !errors.Is(err, want) || !mq.IsOutcomeUnknown(err) {
		t.Fatalf("unknown outcome changed: %v", err)
	}
}

func TestPublisherForwardsCancellation(t *testing.T) {
	i, _, _, _ := testInstrumentation(t)
	target := &contextPublisher{}
	message, _ := mq.NewMessage("trace", []byte("payload"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := i.Publisher(target).Publish(ctx, message)
	if !errors.Is(err, context.Canceled) || !errors.Is(target.contextError, context.Canceled) {
		t.Fatalf("cancellation changed: wrapper=%v target=%v", err, target.contextError)
	}
}

func TestBatchPublisherPreservesResultOrderAndCallerHeaders(t *testing.T) {
	i, _, _, tracer := testInstrumentation(t)
	target := &captureBatchPublisher{results: []mq.PublishResult{{State: mq.PublishAccepted}, {State: mq.PublishRejected, Err: errors.New("invalid")}}}
	first, _ := mq.NewMessage("orders", []byte("first"))
	second, _ := mq.NewMessage("orders", []byte("second"))
	first.Headers = map[string]string{"schema": "v1"}
	ctx, span := tracer.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	results := i.BatchPublisher(target).PublishBatch(ctx, []mq.Message{first, second})
	if len(results) != 2 || results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected {
		t.Fatalf("batch results changed: %+v", results)
	}
	if first.Headers["traceparent"] != "" || target.messages[0].Headers["traceparent"] == "" || target.messages[1].Headers["traceparent"] == "" {
		t.Fatalf("batch trace propagation failed: input=%v output=%v", first.Headers, target.messages)
	}
}

func TestEmptyBatchPublisherStillRecordsOneDuration(t *testing.T) {
	i, _, reader, _ := testInstrumentation(t)
	target := &captureBatchPublisher{}
	results := i.BatchPublisher(target).PublishBatch(context.Background(), nil)
	if len(results) != 0 || histogramCount(t, reader, "mq.publish.duration") != 1 {
		t.Fatalf("empty batch results=%v duration count=%d", results, histogramCount(t, reader, "mq.publish.duration"))
	}
}

func TestMultiTopicAcceptedBatchDurationIsAccepted(t *testing.T) {
	i, _, reader, _ := testInstrumentation(t)
	target := &captureBatchPublisher{results: []mq.PublishResult{{State: mq.PublishAccepted}, {State: mq.PublishAccepted}}}
	first, _ := mq.NewMessage("orders", []byte("one"))
	second, _ := mq.NewMessage("traces", []byte("two"))
	i.BatchPublisher(target).PublishBatch(context.Background(), []mq.Message{first, second})
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name != "mq.publish.duration" {
				continue
			}
			histogram, ok := recorded.Data.(metricdata.Histogram[float64])
			if !ok || len(histogram.DataPoints) != 1 {
				t.Fatalf("unexpected duration data: %+v", recorded.Data)
			}
			attrs := histogram.DataPoints[0].Attributes
			if value, _ := attrs.Value(attribute.Key("topic")); value.AsString() != "__mixed__" {
				t.Fatalf("duration topic = %q", value.AsString())
			}
			if value, _ := attrs.Value(attribute.Key("outcome")); value.AsString() != "accepted" {
				t.Fatalf("duration outcome = %q, want accepted", value.AsString())
			}
			return
		}
	}
	t.Fatal("missing batch publish duration")
}

func TestBatchSubscriberLinksDistinctMessageTraces(t *testing.T) {
	i, exporter, reader, tracer := testInstrumentation(t)
	first, _ := mq.NewMessage("traces", []byte("one"))
	second, _ := mq.NewMessage("traces", []byte("two"))
	firstTarget, secondTarget := &capturePublisher{}, &capturePublisher{}
	ctx1, parent1 := tracer.Tracer("test").Start(context.Background(), "first-parent")
	if err := i.Publisher(firstTarget).Publish(ctx1, first); err != nil {
		t.Fatal(err)
	}
	parent1.End()
	ctx2, parent2 := tracer.Tracer("test").Start(context.Background(), "second-parent")
	if err := i.Publisher(secondTarget).Publish(ctx2, second); err != nil {
		t.Fatal(err)
	}
	parent2.End()
	want := errors.New("retry second")
	sub := &captureBatchSubscriber{messages: []mq.Message{firstTarget.message, secondTarget.message}}
	var gotSpan trace.SpanContext
	err := i.BatchSubscriber(sub).RunBatch(context.Background(), mq.Subscription{Topic: "traces", Name: "clickhouse"}, mq.DefaultBatchOptions(), func(ctx context.Context, messages []mq.Message) ([]error, error) {
		gotSpan = trace.SpanFromContext(ctx).SpanContext()
		return []error{nil, want}, nil
	})
	if err != nil || !gotSpan.IsValid() || len(sub.results) != 2 || !errors.Is(sub.results[1], want) {
		t.Fatalf("batch handler result changed: err=%v span=%v results=%v", err, gotSpan, sub.results)
	}
	spans := exporter.GetSpans()
	if len(spans) != 5 || spans[4].Name != "mq.consume.batch" || len(spans[4].Links) != 2 {
		t.Fatalf("batch links missing: %+v", spans)
	}
	if spans[4].Links[0].SpanContext.TraceID() != parent1.SpanContext().TraceID() || spans[4].Links[1].SpanContext.TraceID() != parent2.SpanContext().TraceID() {
		t.Fatalf("batch links have wrong traces: %+v", spans[4].Links)
	}
	if count := histogramCount(t, reader, "mq.consume.duration"); count != 1 {
		t.Fatalf("mixed batch recorded %d duration observations, want 1", count)
	}
}

func TestLargeBatchPreservesEveryTraceLink(t *testing.T) {
	i, exporter, _, _ := testInstrumentation(t)
	const count = 256
	messages := make([]mq.Message, count)
	for index := range messages {
		messages[index], _ = mq.NewMessage("traces", []byte("payload"))
		messages[index].Headers = make(map[string]string)
		spanID := trace.SpanID{1, 2, 3, 4, 5, 6, byte(index >> 8), byte(index)}
		sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, SpanID: spanID, TraceFlags: trace.FlagsSampled})
		propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(context.Background(), sc), propagation.MapCarrier(messages[index].Headers))
	}
	sub := &captureBatchSubscriber{messages: messages}
	if err := i.BatchSubscriber(sub).RunBatch(context.Background(), mq.Subscription{Topic: "traces", Name: "sink"}, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	linked := 0
	for _, span := range exporter.GetSpans() {
		linked += len(span.Links)
	}
	if linked != count {
		t.Fatalf("linked %d of %d incoming traces", linked, count)
	}
}

func TestInvalidBatchResultMarksSpanError(t *testing.T) {
	i, exporter, _, _ := testInstrumentation(t)
	m, _ := mq.NewMessage("traces", []byte("payload"))
	sub := &captureBatchSubscriber{messages: []mq.Message{m}}
	if err := i.BatchSubscriber(sub).RunBatch(context.Background(), mq.Subscription{Topic: "traces", Name: "sink"}, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { return []error{}, nil }); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("invalid batch span status = %+v", spans)
	}
}

func histogramCount(t *testing.T, reader *metric.ManualReader, name string) uint64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	var count uint64
	for _, scope := range data.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name == name {
				if histogram, ok := recorded.Data.(metricdata.Histogram[float64]); ok {
					for _, point := range histogram.DataPoints {
						count += point.Count
					}
				}
			}
		}
	}
	return count
}

func TestScheduledPublisherPreservesDueTimeAndHeaders(t *testing.T) {
	i, _, _, tracer := testInstrumentation(t)
	target := &captureScheduler{}
	m, _ := mq.NewMessage("expire", []byte("payload"))
	due := time.Now().Add(time.Hour)
	ctx, span := tracer.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	if err := i.ScheduledPublisher(target).PublishAt(ctx, m, due); err != nil {
		t.Fatal(err)
	}
	if !target.due.Equal(due) || target.message.Headers["traceparent"] == "" || m.Headers != nil {
		t.Fatalf("schedule changed: due=%s headers=%v", target.due, target.message.Headers)
	}
}
