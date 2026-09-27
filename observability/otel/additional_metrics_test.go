package otelmq

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func collectedMetric(t *testing.T, reader *metric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			if recorded.Name == name {
				return recorded
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func labelsMatch(set attribute.Set, labels map[string]string) bool {
	for key, want := range labels {
		got, ok := set.Value(attribute.Key(key))
		if !ok || got.AsString() != want {
			return false
		}
	}
	return true
}

func sumPoint(t *testing.T, reader *metric.ManualReader, name string, labels map[string]string) int64 {
	t.Helper()
	data, ok := collectedMetric(t, reader, name).Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 sum", name)
	}
	for _, point := range data.DataPoints {
		if labelsMatch(point.Attributes, labels) {
			return point.Value
		}
	}
	t.Fatalf("metric %q has no point with labels %v", name, labels)
	return 0
}

func histogramPoint(t *testing.T, reader *metric.ManualReader, name string, labels map[string]string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	data, ok := collectedMetric(t, reader, name).Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q is not a float64 histogram", name)
	}
	for _, point := range data.DataPoints {
		if labelsMatch(point.Attributes, labels) {
			return point
		}
	}
	t.Fatalf("metric %q has no point with labels %v", name, labels)
	return metricdata.HistogramDataPoint[float64]{}
}

func TestPublishPayloadBytesMatchMessageOutcomes(t *testing.T) {
	i, _, reader, _ := testInstrumentation(t)
	first, _ := mq.NewMessage("orders", []byte("one"))
	second, _ := mq.NewMessage("orders", []byte("four"))
	third, _ := mq.NewMessage("orders", []byte("three"))
	if err := i.Publisher(&capturePublisher{}).Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	batch := &captureBatchPublisher{results: []mq.PublishResult{{State: mq.PublishAccepted}, {State: mq.PublishRejected, Err: errors.New("rejected")}}}
	i.BatchPublisher(batch).PublishBatch(context.Background(), []mq.Message{second, third})
	if err := i.ScheduledPublisher(&captureScheduler{}).PublishAt(context.Background(), first, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		operation, outcome string
		want               int64
	}{
		{"publish", "accepted", 3},
		{"publish_batch", "accepted", 4},
		{"publish_batch", "rejected", 5},
		{"schedule", "accepted", 3},
	} {
		labels := map[string]string{"operation": test.operation, "topic": "orders", "outcome": test.outcome}
		if got := sumPoint(t, reader, "mq.publish.payload", labels); got != test.want {
			t.Errorf("%s/%s payload bytes = %d, want %d", test.operation, test.outcome, got, test.want)
		}
	}
}

func TestConsumePayloadAgeAndInflight(t *testing.T) {
	i, _, reader, _ := testInstrumentation(t)
	message, _ := mq.NewMessage("traces", []byte("one"))
	message.CreatedAt = time.Now().Add(-2 * time.Second)
	sub := mq.Subscription{Topic: "traces", Name: "single"}
	if err := i.Subscriber(captureSubscriber{message: message}).Run(context.Background(), sub, func(context.Context, mq.Message) error {
		if got := sumPoint(t, reader, "mq.consume.inflight", map[string]string{"topic": "traces", "subscription": "single"}); got != 1 {
			t.Fatalf("single inflight = %d, want 1", got)
		}
		return errors.New("retry")
	}); err == nil {
		t.Fatal("handler error was lost")
	}
	if got := sumPoint(t, reader, "mq.consume.inflight", map[string]string{"topic": "traces", "subscription": "single"}); got != 0 {
		t.Errorf("single inflight after handler = %d, want 0", got)
	}
	if got := sumPoint(t, reader, "mq.consume.payload", map[string]string{"topic": "traces", "subscription": "single", "outcome": "retry"}); got != 3 {
		t.Errorf("single payload bytes = %d, want 3", got)
	}
	age := histogramPoint(t, reader, "mq.consume.oldest_age", map[string]string{"topic": "traces", "subscription": "single"})
	if age.Count != 1 || age.Sum < 1.5 || age.Sum > 3 {
		t.Errorf("single oldest age count=%d sum=%f, want one observation near 2s", age.Count, age.Sum)
	}

	newer, _ := mq.NewMessage("traces", []byte("four"))
	newer.CreatedAt = time.Now().Add(-time.Second)
	older, _ := mq.NewMessage("traces", []byte("three"))
	older.CreatedAt = time.Now().Add(-5 * time.Second)
	batchSub := mq.Subscription{Topic: "traces", Name: "batch"}
	batch := &captureBatchSubscriber{messages: []mq.Message{newer, older}}
	if err := i.BatchSubscriber(batch).RunBatch(context.Background(), batchSub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) {
		if got := sumPoint(t, reader, "mq.consume.inflight", map[string]string{"topic": "traces", "subscription": "batch"}); got != 2 {
			t.Fatalf("batch inflight = %d, want 2", got)
		}
		return []error{nil, errors.New("retry")}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := sumPoint(t, reader, "mq.consume.inflight", map[string]string{"topic": "traces", "subscription": "batch"}); got != 0 {
		t.Errorf("batch inflight after handler = %d, want 0", got)
	}
	for _, test := range []struct {
		outcome string
		want    int64
	}{{"success", 4}, {"retry", 5}} {
		if got := sumPoint(t, reader, "mq.consume.payload", map[string]string{"topic": "traces", "subscription": "batch", "outcome": test.outcome}); got != test.want {
			t.Errorf("batch %s payload bytes = %d, want %d", test.outcome, got, test.want)
		}
	}
	age = histogramPoint(t, reader, "mq.consume.oldest_age", map[string]string{"topic": "traces", "subscription": "batch"})
	if age.Count != 1 || age.Sum < 4.5 || age.Sum > 6 {
		t.Errorf("batch oldest age count=%d sum=%f, want one observation near 5s", age.Count, age.Sum)
	}
}

func TestDurationHistogramsResolveMillisecondOperations(t *testing.T) {
	i, _, reader, _ := testInstrumentation(t)
	message, _ := mq.NewMessage("traces", []byte("payload"))
	if err := i.Publisher(&capturePublisher{}).Publish(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if err := i.Subscriber(captureSubscriber{message: message}).Run(context.Background(), mq.Subscription{Topic: "traces", Name: "sink"}, func(context.Context, mq.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mq.publish.duration", "mq.consume.duration"} {
		point := histogramPoint(t, reader, name, nil)
		if !slices.Contains(point.Bounds, 0.0005) {
			t.Errorf("%s bounds = %v, need a 0.5ms boundary", name, point.Bounds)
		}
	}
}
