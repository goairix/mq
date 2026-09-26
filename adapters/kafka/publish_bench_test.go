package kafkaadapter

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BenchmarkConfirmedBatch compares the adapter with direct franz-go using the
// same producer, topic, confirmation policy, payloads, and record envelope.
func BenchmarkConfirmedBatch(b *testing.B) {
	value := os.Getenv("MQ_TEST_KAFKA_BROKERS")
	topic := os.Getenv("MQ_TEST_KAFKA_TOPIC")
	if value == "" || topic == "" {
		b.Skip("set MQ_TEST_KAFKA_BROKERS and MQ_TEST_KAFKA_TOPIC")
	}
	brokers := strings.Split(value, ",")
	a, err := New(brokers, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close(context.Background())
	messages := make([]mq.Message, 256)
	for i := range messages {
		messages[i], err = mq.NewMessage(topic, make([]byte, 1024))
		if err != nil {
			b.Fatal(err)
		}
	}
	// Resolve metadata and establish broker connections before either measured
	// path. On a fresh cluster this can otherwise consume the full benchtime
	// in the first adapter iteration while the direct path gets warm sockets.
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer warmCancel()
	for _, result := range a.PublishBatch(warmCtx, messages) {
		if result.Err != nil {
			b.Fatalf("prewarm confirmed Kafka publish: %v", result.Err)
		}
	}
	measure := func(b *testing.B, publish func(context.Context) error) {
		b.SetBytes(int64(len(messages) * 1024))
		b.ReportAllocs()
		latencies := make([]time.Duration, 0, b.N)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			if err := publish(ctx); err != nil {
				b.Fatal(err)
			}
			latencies = append(latencies, time.Since(start))
		}
		b.StopTimer()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		if len(latencies) > 0 {
			b.ReportMetric(float64(latencies[(len(latencies)*95-1)/100].Microseconds())/1000, "p95_ms")
		}
	}
	b.Run("adapter", func(b *testing.B) {
		measure(b, func(ctx context.Context) error {
			results := a.PublishBatch(ctx, messages)
			for _, result := range results {
				if result.Err != nil {
					return result.Err
				}
			}
			return nil
		})
	})
	b.Run("direct_franz", func(b *testing.B) {
		measure(b, func(ctx context.Context) error {
			records := make([]*kgo.Record, len(messages))
			for i, message := range messages {
				records[i] = encode(message)
			}
			for _, result := range a.producer.ProduceSync(ctx, records...) {
				if result.Err != nil {
					return result.Err
				}
			}
			return nil
		})
	})
}
