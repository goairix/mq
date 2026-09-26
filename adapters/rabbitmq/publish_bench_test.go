package rabbitadapter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Compare confirmed durable quorum publishing with the same AMQP client and
// topology. Set MQ_TEST_RABBIT_URL and run -bench=BenchmarkConfirmedBatch.
func BenchmarkConfirmedBatch(b *testing.B) {
	url := os.Getenv("MQ_TEST_RABBIT_URL")
	if url == "" {
		b.Skip("set MQ_TEST_RABBIT_URL")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	a, err := New(conn, Options{Prefix: fmt.Sprintf("mq-v2-bench-%d", time.Now().UnixNano())})
	if err != nil {
		b.Fatal(err)
	}
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "benchmark", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		b.Fatal(err)
	}
	const batchSize = 256
	const payloadBytes = 1024
	messages := make([]mq.Message, batchSize)
	for i := range messages {
		messages[i], _ = mq.NewMessage(sub.Topic, make([]byte, payloadBytes))
	}
	b.SetBytes(batchSize * payloadBytes)
	b.Run("adapter", func(b *testing.B) {
		b.SetBytes(batchSize * payloadBytes)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			results := a.PublishBatch(context.Background(), messages)
			for _, result := range results {
				if result.State != mq.PublishAccepted {
					b.Fatal(result.Err)
				}
			}
		}
	})
	b.Run("direct_amqp", func(b *testing.B) {
		ch, err := conn.Channel()
		if err != nil {
			b.Fatal(err)
		}
		defer ch.Close()
		if err := ch.Confirm(false); err != nil {
			b.Fatal(err)
		}
		returns := ch.NotifyReturn(make(chan amqp.Return, batchSize))
		b.SetBytes(batchSize * payloadBytes)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			confirms := make([]*amqp.DeferredConfirmation, batchSize)
			for j, message := range messages {
				publishing := encode(message)
				confirmation, err := ch.PublishWithDeferredConfirmWithContext(context.Background(), a.options.Exchange, a.routeKey(message.Topic), true, false, publishing)
				if err != nil {
					b.Fatal(err)
				}
				confirms[j] = confirmation
			}
			for _, confirmation := range confirms {
				if !confirmation.Wait() {
					b.Fatal("publisher NACK")
				}
			}
			select {
			case returned := <-returns:
				b.Fatalf("returned message: %v", returned)
			default:
			}
		}
	})
}
