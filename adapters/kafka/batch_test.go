package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestBatchPartialSuccessDoesNotCommitPastFailedOffset(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.partial.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "partial"}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := createContractTopics(ctx, a.producer, topic, topic+a.options.DLQSuffix); err != nil {
		t.Fatal(err)
	}
	first, _ := mq.NewMessage(topic, []byte("first"))
	first.Key = []byte("same-partition")
	second, _ := mq.NewMessage(topic, []byte("second"))
	second.Key = first.Key
	for _, m := range []mq.Message{first, second} {
		if err := a.Publish(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages, opts.MaxWait = 2, 200*time.Millisecond
	runCtx, cancel := context.WithCancel(ctx)
	err = a.RunBatch(runCtx, sub, opts, func(_ context.Context, messages []mq.Message) ([]error, error) {
		if len(messages) != 2 || messages[0].ID != first.ID || messages[1].ID != second.ID {
			t.Fatalf("unexpected first batch: %+v", messages)
		}
		cancel() // crash before the failed first offset can be retried
		return []error{errors.New("transient"), nil}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first RunBatch error = %v", err)
	}
	lag, err := kafkaOutstanding(ctx, a.producer, sub)
	if err != nil {
		t.Fatal(err)
	}
	if lag != 2 {
		t.Fatalf("lag after partial success = %d, want 2", lag)
	}
	seen := make(map[string]bool)
	runCtx, cancel = context.WithCancel(ctx)
	err = a.RunBatch(runCtx, sub, opts, func(_ context.Context, messages []mq.Message) ([]error, error) {
		for _, m := range messages {
			seen[m.ID] = true
		}
		cancel()
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) || !seen[first.ID] || !seen[second.ID] {
		t.Fatalf("redelivery error=%v seen=%v", err, seen)
	}
}

func TestRunBatchPartialFailureRetriesWithoutSkippingOffset(t *testing.T) {
	brokers := testBrokers(t)
	topic := os.Getenv("MQ_TEST_KAFKA_TOPIC")
	if topic == "" {
		t.Skip("set MQ_TEST_KAFKA_TOPIC")
	}
	a, err := New(brokers, Options{RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: topic, Name: "mq-v2-kafka-batch-" + time.Now().Format("150405.000000000")}
	first, _ := mq.NewMessage(topic, []byte("first-"+sub.Name))
	first.Key = []byte(sub.Name)
	second, _ := mq.NewMessage(topic, []byte("second-"+sub.Name))
	second.Key = first.Key
	for _, m := range []mq.Message{first, second} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages = 2
	opts.MaxWait = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	got := make(chan string, 4)
	var attempts atomic.Int32
	go func() {
		done <- a.RunBatch(ctx, sub, opts, func(_ context.Context, messages []mq.Message) ([]error, error) {
			results := make([]error, len(messages))
			for i, m := range messages {
				if m.ID == first.ID && attempts.Add(1) == 1 {
					results[i] = errors.New("transient")
					continue
				}
				if m.ID == first.ID || m.ID == second.ID {
					got <- m.ID
				}
			}
			return results, nil
		})
	}()
	seen := map[string]bool{}
	for !seen[first.ID] || !seen[second.ID] {
		select {
		case id := <-got:
			seen[id] = true
		case <-ctx.Done():
			t.Fatal("batch did not retry")
		}
	}
	cancel()
	<-done
}

func TestRunBatchInvalidResultDoesNotCommit(t *testing.T) {
	brokers := testBrokers(t)
	topic := os.Getenv("MQ_TEST_KAFKA_TOPIC")
	if topic == "" {
		t.Skip("set MQ_TEST_KAFKA_TOPIC")
	}
	a, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: topic, Name: "mq-v2-kafka-invalid-" + time.Now().Format("150405.000000000")}
	m, _ := mq.NewMessage(topic, []byte("invalid-"+sub.Name))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var invoked atomic.Bool
	if err := a.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { invoked.Store(true); return []error{}, nil }); err == nil || !invoked.Load() {
		t.Fatalf("invalid batch result = %v, invoked=%t", err, invoked.Load())
	}
	got := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(_ context.Context, message mq.Message) error {
			if message.ID == m.ID {
				got <- message.ID
			}
			return nil
		})
	}()
	select {
	case id := <-got:
		if id != m.ID {
			t.Fatal("wrong redelivery")
		}
	case <-ctx.Done():
		t.Fatal("invalid result committed source record")
	}
	cancel()
	<-done
}
