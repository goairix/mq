package kafkaadapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestDeadLetterHeadroomDoesNotBlockValidSourceRecord(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{BatchMaxBytes: 512, MaxMessageBytes: 512, MaxBufferedBytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.dlq.headroom.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "headroom"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic, topic+a.options.DLQSuffix); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(topic, bytes.Repeat([]byte("x"), 300))
	if err := a.Publish(ctx, m); err != nil {
		t.Fatalf("source publish: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	err = a.Run(runCtx, sub, func(context.Context, mq.Message) error {
		stop()
		return mq.Permanent(errors.New("bad payload"))
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DLQ run error = %v", err)
	}
	lag, err := kafkaOutstanding(ctx, a.producer, sub)
	if err != nil {
		t.Fatal(err)
	}
	if lag != 0 {
		t.Fatalf("source lag after confirmed DLQ = %d, want 0", lag)
	}
}

func TestMissingDeadLetterTopicDoesNotCommitSource(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.missing.dlq.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "missing-dlq"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(topic, []byte("poison"))
	if err := a.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	err = a.Run(ctx, sub, func(context.Context, mq.Message) error {
		return mq.Permanent(errors.New("poison"))
	})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing DLQ should return publish error, got %v", err)
	}
	lag, err := kafkaOutstanding(ctx, a.producer, sub)
	if err != nil {
		t.Fatal(err)
	}
	if lag != 1 {
		t.Fatalf("source lag after unavailable DLQ = %d, want 1", lag)
	}
	runCtx, stop := context.WithCancel(ctx)
	seen := false
	err = a.Run(runCtx, sub, func(_ context.Context, message mq.Message) error {
		seen = message.ID == m.ID
		stop()
		return nil
	})
	if !errors.Is(err, context.Canceled) || !seen {
		t.Fatalf("redelivery error=%v seen=%t", err, seen)
	}
}

func TestPollWorkDeadlineDoesNotCommitLateHandler(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{MaxPollWork: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.poll.deadline.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "deadline"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(topic, []byte("late"))
	if err := a.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	err = a.Run(ctx, sub, func(handlerCtx context.Context, _ mq.Message) error {
		<-handlerCtx.Done()
		return nil // late success must not advance the committed offset
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("poll work deadline error = %v", err)
	}
	lag, err := kafkaOutstanding(ctx, a.producer, sub)
	if err != nil {
		t.Fatal(err)
	}
	if lag != 1 {
		t.Fatalf("lag after late handler = %d, want 1", lag)
	}
}

func TestPollWorkDeadlineInterruptsRetryWait(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{MaxPollWork: 100 * time.Millisecond, RetryMin: 2 * time.Second, RetryMax: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.retry.deadline.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "retry-deadline"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(topic, []byte("retry"))
	if err := a.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	var handlerStart time.Time
	err = a.Run(ctx, sub, func(context.Context, mq.Message) error {
		if handlerStart.IsZero() {
			handlerStart = time.Now()
		}
		return errors.New("retry")
	})
	if !errors.Is(err, context.DeadlineExceeded) || handlerStart.IsZero() || time.Since(handlerStart) > time.Second {
		t.Fatalf("retry deadline = %v, elapsed since handler %s", err, time.Since(handlerStart))
	}
}

func TestRunRetriesAndCommitsAfterSuccess(t *testing.T) {
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
	sub := mq.Subscription{Topic: topic, Name: "mq-v2-kafka-retry-" + time.Now().Format("150405.000000000")}
	m, _ := mq.NewMessage(topic, []byte("retry-"+sub.Name))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	got := make(chan string, 1)
	attempts := 0
	go func() {
		done <- a.Run(ctx, sub, func(_ context.Context, message mq.Message) error {
			if message.ID != m.ID {
				return nil
			}
			attempts++
			if attempts == 1 {
				return errors.New("transient")
			}
			got <- message.ID
			return nil
		})
	}()
	select {
	case id := <-got:
		if id != m.ID || attempts != 2 {
			t.Fatalf("id=%s attempts=%d", id, attempts)
		}
	case <-ctx.Done():
		t.Fatal("message not processed")
	}
	cancel()
	<-done
}

func TestRunPermanentFailureDoesNotBlockLaterMessage(t *testing.T) {
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
	sub := mq.Subscription{Topic: topic, Name: "mq-v2-kafka-dlq-" + time.Now().Format("150405.000000000")}
	bad, _ := mq.NewMessage(topic, []byte("bad-"+sub.Name))
	good, _ := mq.NewMessage(topic, []byte("good-"+sub.Name))
	for _, m := range []mq.Message{bad, good} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	got := make(chan string, 1)
	go func() {
		done <- a.Run(ctx, sub, func(_ context.Context, message mq.Message) error {
			if message.ID == bad.ID {
				return mq.Permanent(errors.New("bad payload"))
			}
			if message.ID == good.ID {
				got <- message.ID
			}
			return nil
		})
	}()
	select {
	case id := <-got:
		if id != good.ID {
			t.Fatal("wrong good ID")
		}
	case <-ctx.Done():
		t.Fatal("permanent message blocked later message")
	}
	cancel()
	<-done
}
