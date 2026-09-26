// Package contracttest contains reusable black-box transport behavior tests.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

type Transport struct {
	Publisher       mq.Publisher
	Subscriber      mq.Subscriber
	BatchSubscriber mq.BatchSubscriber
	Prepare         func(context.Context, mq.Subscription) error
	Outstanding     func(context.Context, mq.Subscription) (int64, error)
	Backpressure    func(*testing.T)
}

type Factory func(*testing.T) Transport

var nextTopic atomic.Uint64

func topic(t *testing.T) string {
	return fmt.Sprintf("contract.%s.%d", strings.ReplaceAll(t.Name(), "/", "."), nextTopic.Add(1))
}

func fixture(t *testing.T, factory Factory) Transport {
	t.Helper()
	f := factory(t)
	if f.Publisher == nil || f.Subscriber == nil || f.BatchSubscriber == nil || f.Outstanding == nil {
		t.Fatal("factory omitted a required transport")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := f.Publisher.Close(ctx); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	})
	return f
}

func waitOutstandingZero(ctx context.Context, probe func(context.Context, mq.Subscription) (int64, error), sub mq.Subscription) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		count, err := probe(ctx, sub)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d messages remain unacknowledged: %w", count, ctx.Err())
		case <-ticker.C:
		}
	}
}

func prepare(t *testing.T, f Transport, sub mq.Subscription) {
	t.Helper()
	if f.Prepare != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := f.Prepare(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
}

func publish(t *testing.T, f Transport, name string) mq.Message {
	t.Helper()
	m, err := mq.NewMessage(name, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := f.Publisher.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	return m
}

func receive(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("delivery timeout")
		return ""
	}
}

func offer(ch chan<- string, value string) {
	select {
	case ch <- value:
	default:
	}
}

func receiveBoth(t *testing.T, ch <-chan string, first, second string) {
	t.Helper()
	seen := make(map[string]bool)
	deadline := time.After(5 * time.Second)
	for !seen[first] || !seen[second] {
		select {
		case id := <-ch:
			seen[id] = true
		case <-deadline:
			t.Fatalf("missing batch deliveries: %s=%t %s=%t", first, seen[first], second, seen[second])
		}
	}
}

func validInvalidResult(err error, handlerRan bool) bool {
	return handlerRan && err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func run(t *testing.T, f Transport, sub mq.Subscription, handler mq.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Subscriber.Run(ctx, sub, handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("subscriber did not stop")
		}
	})
}

func runBatch(t *testing.T, f Transport, sub mq.Subscription, handler mq.BatchHandler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.BatchSubscriber.RunBatch(ctx, sub, mq.DefaultBatchOptions(), handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("batch subscriber did not stop")
		}
	})
}

// Run checks portable at-least-once semantics. Each subtest uses a fresh
// transport, so an adapter can map Prepare to durable topology setup.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("publish and fanout", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		a, b := mq.Subscription{Topic: name, Name: "a"}, mq.Subscription{Topic: name, Name: "b"}
		prepare(t, f, a)
		prepare(t, f, b)
		first, second := make(chan string, 1), make(chan string, 1)
		run(t, f, a, func(_ context.Context, m mq.Message) error { offer(first, m.ID); return nil })
		run(t, f, b, func(_ context.Context, m mq.Message) error { offer(second, m.ID); return nil })
		m := publish(t, f, name)
		if receive(t, first) != m.ID || receive(t, second) != m.ID {
			t.Fatal("fanout mismatch")
		}
	})
	t.Run("transient redelivery", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		sub := mq.Subscription{Topic: name, Name: "retry"}
		prepare(t, f, sub)
		m := publish(t, f, name)
		var attempts atomic.Int32
		got := make(chan string, 1)
		run(t, f, sub, func(_ context.Context, msg mq.Message) error {
			if attempts.Add(1) == 1 {
				return errors.New("temporary")
			}
			offer(got, msg.ID)
			return nil
		})
		if receive(t, got) != m.ID || attempts.Load() < 2 {
			t.Fatal("missing redelivery")
		}
	})
	t.Run("permanent failure isolation", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		sub := mq.Subscription{Topic: name, Name: "permanent"}
		prepare(t, f, sub)
		bad, good := publish(t, f, name), publish(t, f, name)
		got := make(chan string, 1)
		run(t, f, sub, func(_ context.Context, msg mq.Message) error {
			if msg.ID == bad.ID {
				return mq.Permanent(errors.New("bad schema"))
			}
			offer(got, msg.ID)
			return nil
		})
		if receive(t, got) != good.ID {
			t.Fatal("poison message blocked group")
		}
	})
	t.Run("batch success and partial result", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		sub := mq.Subscription{Topic: name, Name: "batch"}
		prepare(t, f, sub)
		first, second := publish(t, f, name), publish(t, f, name)
		seen := make(chan string, 16)
		var failed atomic.Bool
		runBatch(t, f, sub, func(_ context.Context, batch []mq.Message) ([]error, error) {
			results := make([]error, len(batch))
			for i, msg := range batch {
				if msg.ID == first.ID && !failed.Swap(true) {
					results[i] = errors.New("retry")
					continue
				}
				offer(seen, msg.ID)
			}
			return results, nil
		})
		receiveBoth(t, seen, first.ID, second.ID)
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := waitOutstandingZero(settleCtx, f.Outstanding, sub); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("batch all success", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		sub := mq.Subscription{Topic: name, Name: "all-success"}
		prepare(t, f, sub)
		first, second := publish(t, f, name), publish(t, f, name)
		seen := make(chan string, 16)
		runBatch(t, f, sub, func(_ context.Context, batch []mq.Message) ([]error, error) {
			for _, message := range batch {
				offer(seen, message.ID)
			}
			return nil, nil
		})
		receiveBoth(t, seen, first.ID, second.ID)
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := waitOutstandingZero(settleCtx, f.Outstanding, sub); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("invalid batch result", func(t *testing.T) {
		f := fixture(t, factory)
		name := topic(t)
		sub := mq.Subscription{Topic: name, Name: "invalid"}
		prepare(t, f, sub)
		m := publish(t, f, name)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		handlerRan := false
		err := f.BatchSubscriber.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) {
			handlerRan = true
			return []error{}, nil
		})
		if !validInvalidResult(err, handlerRan) {
			t.Fatalf("invalid batch result not detected: handlerRan=%t error=%v", handlerRan, err)
		}
		got := make(chan string, 1)
		run(t, f, sub, func(_ context.Context, msg mq.Message) error { offer(got, msg.ID); return nil })
		if receive(t, got) != m.ID {
			t.Fatal("invalid result acknowledged message")
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		f := fixture(t, factory)
		sub := mq.Subscription{Topic: topic(t), Name: "cancel"}
		prepare(t, f, sub)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := f.Subscriber.Run(ctx, sub, func(context.Context, mq.Message) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run cancellation = %v", err)
		}
		if err := f.BatchSubscriber.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { return nil, nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("RunBatch cancellation = %v", err)
		}
	})
	t.Run("capacity", func(t *testing.T) {
		f := fixture(t, factory)
		if f.Backpressure != nil {
			f.Backpressure(t)
		}
	})
}
