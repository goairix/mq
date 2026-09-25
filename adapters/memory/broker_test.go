package memory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/goairix/mq/v2/contracttest"
)

func testMessage(t *testing.T, topic string) mq.Message {
	t.Helper()
	m, err := mq.NewMessage(topic, []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func runOne(t *testing.T, b *Broker, sub mq.Subscription, handler mq.Handler) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Run(ctx, sub, handler) }()
	return cancel
}

func await(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
		return ""
	}
}

func TestNewAndPublishValidation(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		if _, err := New(capacity); err == nil {
			t.Fatalf("accepted capacity %d", capacity)
		}
	}
	b, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), mq.Message{}); err == nil {
		t.Fatal("accepted invalid message")
	}
	m := testMessage(t, "orders")
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), testMessage(t, "orders")); !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("want backpressure, got %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), m); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("want closed, got %v", err)
	}
}

func TestPublishCopiesMessage(t *testing.T) {
	b, _ := New(4)
	m := testMessage(t, "copy")
	m.Headers = map[string]string{"kind": "old"}
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	m.Payload[0] = 'X'
	m.Headers["kind"] = "new"
	got := make(chan string, 1)
	cancel := runOne(t, b, mq.Subscription{Topic: "copy", Name: "one"}, func(_ context.Context, received mq.Message) error {
		got <- string(received.Payload) + "/" + received.Headers["kind"]
		return nil
	})
	defer cancel()
	if value := await(t, got); value != "original/old" {
		t.Fatalf("mutated stored message: %q", value)
	}
}

func TestFanoutAndCompetition(t *testing.T) {
	b, _ := New(8)
	first, second := make(chan string, 2), make(chan string, 2)
	c1 := runOne(t, b, mq.Subscription{Topic: "fanout", Name: "billing"}, func(_ context.Context, m mq.Message) error { first <- m.ID; return nil })
	c2 := runOne(t, b, mq.Subscription{Topic: "fanout", Name: "email"}, func(_ context.Context, m mq.Message) error { second <- m.ID; return nil })
	defer c1()
	defer c2()
	m := testMessage(t, "fanout")
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if await(t, first) != m.ID || await(t, second) != m.ID {
		t.Fatal("fanout lost message")
	}
	var count atomic.Int32
	ca := runOne(t, b, mq.Subscription{Topic: "compete", Name: "one"}, func(context.Context, mq.Message) error { count.Add(1); return nil })
	cb := runOne(t, b, mq.Subscription{Topic: "compete", Name: "one"}, func(context.Context, mq.Message) error { count.Add(1); return nil })
	defer ca()
	defer cb()
	if err := b.Publish(context.Background(), testMessage(t, "compete")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for count.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("not delivered")
		case <-time.After(time.Millisecond):
		}
	}
	time.Sleep(20 * time.Millisecond)
	if got := count.Load(); got != 1 {
		t.Fatalf("same-group delivery count = %d", got)
	}
}

func TestRetryAndDeadLetter(t *testing.T) {
	b, _ := New(8)
	m := testMessage(t, "retry")
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	delivered := make(chan string, 1)
	cancel := runOne(t, b, mq.Subscription{Topic: "retry", Name: "one"}, func(_ context.Context, received mq.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary")
		}
		delivered <- received.ID
		return nil
	})
	if await(t, delivered) != m.ID || attempts.Load() != 2 {
		t.Fatal("retry did not preserve message")
	}
	cancel()
	bad := testMessage(t, "bad")
	if err := b.Publish(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	good := testMessage(t, "bad")
	if err := b.Publish(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	stop := runOne(t, b, mq.Subscription{Topic: "bad", Name: "one"}, func(_ context.Context, msg mq.Message) error {
		if msg.ID == bad.ID {
			return mq.Permanent(errors.New("schema"))
		}
		got <- msg.ID
		return nil
	})
	defer stop()
	if await(t, got) != good.ID {
		t.Fatal("poison message blocked later message")
	}
	letters := b.DeadLetters(mq.Subscription{Topic: "bad", Name: "one"})
	if len(letters) != 1 || letters[0].Message.ID != bad.ID {
		t.Fatalf("dead letters = %+v", letters)
	}
}

func TestCanceledHandlerIsRedelivered(t *testing.T) {
	b, _ := New(1)
	m := testMessage(t, "cancel")
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	first := make(chan string, 1)
	cancel := runOne(t, b, mq.Subscription{Topic: "cancel", Name: "one"}, func(_ context.Context, received mq.Message) error {
		select {
		case first <- received.ID:
		default:
		}
		return errors.New("temporary")
	})
	if await(t, first) != m.ID {
		t.Fatal("first delivery changed ID")
	}
	cancel()
	redelivered := make(chan string, 1)
	stop := runOne(t, b, mq.Subscription{Topic: "cancel", Name: "one"}, func(_ context.Context, received mq.Message) error {
		redelivered <- received.ID
		return nil
	})
	defer stop()
	if await(t, redelivered) != m.ID {
		t.Fatal("redelivery changed ID")
	}
	if err := b.Publish(context.Background(), testMessage(t, "cancel")); err != nil {
		t.Fatalf("consumed record did not free capacity: %v", err)
	}
}

func TestPortableContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Transport {
		b, err := New(64)
		if err != nil {
			t.Fatal(err)
		}
		return contracttest.Transport{
			Publisher: b, Subscriber: b, BatchSubscriber: b,
			Backpressure: func(t *testing.T) {
				limited, _ := New(1)
				if err := limited.Publish(context.Background(), testMessage(t, "full")); err != nil {
					t.Fatal(err)
				}
				if err := limited.Publish(context.Background(), testMessage(t, "full")); !errors.Is(err, mq.ErrBackpressure) {
					t.Fatalf("backpressure = %v", err)
				}
			},
		}
	})
}

func TestPublishBatchResults(t *testing.T) {
	b, _ := New(1)
	results := b.PublishBatch(context.Background(), []mq.Message{testMessage(t, "batch"), {}, testMessage(t, "batch")})
	if err := mq.ValidatePublishResults(3, results); err != nil {
		t.Fatal(err)
	}
	if results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected || !errors.Is(results[2].Err, mq.ErrBackpressure) {
		t.Fatalf("results = %+v", results)
	}
}

func TestBatchOversizeLimits(t *testing.T) {
	b, _ := New(4)
	m := testMessage(t, "oversize")
	m.Payload = make([]byte, 100)
	if err := b.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxBytes = 32
	opts.MaxInFlightBytes = 256
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan int, 1)
	go func() {
		_ = b.RunBatch(ctx, mq.Subscription{Topic: "oversize", Name: "soft"}, opts, func(_ context.Context, batch []mq.Message) ([]error, error) { delivered <- len(batch); return nil, nil })
	}()
	if got := awaitInt(t, delivered); got != 1 {
		t.Fatalf("oversize batch length = %d", got)
	}
	cancel()
	opts.MaxInFlightBytes = 32
	ctx2, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := b.RunBatch(ctx2, mq.Subscription{Topic: "oversize", Name: "hard"}, opts, func(context.Context, []mq.Message) ([]error, error) { t.Error("handler invoked"); return nil, nil }); err == nil {
		t.Fatal("oversize record above hard cap accepted")
	}
}

func awaitInt(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("batch timeout")
		return 0
	}
}

func TestBatchPartialSuccessDoesNotRepeatLaterRecord(t *testing.T) {
	b, _ := New(4)
	first, second := testMessage(t, "partial"), testMessage(t, "partial")
	for _, m := range []mq.Message{first, second} {
		if err := b.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan []string, 3)
	go func() {
		_ = b.RunBatch(ctx, mq.Subscription{Topic: "partial", Name: "one"}, mq.DefaultBatchOptions(), func(_ context.Context, batch []mq.Message) ([]error, error) {
			ids := make([]string, len(batch))
			for i, m := range batch {
				ids[i] = m.ID
			}
			calls <- ids
			if len(batch) == 2 {
				return []error{errors.New("retry"), nil}, nil
			}
			return nil, nil
		})
	}()
	initial := awaitIDs(t, calls)
	retried := awaitIDs(t, calls)
	if len(initial) != 2 || initial[0] != first.ID || initial[1] != second.ID || len(retried) != 1 || retried[0] != first.ID {
		t.Fatalf("batches = %v then %v", initial, retried)
	}
}

func awaitIDs(t *testing.T, ch <-chan []string) []string {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("batch timeout")
		return nil
	}
}
