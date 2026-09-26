package redisadapter

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/goairix/mq/v2/contracttest"
)

func TestPortableContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Transport {
		client := integrationClient(t)
		opts := consumeOptions(t)
		adapter, err := New(client, opts)
		if err != nil {
			t.Fatal(err)
		}
		return contracttest.Transport{Publisher: adapter, Subscriber: adapter, BatchSubscriber: adapter, Prepare: adapter.Prepare,
			Outstanding: func(ctx context.Context, sub mq.Subscription) (int64, error) {
				info, err := client.XPending(ctx, adapter.streamKey(sub.Topic), sub.Name).Result()
				if err != nil {
					return 0, err
				}
				return info.Count, nil
			},
		}
	})
}

func batchMessage(t *testing.T, topic, payload string) mq.Message {
	t.Helper()
	m, err := mq.NewMessage(topic, []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestBatchPartialResultPreservesLaterSuccess(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "partial", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	first, second := batchMessage(t, sub.Topic, "first"), batchMessage(t, sub.Topic, "second")
	for _, m := range []mq.Message{first, second} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages = 2
	opts.MaxWait = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan []string, 3)
	go func() {
		_ = a.RunBatch(ctx, sub, opts, func(_ context.Context, batch []mq.Message) ([]error, error) {
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
	initial, retried := awaitIDs(t, calls), awaitIDs(t, calls)
	if len(initial) != 2 || initial[0] != first.ID || initial[1] != second.ID || len(retried) != 1 || retried[0] != first.ID {
		t.Fatalf("batches %v then %v", initial, retried)
	}
}

func awaitIDs(t *testing.T, ch <-chan []string) []string {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("batch delivery timeout")
		return nil
	}
}

func TestBatchAllSuccessAndPermanent(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "batch-permanent", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	bad, good := batchMessage(t, sub.Topic, "bad"), batchMessage(t, sub.Topic, "good")
	for _, m := range []mq.Message{bad, good} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages = 2
	opts.MaxWait = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan []string, 1)
	go func() {
		_ = a.RunBatch(ctx, sub, opts, func(_ context.Context, batch []mq.Message) ([]error, error) {
			ids := make([]string, len(batch))
			results := make([]error, len(batch))
			for i, m := range batch {
				ids[i] = m.ID
				if m.ID == bad.ID {
					results[i] = mq.Permanent(errors.New("schema"))
				}
			}
			seen <- ids
			return results, nil
		})
	}()
	if ids := awaitIDs(t, seen); len(ids) != 2 {
		t.Fatalf("batch = %v", ids)
	}
	deadline := time.After(2 * time.Second)
	for {
		pending, err := client.XPending(context.Background(), a.streamKey(sub.Topic), sub.Name).Result()
		if err != nil {
			t.Fatal(err)
		}
		if pending.Count == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("batch was not ACKed")
		case <-time.After(time.Millisecond):
		}
	}
	entries, err := client.XRange(context.Background(), a.DeadLetterStream(sub), "-", "+").Result()
	if err != nil || len(entries) != 1 || entries[0].Values["id"] != bad.ID {
		t.Fatalf("dead letters = %+v, %v", entries, err)
	}
}

func TestBatchWholeFailureAndInvalidLength(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "batch-error", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m := batchMessage(t, sub.Topic, "one")
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { return []error{}, nil }); err == nil {
		t.Fatal("invalid length accepted")
	}
	pending, err := client.XPending(context.Background(), a.streamKey(sub.Topic), sub.Name).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	var attempts atomic.Int32
	got := make(chan string, 1)
	go func() {
		_ = a.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(_ context.Context, batch []mq.Message) ([]error, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("whole batch retry")
			}
			select {
			case got <- batch[0].ID:
			default:
			}
			return nil, nil
		})
	}()
	if receiveID(t, got) != m.ID || attempts.Load() < 2 {
		t.Fatal("whole batch did not retry")
	}
}

func TestBatchByteLimitsAndMaxWait(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "batch-limits", Name: "soft"}
	big := batchMessage(t, sub.Topic, strings.Repeat("x", 100))
	if err := a.Publish(context.Background(), big); err != nil {
		t.Fatal(err)
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxBytes = 32
	opts.MaxInFlightBytes = 256
	opts.MaxWait = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan int, 1)
	go func() {
		_ = a.RunBatch(ctx, sub, opts, func(_ context.Context, batch []mq.Message) ([]error, error) { got <- len(batch); return nil, nil })
	}()
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("oversize batch size %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("oversize record blocked")
	}
	hard := mq.Subscription{Topic: sub.Topic, Name: "hard"}
	opts.MaxInFlightBytes = 32
	shortCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := a.RunBatch(shortCtx, hard, opts, func(context.Context, []mq.Message) ([]error, error) {
		t.Error("oversize handler invoked")
		return nil, nil
	}); err == nil {
		t.Fatal("hard limit ignored")
	}
	waitSub := mq.Subscription{Topic: "wait", Name: "group"}
	if err := a.Prepare(context.Background(), waitSub); err != nil {
		t.Fatal(err)
	}
	first := batchMessage(t, waitSub.Topic, "a")
	second := batchMessage(t, waitSub.Topic, "b")
	waitOpts := mq.DefaultBatchOptions()
	waitOpts.MaxMessages = 2
	waitOpts.MaxWait = 200 * time.Millisecond
	waitCtx, waitCancel := context.WithCancel(context.Background())
	defer waitCancel()
	sizes := make(chan int, 1)
	go func() {
		_ = a.RunBatch(waitCtx, waitSub, waitOpts, func(_ context.Context, batch []mq.Message) ([]error, error) { sizes <- len(batch); return nil, nil })
	}()
	if err := a.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-sizes:
		t.Fatalf("early batch of %d", n)
	case <-time.After(50 * time.Millisecond):
	}
	if err := a.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-sizes:
		if n != 2 {
			t.Fatalf("coalesced batch size %d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("MaxWait batch not delivered")
	}
}

func TestBatchReadAheadRespectsByteLimit(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "read-ahead", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		m := batchMessage(t, sub.Topic, strings.Repeat("x", 32<<10))
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxBytes, opts.MaxInFlightBytes, opts.MaxWait = 40<<10, 40<<10, 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- a.RunBatch(ctx, sub, opts, func(context.Context, []mq.Message) ([]error, error) { close(entered); <-release; return nil, nil })
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("batch handler did not enter")
	}
	pending, err := client.XPending(context.Background(), a.streamKey(sub.Topic), sub.Name).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count > 1 {
		t.Fatalf("read ahead retained %d entries beyond byte limit", pending.Count)
	}
	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not stop")
	}
}

func TestBatchDrainAcknowledgesCompletion(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	opts.DrainTimeout = time.Second
	a, _ := New(client, opts)
	sub := mq.Subscription{Topic: "batch-drain", Name: "group"}
	m := batchMessage(t, sub.Topic, "work")
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	batchOpts := mq.DefaultBatchOptions()
	batchOpts.MaxWait = 0
	ctx, cancel := context.WithCancel(context.Background())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.RunBatch(ctx, sub, batchOpts, func(handlerCtx context.Context, _ []mq.Message) ([]error, error) {
			close(entered)
			select {
			case <-release:
				return nil, nil
			case <-handlerCtx.Done():
				return nil, handlerCtx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("batch handler did not enter")
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunBatch = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not drain")
	}
	pending, err := client.XPending(context.Background(), a.streamKey(sub.Topic), sub.Name).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after batch drain = %+v, %v", pending, err)
	}
}

func TestBatchLongBlockCancellationIsBounded(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	opts.Block = time.Minute
	a, _ := New(client, opts)
	sub := mq.Subscription{Topic: "batch-long-block", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- a.RunBatch(ctx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) { return nil, nil })
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunBatch = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("long block delayed batch cancellation")
	}
}

func TestBatchReloadsOverflowPendingEntry(t *testing.T) {
	client := integrationClient(t)
	a, _ := New(client, consumeOptions(t))
	sub := mq.Subscription{Topic: "overflow", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	first := batchMessage(t, sub.Topic, strings.Repeat("a", 32<<10))
	second := batchMessage(t, sub.Topic, strings.Repeat("b", 32<<10))
	for _, m := range []mq.Message{first, second} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxBytes, opts.MaxInFlightBytes, opts.MaxWait = 40<<10, 40<<10, 100*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen, done := make(chan string, 2), make(chan error, 1)
	go func() {
		done <- a.RunBatch(ctx, sub, opts, func(_ context.Context, batch []mq.Message) ([]error, error) {
			if len(batch) != 1 {
				return nil, errors.New("overflow batch size")
			}
			seen <- batch[0].ID
			return nil, nil
		})
	}()
	firstID, secondID := receiveID(t, seen), receiveID(t, seen)
	if firstID != first.ID || secondID != second.ID {
		t.Fatalf("overflow deliveries = %s, %s", firstID, secondID)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch did not stop")
	}
}
