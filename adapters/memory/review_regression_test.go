package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestBatchMaxWaitCollectsSparseArrivals(t *testing.T) {
	b, _ := New(4)
	defer b.Close(context.Background())
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages = 2
	opts.MaxWait = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batchSize := make(chan int, 1)
	go func() {
		_ = b.RunBatch(ctx, mq.Subscription{Topic: "wait", Name: "one"}, opts, func(_ context.Context, batch []mq.Message) ([]error, error) { batchSize <- len(batch); return nil, nil })
	}()
	if err := b.Publish(context.Background(), testMessage(t, "wait")); err != nil {
		t.Fatal(err)
	}
	select {
	case size := <-batchSize:
		t.Fatalf("delivered a sparse batch of %d before MaxWait", size)
	case <-time.After(50 * time.Millisecond):
	}
	if err := b.Publish(context.Background(), testMessage(t, "wait")); err != nil {
		t.Fatal(err)
	}
	if size := awaitInt(t, batchSize); size != 2 {
		t.Fatalf("collected %d messages, want 2", size)
	}
}

func TestBatchMaxWaitExpires(t *testing.T) {
	b, _ := New(2)
	defer b.Close(context.Background())
	opts := mq.DefaultBatchOptions()
	opts.MaxWait = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batchSize := make(chan int, 1)
	go func() {
		_ = b.RunBatch(ctx, mq.Subscription{Topic: "expire", Name: "one"}, opts, func(_ context.Context, batch []mq.Message) ([]error, error) { batchSize <- len(batch); return nil, nil })
	}()
	start := time.Now()
	if err := b.Publish(context.Background(), testMessage(t, "expire")); err != nil {
		t.Fatal(err)
	}
	if size := awaitInt(t, batchSize); size != 1 {
		t.Fatalf("batch size = %d", size)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("MaxWait ignored: %v", elapsed)
	}
}

func TestDeadLettersBoundedAndOriginalRemains(t *testing.T) {
	b, _ := New(1)
	defer b.Close(context.Background())
	sub := mq.Subscription{Topic: "poison", Name: "one"}
	first := testMessage(t, "poison")
	if err := b.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = b.Run(ctx, sub, func(context.Context, mq.Message) error { return mq.Permanent(errors.New("bad")) })
	}()
	deadline := time.After(2 * time.Second)
	for len(b.DeadLetters(sub)) == 0 {
		select {
		case <-deadline:
			t.Fatal("first dead letter not stored")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	second := testMessage(t, "poison")
	if err := b.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := b.RunBatch(runCtx, sub, mq.DefaultBatchOptions(), func(context.Context, []mq.Message) ([]error, error) {
		return []error{mq.Permanent(errors.New("bad again"))}, nil
	}); !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("full dead-letter storage error = %v", err)
	}
	if letters := b.DeadLetters(sub); len(letters) != 1 || letters[0].Message.ID != first.ID {
		t.Fatalf("dead letters = %+v", letters)
	}
	got := make(chan string, 1)
	resume := runOne(t, b, sub, func(_ context.Context, msg mq.Message) error { got <- msg.ID; return nil })
	defer resume()
	if await(t, got) != second.ID {
		t.Fatal("unacknowledged poison record was lost")
	}
}
