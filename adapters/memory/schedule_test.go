package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestPublishAtPastAndFuture(t *testing.T) {
	b, _ := New(4)
	defer b.Close(context.Background())
	past := testMessage(t, "scheduled")
	if err := b.PublishAt(context.Background(), past, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 4)
	stop := runOne(t, b, mq.Subscription{Topic: "scheduled", Name: "one"}, func(_ context.Context, m mq.Message) error { got <- m.ID; return nil })
	defer stop()
	if await(t, got) != past.ID {
		t.Fatal("past-due message changed ID")
	}
	future := testMessage(t, "scheduled")
	caller, cancel := context.WithCancel(context.Background())
	if err := b.PublishAt(caller, future, time.Now().Add(150*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case id := <-got:
		t.Fatalf("delivered %s early", id)
	case <-time.After(50 * time.Millisecond):
	}
	if await(t, got) != future.ID {
		t.Fatal("future message lost after caller cancellation")
	}
}

func TestScheduledCapacityAndClose(t *testing.T) {
	b, _ := New(1)
	m := testMessage(t, "scheduled-capacity")
	if err := b.PublishAt(context.Background(), m, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), testMessage(t, "normal")); !errors.Is(err, mq.ErrBackpressure) {
		t.Fatalf("scheduled record did not count toward capacity: %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishAt(context.Background(), m, time.Now().Add(time.Hour)); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("closed scheduling = %v", err)
	}
}

func TestScheduledMessageCopiesPayload(t *testing.T) {
	b, _ := New(2)
	defer b.Close(context.Background())
	m := testMessage(t, "scheduled-copy")
	m.Headers = map[string]string{"kind": "old"}
	if err := b.PublishAt(context.Background(), m, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	m.Payload[0] = 'X'
	m.Headers["kind"] = "new"
	got := make(chan string, 1)
	stop := runOne(t, b, mq.Subscription{Topic: "scheduled-copy", Name: "one"}, func(_ context.Context, received mq.Message) error {
		got <- string(received.Payload) + "/" + received.Headers["kind"]
		return nil
	})
	defer stop()
	if value := await(t, got); value != "original/old" {
		t.Fatalf("scheduled message mutated: %q", value)
	}
}
