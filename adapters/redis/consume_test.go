package redisadapter

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func consumeOptions(t *testing.T) Options {
	t.Helper()
	return Options{Prefix: "mq:v2:consume:" + strings.ReplaceAll(t.Name(), "/", ":") + ":" + time.Now().Format("150405.000000000"), ClaimIdle: 50 * time.Millisecond, Block: 20 * time.Millisecond, RetryMin: 10 * time.Millisecond, RetryMax: 50 * time.Millisecond}
}

func receiveID(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case id := <-ch:
		return id
	case <-time.After(3 * time.Second):
		t.Fatal("delivery timeout")
		return ""
	}
}

func runConsumer(t *testing.T, adapter *Adapter, sub mq.Subscription, handler mq.Handler) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- adapter.Run(ctx, sub, handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("consumer did not stop")
		}
	})
	return cancel, done
}

func TestConsumerGroupsFanoutAndCompetition(t *testing.T) {
	client := integrationClient(t)
	adapter, err := New(client, consumeOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	a, b := mq.Subscription{Topic: "events", Name: "billing"}, mq.Subscription{Topic: "events", Name: "email"}
	for _, sub := range []mq.Subscription{a, b} {
		if err := adapter.Prepare(context.Background(), sub); err != nil {
			t.Fatal(err)
		}
	}
	first, second := make(chan string, 1), make(chan string, 1)
	runConsumer(t, adapter, a, func(_ context.Context, m mq.Message) error {
		select {
		case first <- m.ID:
		default:
		}
		return nil
	})
	runConsumer(t, adapter, b, func(_ context.Context, m mq.Message) error {
		select {
		case second <- m.ID:
		default:
		}
		return nil
	})
	m, _ := mq.NewMessage("events", []byte("created"))
	if err := adapter.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if receiveID(t, first) != m.ID || receiveID(t, second) != m.ID {
		t.Fatal("fanout mismatch")
	}
	compete := mq.Subscription{Topic: "work", Name: "workers"}
	if err := adapter.Prepare(context.Background(), compete); err != nil {
		t.Fatal(err)
	}
	var processed atomic.Int32
	done := make(chan string, 2)
	for i := 0; i < 2; i++ {
		runConsumer(t, adapter, compete, func(_ context.Context, message mq.Message) error {
			processed.Add(1)
			select {
			case done <- message.ID:
			default:
			}
			return nil
		})
	}
	work, _ := mq.NewMessage("work", nil)
	if err := adapter.Publish(context.Background(), work); err != nil {
		t.Fatal(err)
	}
	if receiveID(t, done) != work.ID {
		t.Fatal("wrong competing delivery")
	}
	time.Sleep(30 * time.Millisecond)
	if got := processed.Load(); got != 1 {
		t.Fatalf("same group processed %d times", got)
	}
}

func TestConsumerReclaimsPendingAfterCancel(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	first, _ := New(client, opts)
	sub := mq.Subscription{Topic: "pending", Name: "group"}
	if err := first.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("pending", nil)
	if err := first.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	failed := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- first.Run(ctx, sub, func(_ context.Context, message mq.Message) error {
			failed <- message.ID
			cancel()
			return errors.New("temporary")
		})
	}()
	if receiveID(t, failed) != m.ID {
		t.Fatal("first delivery mismatch")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled consumer did not stop")
	}
	pending, err := client.XPending(context.Background(), first.streamKey(sub.Topic), sub.Name).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	second, _ := New(client, opts)
	got := make(chan string, 1)
	runConsumer(t, second, sub, func(_ context.Context, message mq.Message) error {
		select {
		case got <- message.ID:
		default:
		}
		return nil
	})
	if receiveID(t, got) != m.ID {
		t.Fatal("reclaimed ID changed")
	}
}

type failDeadLetterClient struct{ redis.UniversalClient }

func (c failDeadLetterClient) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	if strings.Contains(args.Stream, ":dlq:") {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(errors.New("dead-letter unavailable"))
		return cmd
	}
	return c.UniversalClient.XAdd(ctx, args)
}

func TestDeadLetterFailureLeavesSourcePending(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	normal, _ := New(client, opts)
	sub := mq.Subscription{Topic: "dead", Name: "group"}
	if err := normal.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("dead", []byte("bad"))
	m.Headers = map[string]string{"schema": "v1"}
	if err := normal.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	failing, _ := New(failDeadLetterClient{client}, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := failing.Run(ctx, sub, func(context.Context, mq.Message) error { return mq.Permanent(errors.New("schema")) }); err == nil {
		t.Fatal("dead-letter failure hidden")
	}
	pending, err := client.XPending(context.Background(), normal.streamKey(sub.Topic), sub.Name).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending after DLQ failure = %+v, %v", pending, err)
	}
	count, err := client.XLen(context.Background(), normal.DeadLetterStream(sub)).Result()
	if err != nil || count != 0 {
		t.Fatalf("DLQ length = %d, %v", count, err)
	}
	delivered := make(chan string, 1)
	runConsumer(t, normal, sub, func(_ context.Context, message mq.Message) error {
		select {
		case delivered <- message.ID:
		default:
		}
		return mq.Permanent(errors.New("schema"))
	})
	if receiveID(t, delivered) != m.ID {
		t.Fatal("dead-letter recovery ID changed")
	}
	deadline := time.After(2 * time.Second)
	for {
		entries, err := client.XRange(context.Background(), normal.DeadLetterStream(sub), "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 1 {
			if entries[0].Values["id"] != m.ID {
				t.Fatalf("DLQ message = %+v", entries[0])
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("dead letter never stored")
		case <-time.After(time.Millisecond):
		}
	}
}

type failAckClient struct{ redis.UniversalClient }

func (c failAckClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	cmd.SetErr(errors.New("ACK unavailable"))
	return cmd
}

func TestAckFailureAllowsRedelivery(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	normal, _ := New(client, opts)
	sub := mq.Subscription{Topic: "ack-fail", Name: "group"}
	if err := normal.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, nil)
	if err := normal.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	failing, _ := New(failAckClient{client}, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first := make(chan string, 1)
	err := failing.Run(ctx, sub, func(_ context.Context, message mq.Message) error { first <- message.ID; return nil })
	if err == nil || receiveID(t, first) != m.ID {
		t.Fatalf("ACK failure = %v", err)
	}
	pending, err := client.XPending(context.Background(), normal.streamKey(sub.Topic), sub.Name).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending after ACK failure = %+v, %v", pending, err)
	}
	second := make(chan string, 1)
	runConsumer(t, normal, sub, func(_ context.Context, message mq.Message) error {
		select {
		case second <- message.ID:
		default:
		}
		return nil
	})
	if receiveID(t, second) != m.ID {
		t.Fatal("redelivery changed ID")
	}
}

func TestStartLatestRequiresExplicitOption(t *testing.T) {
	client := integrationClient(t)
	opts := consumeOptions(t)
	opts.StartLatest = true
	a, _ := New(client, opts)
	sub := mq.Subscription{Topic: "latest", Name: "new-group"}
	old, _ := mq.NewMessage(sub.Topic, []byte("old"))
	if err := a.Publish(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	newMessage, _ := mq.NewMessage(sub.Topic, []byte("new"))
	if err := a.Publish(context.Background(), newMessage); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	runConsumer(t, a, sub, func(_ context.Context, message mq.Message) error {
		select {
		case got <- message.ID:
		default:
		}
		return nil
	})
	if receiveID(t, got) != newMessage.ID {
		t.Fatal("latest group replayed earlier entry")
	}
}
