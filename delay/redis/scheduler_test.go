package redisdelay

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

type targetStub struct{}

func (targetStub) Publish(context.Context, mq.Message) error { return nil }
func (targetStub) Close(context.Context) error               { return nil }

type recordingTarget struct {
	mu       sync.Mutex
	messages []mq.Message
	err      error
	seen     chan mq.Message
}

func (r *recordingTarget) Publish(_ context.Context, m mq.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.messages = append(r.messages, m)
	if r.seen != nil {
		select {
		case r.seen <- m:
		default:
		}
	}
	return nil
}
func (*recordingTarget) Close(context.Context) error { return nil }

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("MQ_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set MQ_TEST_REDIS_ADDR for Redis integration")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func testPrefix(t *testing.T) string {
	return "mq:test:delay:" + t.Name() + ":" + time.Now().Format("150405.000000000")
}

func TestNewValidatesOptionsAndDependencies(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	for _, tc := range []struct {
		client  redis.UniversalClient
		target  mq.Publisher
		options Options
	}{
		{nil, targetStub{}, Options{}}, {client, nil, Options{}},
		{client, targetStub{}, Options{Shards: -1}}, {client, targetStub{}, Options{Prefix: "bad{tag}"}},
		{client, targetStub{}, Options{LeaseDuration: -time.Second}},
	} {
		if _, err := New(tc.client, tc.target, tc.options); err == nil {
			t.Fatalf("accepted invalid options %+v", tc.options)
		}
	}
}

func TestCodecPreservesBinaryEnvelope(t *testing.T) {
	m, _ := mq.NewMessage("领域.事件", []byte{0, 255, 1})
	m.Key = []byte{255, 0, 42}
	m.Headers = map[string]string{"trace": "链路"}
	due := time.Now().UTC().Add(time.Hour)
	encoded, err := encodeTask(m, due)
	if err != nil {
		t.Fatal(err)
	}
	got, gotDue, err := decodeTask(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Topic != m.Topic || string(got.Key) != string(m.Key) || string(got.Payload) != string(m.Payload) || got.Headers["trace"] != m.Headers["trace"] || gotDue.UnixMilli() != ceilMillis(due) {
		t.Fatalf("round trip = %+v, %v", got, gotDue)
	}
}

func TestPublishAtIsAtomicAndIdempotent(t *testing.T) {
	client := testRedis(t)
	a, err := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 2})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("schedule", []byte("payload"))
	due := time.Now().Add(time.Hour)
	if err := a.PublishAt(context.Background(), m, due); err != nil {
		t.Fatal(err)
	}
	if err := a.PublishAt(context.Background(), m, due); err != nil {
		t.Fatal(err)
	}
	lane := a.laneFor(m)
	count, err := client.ZCard(context.Background(), a.keys(lane).due).Result()
	if err != nil || count != 1 {
		t.Fatalf("due count = %d, %v", count, err)
	}
	if err := a.PublishAt(context.Background(), m, due.Add(time.Second)); !errors.Is(err, ErrScheduleConflict) {
		t.Fatalf("changed deadline = %v", err)
	}
	if err := a.PublishAt(context.Background(), mq.Message{}, due); err == nil {
		t.Fatal("accepted invalid message")
	}
}

func TestWorkerWaitsUntilDueAndConfirmsBeforeRemoval(t *testing.T) {
	client := testRedis(t)
	target := &recordingTarget{seen: make(chan mq.Message, 2)}
	a, err := New(client, target, Options{Prefix: testPrefix(t), Shards: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("delayed", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(250*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-target.seen:
		t.Fatal("delivered before due time")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case got := <-target.seen:
		if got.ID != m.ID {
			t.Fatalf("ID = %s", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("due message not delivered")
	}
	keys := a.keys(a.laneFor(m))
	deadline := time.After(time.Second)
	for {
		count, err := client.HLen(context.Background(), keys.records).Result()
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("confirmed task not removed")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestLeasePreventsConcurrentClaimAndRecoversAfterExpiry(t *testing.T) {
	client := testRedis(t)
	opts := Options{Prefix: testPrefix(t), Shards: 1, LeaseDuration: 50 * time.Millisecond}
	a, _ := New(client, targetStub{}, opts)
	b, _ := New(client, targetStub{}, opts)
	m, _ := mq.NewMessage("lease", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	first, err := a.claimOne(context.Background(), 0)
	if err != nil || first == nil || first.message.ID != m.ID {
		t.Fatalf("first claim = %+v, %v", first, err)
	}
	other, err := b.claimOne(context.Background(), 0)
	if err != nil || other != nil {
		t.Fatalf("concurrent claim = %+v, %v", other, err)
	}
	time.Sleep(70 * time.Millisecond)
	second, err := b.claimOne(context.Background(), 0)
	if err != nil || second == nil || second.message.ID != m.ID {
		t.Fatalf("reclaim = %+v, %v", second, err)
	}
	completed, err := a.complete(context.Background(), first)
	if err != nil || completed {
		t.Fatalf("stale completion = %t, %v", completed, err)
	}
	completed, err = b.complete(context.Background(), second)
	if err != nil || !completed {
		t.Fatalf("valid completion = %t, %v", completed, err)
	}
}

func TestTargetFailureRetainsScheduledTask(t *testing.T) {
	client := testRedis(t)
	target := &recordingTarget{err: errors.New("target unavailable")}
	a, _ := New(client, target, Options{Prefix: testPrefix(t), Shards: 1, LeaseDuration: 40 * time.Millisecond, PollInterval: 10 * time.Millisecond})
	m, _ := mq.NewMessage("failure", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Run(ctx); err == nil {
		t.Fatal("target failure hidden")
	}
	keys := a.keys(0)
	count, err := client.HLen(context.Background(), keys.records).Result()
	if err != nil || count != 1 {
		t.Fatalf("task lost after target failure: %d, %v", count, err)
	}
	target.mu.Lock()
	target.err = nil
	target.seen = make(chan mq.Message, 1)
	target.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	secondCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- a.Run(secondCtx) }()
	select {
	case got := <-target.seen:
		if got.ID != m.ID {
			t.Fatal("recovery changed message ID")
		}
	case <-time.After(time.Second):
		t.Fatal("leased task not recovered")
	}
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery worker did not stop")
	}
}
