package redisdelay

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
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

type blockingTarget struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingTarget) Publish(ctx context.Context, _ mq.Message) error {
	close(b.entered)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*blockingTarget) Close(context.Context) error { return nil }

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
	settings, err := client.ConfigGet(context.Background(), "append*").Result()
	if err != nil || settings["appendonly"] != "yes" || settings["appendfsync"] != "always" {
		t.Fatalf("delay integration requires AOF appendfsync=always: %+v, %v", settings, err)
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

func TestWorkerCancellationDrainsConfirmedPublication(t *testing.T) {
	client := testRedis(t)
	target := &blockingTarget{entered: make(chan struct{}), release: make(chan struct{})}
	a, _ := New(client, target, Options{Prefix: testPrefix(t), Shards: 1, PollInterval: 10 * time.Millisecond, DrainTimeout: time.Second})
	m, _ := mq.NewMessage("drain", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("target publish did not start")
	}
	cancel()
	close(target.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not drain")
	}
	count, err := client.HLen(context.Background(), a.keys(0).records).Result()
	if err != nil || count != 0 {
		t.Fatalf("confirmed task retained: %d, %v", count, err)
	}
}

func TestWorkerDrainExpiryRetainsTask(t *testing.T) {
	client := testRedis(t)
	target := &blockingTarget{entered: make(chan struct{}), release: make(chan struct{})}
	a, _ := New(client, target, Options{Prefix: testPrefix(t), Shards: 1, PollInterval: 10 * time.Millisecond, DrainTimeout: 30 * time.Millisecond})
	m, _ := mq.NewMessage("expiry", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("target publish did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("drain timeout hidden")
		}
	case <-time.After(time.Second):
		t.Fatal("drain timeout not enforced")
	}
	count, err := client.HLen(context.Background(), a.keys(0).records).Result()
	if err != nil || count != 1 {
		t.Fatalf("unconfirmed task deleted: %d, %v", count, err)
	}
}

func TestCrashAfterTargetConfirmCanDuplicate(t *testing.T) {
	client := testRedis(t)
	target := &recordingTarget{seen: make(chan mq.Message, 2)}
	opts := Options{Prefix: testPrefix(t), Shards: 1, LeaseDuration: 40 * time.Millisecond, PollInterval: 10 * time.Millisecond}
	first, _ := New(client, target, opts)
	second, _ := New(client, target, opts)
	m, _ := mq.NewMessage("crash-window", []byte("payload"))
	if err := first.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claim, err := first.claimOne(context.Background(), 0)
	if err != nil || claim == nil {
		t.Fatalf("claim = %+v, %v", claim, err)
	}
	if err := target.Publish(context.Background(), claim.message); err != nil {
		t.Fatal(err)
	}
	// Simulate process death before its completion script.
	time.Sleep(60 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	select {
	case <-target.seen:
	case <-time.After(time.Second):
		t.Fatal("first target confirmation missing")
	}
	select {
	case got := <-target.seen:
		if got.ID != m.ID {
			t.Fatal("duplicate changed message ID")
		}
	case <-time.After(time.Second):
		t.Fatal("expired lease not redelivered")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestCloseWaitsForScheduledPublish(t *testing.T) {
	client := testRedis(t)
	target := &blockingTarget{entered: make(chan struct{}), release: make(chan struct{})}
	a, _ := New(client, target, Options{Prefix: testPrefix(t), Shards: 1, PollInterval: 10 * time.Millisecond})
	m, _ := mq.NewMessage("close", []byte("payload"))
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("target did not enter")
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := a.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close returned early: %v", err)
	}
	close(target.release)
	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("Run after Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed worker did not stop")
	}
}

func TestPublishRetryRepairsMissingDueIndex(t *testing.T) {
	client := testRedis(t)
	a, _ := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 1})
	m, _ := mq.NewMessage("partial-insert", []byte("payload"))
	due := time.Now().Add(time.Hour)
	encoded, err := encodeTask(m, due)
	if err != nil {
		t.Fatal(err)
	}
	keys := a.keys(0)
	if err := client.HSet(context.Background(), keys.records, taskID(m), encoded).Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.PublishAt(context.Background(), m, due); err != nil {
		t.Fatal(err)
	}
	count, err := client.ZCard(context.Background(), keys.due).Result()
	if err != nil || count != 1 {
		t.Fatalf("retry did not restore due index: %d, %v", count, err)
	}
}

func TestInsertWrongTypeDoesNotStrandTask(t *testing.T) {
	client := testRedis(t)
	a, _ := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 1})
	m, _ := mq.NewMessage("bad-due-key", []byte("payload"))
	due := time.Now().Add(time.Hour)
	keys := a.keys(0)
	if err := client.Set(context.Background(), keys.due, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.PublishAt(context.Background(), m, due); !mq.IsOutcomeUnknown(err) {
		t.Fatalf("bad due key outcome = %v", err)
	}
	if err := client.Del(context.Background(), keys.due).Err(); err != nil {
		t.Fatal(err)
	}
	if err := a.PublishAt(context.Background(), m, due); err != nil {
		t.Fatal(err)
	}
	count, err := client.ZCard(context.Background(), keys.due).Result()
	if err != nil || count != 1 {
		t.Fatalf("task stranded after retry: %d, %v", count, err)
	}
}

func TestRunCancellationWhileIdle(t *testing.T) {
	client := testRedis(t)
	a, _ := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 1, PollInterval: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("idle worker ignored cancellation")
	}
}

func TestLaneKeysShareClusterHashTag(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	a, _ := New(client, targetStub{}, Options{Prefix: "mq:test:cluster", Shards: 4})
	for lane := 0; lane < 4; lane++ {
		keys := a.keys(lane)
		parts := []string{keys.due, keys.leased, keys.records, keys.tokens}
		tag := "{" + a.options.Prefix + ":" + strconv.Itoa(lane) + "}"
		for _, key := range parts {
			if !strings.HasPrefix(key, tag) {
				t.Fatalf("key %q lacks lane hash tag %q", key, tag)
			}
		}
	}
}

func TestClusterClientRouting(t *testing.T) {
	addresses := os.Getenv("MQ_TEST_REDIS_CLUSTER_ADDR")
	options := &redis.ClusterOptions{}
	if addresses != "" {
		options.Addrs = strings.Split(addresses, ",")
	} else {
		standalone := os.Getenv("MQ_TEST_REDIS_ADDR")
		if standalone == "" {
			t.Skip("set MQ_TEST_REDIS_ADDR for ClusterClient routing smoke test or MQ_TEST_REDIS_CLUSTER_ADDR for real cluster")
		}
		options.Addrs = []string{standalone}
		options.ClusterSlots = func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: standalone}}}}, nil
		}
	}
	client := redis.NewClusterClient(options)
	defer client.Close()
	a, err := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 4})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("cluster", nil)
	if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claim, err := a.claimOne(context.Background(), a.laneFor(m))
	if err != nil || claim == nil || claim.message.ID != m.ID {
		t.Fatalf("cluster claim = %+v, %v", claim, err)
	}
	complete, err := a.complete(context.Background(), claim)
	if err != nil || !complete {
		t.Fatalf("cluster complete = %t, %v", complete, err)
	}
}

func TestClaimErrorsPreserveRecoverableIndex(t *testing.T) {
	t.Run("fresh due with bad leased key", func(t *testing.T) {
		client := testRedis(t)
		a, _ := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 1})
		m, _ := mq.NewMessage("bad-leased-key", nil)
		if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		keys := a.keys(0)
		if err := client.Set(context.Background(), keys.leased, "wrong-type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := a.claimOne(context.Background(), 0); err == nil {
			t.Fatal("bad leased key accepted")
		}
		if err := client.Del(context.Background(), keys.leased).Err(); err != nil {
			t.Fatal(err)
		}
		claim, err := a.claimOne(context.Background(), 0)
		if err != nil || claim == nil || claim.message.ID != m.ID {
			t.Fatalf("due task stranded: %+v, %v", claim, err)
		}
	})
	t.Run("expired lease with bad token key", func(t *testing.T) {
		client := testRedis(t)
		a, _ := New(client, targetStub{}, Options{Prefix: testPrefix(t), Shards: 1, LeaseDuration: 30 * time.Millisecond})
		m, _ := mq.NewMessage("bad-token-key", nil)
		if err := a.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		first, err := a.claimOne(context.Background(), 0)
		if err != nil || first == nil {
			t.Fatalf("initial claim: %+v, %v", first, err)
		}
		keys := a.keys(0)
		if err := client.Del(context.Background(), keys.tokens).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.Set(context.Background(), keys.tokens, "wrong-type", 0).Err(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		if _, err := a.claimOne(context.Background(), 0); err == nil {
			t.Fatal("bad token key accepted")
		}
		if err := client.Del(context.Background(), keys.tokens).Err(); err != nil {
			t.Fatal(err)
		}
		recovered, err := a.claimOne(context.Background(), 0)
		if err != nil || recovered == nil || recovered.message.ID != m.ID {
			t.Fatalf("expired task stranded: %+v, %v", recovered, err)
		}
	})
}
