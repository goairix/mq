package redisdelay

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

type targetStub struct{}

func (targetStub) Publish(context.Context, mq.Message) error { return nil }
func (targetStub) Close(context.Context) error               { return nil }

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
	a, err := New(client, targetStub{}, Options{Prefix: "mq:test:delay:" + t.Name(), Shards: 2})
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
