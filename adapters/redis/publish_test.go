package redisadapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func integrationClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("MQ_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set MQ_TEST_REDIS_ADDR for Redis integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func integrationAdapter(t *testing.T) (*Adapter, *redis.Client) {
	t.Helper()
	client := integrationClient(t)
	prefix := "mq:v2:test:" + strings.ReplaceAll(t.Name(), "/", ":") + ":" + time.Now().Format("150405.000000000")
	adapter, err := New(client, Options{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	return adapter, client
}

func TestPublishConfirmed(t *testing.T) {
	a, client := integrationAdapter(t)
	m, _ := mq.NewMessage("trace/链路", []byte{0, 255, 1})
	m.Key = []byte{0, 255}
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	entries, err := client.XRange(context.Background(), a.streamKey(m.Topic), "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("XRange = %+v, %v", entries, err)
	}
	decoded, err := decode(entries[0])
	if err != nil || decoded.ID != m.ID || string(decoded.Payload) != string(m.Payload) {
		t.Fatalf("stored message = %+v, %v", decoded, err)
	}
}

func TestPublishBatchResults(t *testing.T) {
	a, client := integrationAdapter(t)
	first, _ := mq.NewMessage("batch", []byte("a"))
	last, _ := mq.NewMessage("batch", []byte("b"))
	results := a.PublishBatch(context.Background(), []mq.Message{first, {}, last})
	if err := mq.ValidatePublishResults(3, results); err != nil {
		t.Fatal(err)
	}
	if results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected || results[2].State != mq.PublishAccepted {
		t.Fatalf("batch results = %+v", results)
	}
	count, err := client.XLen(context.Background(), a.streamKey("batch")).Result()
	if err != nil || count != 2 {
		t.Fatalf("stream length %d, %v", count, err)
	}
}

func TestPublishTransportFailureIsUnknown(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 100 * time.Millisecond})
	defer client.Close()
	a, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("failed", nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = a.Publish(ctx, m)
	if !mq.IsOutcomeUnknown(err) {
		t.Fatalf("publish error = %v", err)
	}
}
