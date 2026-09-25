package redisadapter

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func TestWireRoundTrip(t *testing.T) {
	m, err := mq.NewMessage("trace/链路", []byte{0, 255, 1})
	if err != nil {
		t.Fatal(err)
	}
	m.Key = []byte{0, 128, 255}
	m.Headers = map[string]string{"trace": "你好🌟"}
	decoded, err := decode(redis.XMessage{ID: "1-0", Values: encode(m)})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != m.ID || decoded.Topic != m.Topic || !bytes.Equal(decoded.Key, m.Key) || !bytes.Equal(decoded.Payload, m.Payload) || decoded.Headers["trace"] != m.Headers["trace"] || !decoded.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("round trip mismatch: %+v", decoded)
	}
	m.Payload = nil
	decoded, err = decode(redis.XMessage{ID: "2-0", Values: encode(m)})
	if err != nil || len(decoded.Payload) != 0 {
		t.Fatalf("empty payload decode: %+v, %v", decoded, err)
	}
}

func TestWireRejectsMalformed(t *testing.T) {
	m, _ := mq.NewMessage("orders", nil)
	cases := []struct {
		name   string
		change func(map[string]any)
	}{
		{"version", func(v map[string]any) { v["v"] = "99" }},
		{"missing ID", func(v map[string]any) { delete(v, "id") }},
		{"timestamp", func(v map[string]any) { v["created_sec"] = "not-a-timestamp" }},
		{"headers", func(v map[string]any) { v["headers"] = "{" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := encode(m)
			tc.change(values)
			if _, err := decode(redis.XMessage{ID: "1-0", Values: values}); err == nil {
				t.Fatal("accepted malformed wire message")
			}
		})
	}
}

func TestWirePreservesHistoricalCreationTime(t *testing.T) {
	m, _ := mq.NewMessage("history", nil)
	m.CreatedAt = time.Date(1500, time.January, 2, 3, 4, 5, 6, time.UTC)
	decoded, err := decode(redis.XMessage{ID: "1-0", Values: encode(m)})
	if err != nil || !decoded.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("created at = %v, error = %v", decoded.CreatedAt, err)
	}
}
