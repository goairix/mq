package kafkaadapter

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWireRoundTrip(t *testing.T) {
	m, _ := mq.NewMessage("trace.events", []byte{0, 255, 7})
	m.Key = []byte{255, 1}
	m.Headers = map[string]string{"traceparent": "00-abc", "name": "你好"}
	r := encode(m)
	got, err := decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Topic != m.Topic || !bytes.Equal(got.Payload, m.Payload) || !bytes.Equal(got.Key, m.Key) || got.Headers["name"] != "你好" || !got.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("round trip = %+v", got)
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: "mq.v", Value: []byte("9")})
	if _, err := decode(r); err == nil {
		t.Fatal("duplicate envelope version accepted")
	}
}

func TestWireRejectsMissingVersion(t *testing.T) {
	if _, err := decode(&kgo.Record{Topic: "t", Timestamp: time.Now()}); err == nil {
		t.Fatal("missing envelope version accepted")
	}
}
