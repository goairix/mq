package rabbitadapter

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func rabbitConnection(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("MQ_TEST_RABBIT_URL")
	if url == "" {
		t.Skip("set MQ_TEST_RABBIT_URL for RabbitMQ 3.13 integration")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestNewValidatesOptions(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("nil RabbitMQ connection accepted")
	}
	conn := rabbitConnection(t)
	for _, opts := range []Options{{PublishChannels: -1}, {PublishBatchSize: -1}, {Prefetch: -1}, {DrainTimeout: -time.Second}, {Exchange: " "}} {
		if _, err := New(conn, opts); err == nil {
			t.Fatalf("invalid options accepted: %+v", opts)
		}
	}
}

func TestWirePreservesEnvelope(t *testing.T) {
	m, _ := mq.NewMessage("领域.事件", []byte{0, 255, 1})
	m.Key = []byte{255, 0, 1}
	m.Headers = map[string]string{"trace": "链路"}
	p := encode(m)
	d := amqp.Delivery{MessageId: p.MessageId, Body: p.Body, Headers: p.Headers}
	got, err := decode(d)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Topic != m.Topic || !bytes.Equal(got.Key, m.Key) || !bytes.Equal(got.Payload, m.Payload) || got.Headers["trace"] != m.Headers["trace"] || !got.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("round trip = %+v", got)
	}
	d.Headers["mq.v"] = int32(99)
	if _, err := decode(d); err == nil {
		t.Fatal("unknown wire version accepted")
	}
}

func TestPrepareDurableFanoutTopology(t *testing.T) {
	conn := rabbitConnection(t)
	a, err := New(conn, Options{Prefix: "mq-v2-test-" + time.Now().Format("150405.000000000")})
	if err != nil {
		t.Fatal(err)
	}
	subA := mq.Subscription{Topic: "events.*.literal", Name: "billing"}
	subB := mq.Subscription{Topic: subA.Topic, Name: "audit"}
	for _, sub := range []mq.Subscription{subA, subB} {
		if err := a.Prepare(context.Background(), sub); err != nil {
			t.Fatal(err)
		}
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if _, err := ch.QueueInspect(a.queueName(subA)); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueInspect(a.queueName(subB)); err != nil {
		t.Fatal(err)
	}
	if err := ch.PublishWithContext(context.Background(), a.options.Exchange, a.routeKey(subA.Topic), false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{a.queueName(subA), a.queueName(subB)} {
		deadline := time.Now().Add(time.Second)
		for {
			d, ok, err := ch.Get(queue, true)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				if string(d.Body) != "one" {
					t.Fatalf("delivery = %q", d.Body)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("queue %s not routed", queue)
			}
			time.Sleep(time.Millisecond)
		}
	}
}
