package rabbitadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestPublishConfirmedAndUnroutable(t *testing.T) {
	conn := rabbitConnection(t)
	a, err := New(conn, Options{Prefix: "mq-v2-publish-" + time.Now().Format("150405.000000000"), PublishChannels: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "orders.created", Name: "billing"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte{0, 255, 1})
	m.Key = []byte{255, 0, 1}
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	d, ok, err := ch.Get(a.queueName(sub), true)
	if err != nil || !ok {
		t.Fatalf("confirmed message missing: %t, %v", ok, err)
	}
	got, err := decode(d)
	if err != nil || got.ID != m.ID || string(got.Payload) != string(m.Payload) || string(got.Key) != string(m.Key) {
		t.Fatalf("delivery = %+v, %v", got, err)
	}
	unroutable, _ := mq.NewMessage("no.binding", nil)
	if err := a.Publish(context.Background(), unroutable); !errors.Is(err, mq.ErrNoRoute) {
		t.Fatalf("unroutable publish = %v", err)
	}
}

func TestPublishBatchOrderedResults(t *testing.T) {
	conn := rabbitConnection(t)
	a, err := New(conn, Options{Prefix: "mq-v2-batch-publish-" + time.Now().Format("150405.000000000"), PublishChannels: 2, PublishBatchSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "events", Name: "audit"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	first, _ := mq.NewMessage(sub.Topic, []byte("first"))
	third, _ := mq.NewMessage(sub.Topic, []byte("third"))
	results := a.PublishBatch(context.Background(), []mq.Message{first, {}, third})
	if err := mq.ValidatePublishResults(3, results); err != nil {
		t.Fatal(err)
	}
	if results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected || results[2].State != mq.PublishAccepted {
		t.Fatalf("results = %+v", results)
	}
	missing, _ := mq.NewMessage("no.route", nil)
	results = a.PublishBatch(context.Background(), []mq.Message{first, missing})
	if results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected || !errors.Is(results[1].Err, mq.ErrNoRoute) {
		t.Fatalf("return results = %+v", results)
	}
}

func TestPublishClosedConnectionIsUnknown(t *testing.T) {
	conn := rabbitConnection(t)
	a, err := New(conn, Options{Prefix: "mq-v2-unknown-" + time.Now().Format("150405.000000000")})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("unavailable", nil)
	if err := a.Publish(context.Background(), m); !mq.IsOutcomeUnknown(err) {
		t.Fatalf("closed connection outcome = %v", err)
	}
}
