package rabbitadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestConsumerAcknowledgesSuccessAndRetriesTransient(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-consume-" + time.Now().Format("150405.000000000"), RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "work", Name: "workers"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("work"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	got := make(chan string, 1)
	attempts := 0
	go func() {
		done <- a.Run(ctx, sub, func(_ context.Context, msg mq.Message) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient")
			}
			got <- msg.ID
			return nil
		})
	}()
	select {
	case id := <-got:
		if id != m.ID {
			t.Fatal("message ID changed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message not retried")
	}
	if attempts < 2 {
		t.Fatal("transient error not retried")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if q, err := ch.QueueInspect(a.queueName(sub)); err != nil || q.Messages != 0 {
		t.Fatalf("source queue = %+v, %v", q, err)
	}
}

func TestConsumerPermanentFailureGoesToConfirmedDLQ(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-dlq-" + time.Now().Format("150405.000000000")})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "events", Name: "audit"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("bad"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(context.Context, mq.Message) error { return mq.Permanent(errors.New("bad schema")) })
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		d, ok, err := ch.Get(a.DeadLetterQueue(sub), true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID || string(d.Body) != "bad" {
				t.Fatalf("DLQ record = %+v", d)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dead letter missing")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestConsumerCancelDrainsCompletedHandler(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-drain-" + time.Now().Format("150405.000000000"), DrainTimeout: time.Second})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "drain", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, nil)
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(handlerCtx context.Context, _ mq.Message) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not drain")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if q, err := ch.QueueInspect(a.queueName(sub)); err != nil || q.Messages != 0 {
		t.Fatalf("unacked after drain = %+v, %v", q, err)
	}
}

func TestDeadLetterRouteFailureLeavesSourceUnacknowledged(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-dlq-fail-" + time.Now().Format("150405.000000000")})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "poison", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("bad"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(context.Context, mq.Message) error {
			close(entered)
			<-release
			return mq.Permanent(errors.New("bad"))
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if _, err := ch.QueueDelete(a.DeadLetterQueue(sub), false, false, false); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrNoRoute) {
			t.Fatalf("DLQ failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DLQ failure hidden")
	}
	deadline := time.Now().Add(time.Second)
	for {
		d, ok, err := ch.Get(a.queueName(sub), true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID {
				t.Fatal("source ID changed")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("source message lost after DLQ failure")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConsumerDrainExpiryRedelivers(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-drain-expiry-" + time.Now().Format("150405.000000000"), DrainTimeout: 30 * time.Millisecond})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "drain-expiry", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, nil)
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(handlerCtx context.Context, _ mq.Message) error {
			close(entered)
			<-handlerCtx.Done()
			return handlerCtx.Err()
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain deadline ignored")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	deadline := time.Now().Add(time.Second)
	for {
		d, ok, err := ch.Get(a.queueName(sub), true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID {
				t.Fatal("redelivery changed ID")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unfinished message not redelivered")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConsumerDoesNotAckLateSuccess(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-late-success-" + time.Now().Format("150405.000000000"), DrainTimeout: 20 * time.Millisecond})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "late", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("late"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(context.Context, mq.Message) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	time.Sleep(40 * time.Millisecond)
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	deadline := time.Now().Add(time.Second)
	for {
		d, ok, err := ch.Get(a.queueName(sub), true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID {
				t.Fatal("wrong redelivery")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late success was ACKed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCloseStopsTransientRetry(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-close-retry-" + time.Now().Format("150405.000000000"), RetryMin: time.Hour, RetryMax: time.Hour})
	sub := mq.Subscription{Topic: "retry", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, nil)
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	entered, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- a.Run(context.Background(), sub, func(context.Context, mq.Message) error { close(entered); return errors.New("retry") })
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
