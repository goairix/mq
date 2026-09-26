package rabbitdelay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingTarget struct {
	delivered chan mq.Message
	err       error
}

type blockingTarget struct{ entered, release chan struct{} }

func (t *blockingTarget) Publish(context.Context, mq.Message) error {
	close(t.entered)
	<-t.release
	return nil
}
func (*blockingTarget) Close(context.Context) error { return nil }

type selectiveTarget struct {
	failedID  string
	delivered chan mq.Message
}

type closingTarget struct {
	conn      *amqp.Connection
	delivered chan mq.Message
}

func (t *closingTarget) Publish(_ context.Context, message mq.Message) error {
	t.delivered <- message
	_ = t.conn.Close()
	return nil
}
func (*closingTarget) Close(context.Context) error { return nil }

func (t *selectiveTarget) Publish(_ context.Context, message mq.Message) error {
	if message.ID == t.failedID {
		return errors.New("target unavailable for this message")
	}
	t.delivered <- message
	return nil
}
func (*selectiveTarget) Close(context.Context) error { return nil }

func (t *recordingTarget) Publish(_ context.Context, message mq.Message) error {
	if t.err != nil {
		return t.err
	}
	t.delivered <- message
	return nil
}
func (*recordingTarget) Close(context.Context) error { return nil }

func testConnection(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("MQ_TEST_RABBIT_URL")
	if url == "" {
		t.Skip("set MQ_TEST_RABBIT_URL")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestBucketSelection(t *testing.T) {
	for _, tc := range []struct{ remaining, want time.Duration }{
		{time.Millisecond, 100 * time.Millisecond}, {100 * time.Millisecond, 100 * time.Millisecond},
		{1500 * time.Millisecond, time.Second}, {2 * time.Hour, time.Hour},
	} {
		if got := chooseBucket(tc.remaining); got != tc.want {
			t.Fatalf("chooseBucket(%s) = %s, want %s", tc.remaining, got, tc.want)
		}
	}
}

func TestScheduledMessageIsNotEarlyAndSurvivesWorkerRestart(t *testing.T) {
	conn := testConnection(t)
	target := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-test-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond, RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	m, _ := mq.NewMessage("scheduled", []byte("hello"))
	m.Key = []byte{1, 0, 255}
	m.Headers = map[string]string{"trace": "abc"}
	due := time.Now().Add(180 * time.Millisecond)
	if err := s.PublishAt(context.Background(), m, due); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-target.delivered:
		t.Fatal("message published early")
	case <-time.After(90 * time.Millisecond):
	}
	cancel()
	<-done
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-target.delivered:
		if time.Now().Before(due) {
			t.Fatal("early target publish")
		}
		if got.ID != m.ID || got.Topic != m.Topic || string(got.Payload) != "hello" || got.Headers["trace"] != "abc" || string(got.Key) != string(m.Key) {
			t.Fatalf("message changed: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("delayed message not released")
	}
	cancel()
	<-done
}

func TestFailedTargetRetainsReleaseMessage(t *testing.T) {
	conn := testConnection(t)
	target := &recordingTarget{delivered: make(chan mq.Message, 1), err: errors.New("target unavailable")}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-fail-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond, RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	m, _ := mq.NewMessage("scheduled", []byte("retry"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	<-ctx.Done()
	<-done
	cancel()
	target.err = nil
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-target.delivered:
		if got.ID != m.ID {
			t.Fatal("ID changed")
		}
	case <-ctx.Done():
		t.Fatal("failed target lost scheduled message")
	}
	cancel()
	<-done
}

func TestLateTargetSuccessIsNotAcked(t *testing.T) {
	conn := testConnection(t)
	target := &blockingTarget{entered: make(chan struct{}), release: make(chan struct{})}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-late-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond, DrainTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	m, _ := mq.NewMessage("late-target", []byte("retry"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("target not called")
	}
	cancel()
	time.Sleep(40 * time.Millisecond)
	close(target.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	deadline := time.Now().Add(time.Second)
	for {
		d, ok, err := ch.Get(s.releaseQueue(), true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if d.MessageId != m.ID {
				t.Fatal("redelivery ID changed")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late success ACKed release message")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBadReleaseMessageIsQuarantinedWithoutBlocking(t *testing.T) {
	conn := testConnection(t)
	target := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-corrupt-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.PublishWithContext(context.Background(), "", s.releaseQueue(), true, false, amqp.Publishing{Body: []byte("not-json"), MessageId: "corrupt", DeliveryMode: amqp.Persistent}); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("healthy", []byte("ok"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-target.delivered:
		if got.ID != m.ID {
			t.Fatal("wrong delivery")
		}
	case <-ctx.Done():
		t.Fatal("corrupt release blocked healthy message")
	}
	cancel()
	<-done
	d, ok, err := ch.Get(s.failedQueue(), true)
	if err != nil || !ok || d.MessageId != "corrupt" || string(d.Body) != "not-json" {
		t.Fatalf("quarantine = %v, %t, %v", d.MessageId, ok, err)
	}
}

func TestFailedTargetDoesNotBlockLaterRelease(t *testing.T) {
	conn := testConnection(t)
	bad, _ := mq.NewMessage("bad-target", []byte("bad"))
	good, _ := mq.NewMessage("good-target", []byte("good"))
	target := &selectiveTarget{failedID: bad.ID, delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-no-hol-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond, RetryMin: time.Second, RetryMax: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	for _, m := range []mq.Message{bad, good} {
		if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-target.delivered:
		if got.ID != good.ID {
			t.Fatalf("delivered %s", got.ID)
		}
	case <-ctx.Done():
		t.Fatal("failed target blocked later release")
	}
	cancel()
	<-done
}

func TestConfirmedTargetBeforeReleaseAckCanDuplicate(t *testing.T) {
	conn := testConnection(t)
	first := &closingTarget{conn: conn, delivered: make(chan mq.Message, 1)}
	prefix := "mq-v2-delay-ack-window-" + time.Now().Format("150405.000000000")
	s, err := New(conn, first, Options{Prefix: prefix, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("duplicate", []byte("same-id"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-first.delivered:
		if got.ID != m.ID {
			t.Fatal("first ID changed")
		}
	case <-ctx.Done():
		t.Fatal("first target publish missing")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("worker did not detect lost ACK connection")
	}
	cancel()
	_ = s.Close(context.Background())
	url := os.Getenv("MQ_TEST_RABBIT_URL")
	secondConn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer secondConn.Close()
	second := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err = New(secondConn, second, Options{Prefix: prefix, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-second.delivered:
		if got.ID != m.ID {
			t.Fatal("redelivery ID changed")
		}
	case <-ctx.Done():
		t.Fatal("unacked release was lost")
	}
	cancel()
	<-done
}

func TestMissingBucketRouteIsRejected(t *testing.T) {
	conn := testConnection(t)
	target := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-missing-bucket-" + time.Now().Format("150405.000000000")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if _, err := ch.QueueDelete(s.bucketQueue(100*time.Millisecond), false, false, false); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("missing-route", []byte("reject"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); !errors.Is(err, mq.ErrNoRoute) {
		t.Fatalf("missing bucket route = %v", err)
	}
}

func TestMissingReleaseRouteRetainsExpiredBucket(t *testing.T) {
	if os.Getenv("MQ_TEST_RABBIT_ROUTE_FAULT") != "1" {
		t.Skip("set MQ_TEST_RABBIT_ROUTE_FAULT=1 for periodic RabbitMQ DLX retry")
	}
	management := os.Getenv("MQ_TEST_RABBIT_MGMT_URL")
	if management == "" {
		t.Skip("set MQ_TEST_RABBIT_MGMT_URL to inspect retained DLX messages")
	}
	conn := testConnection(t)
	target := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: "mq-v2-delay-route-" + time.Now().Format("150405.000000000"), PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	m, _ := mq.NewMessage("route", []byte("held"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.QueueUnbind(s.releaseQueue(), "ready", s.releaseExchange(), nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	deadline := time.Now().Add(12 * time.Second)
	for {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, management+"/api/queues/%2F/"+url.PathEscape(s.bucketQueue(100*time.Millisecond)), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.SetBasicAuth("guest", "guest")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			t.Fatalf("management status = %d", response.StatusCode)
		}
		var state struct {
			MessagesDLX int64 `json:"messages_dlx"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&state)
		response.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if state.MessagesDLX > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired message was not retained while release route was missing")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := ch.QueueBind(s.releaseQueue(), "ready", s.releaseExchange(), false, nil); err != nil {
		t.Fatal(err)
	}
}
