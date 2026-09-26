package rabbitadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/goairix/mq/v2/contracttest"
)

func TestPortableContract(t *testing.T) {
	management := os.Getenv("MQ_TEST_RABBIT_MGMT_URL")
	if management == "" {
		t.Skip("set MQ_TEST_RABBIT_MGMT_URL for RabbitMQ pending-count contract")
	}
	contracttest.Run(t, func(t *testing.T) contracttest.Transport {
		conn := rabbitConnection(t)
		a, err := New(conn, Options{Prefix: "mq-v2-contract-" + time.Now().Format("150405.000000000"), RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond, PollInterval: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return contracttest.Transport{Publisher: a, Subscriber: a, BatchSubscriber: a, Prepare: a.Prepare,
			Outstanding: func(ctx context.Context, sub mq.Subscription) (int64, error) {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, management+"/api/queues/%2F/"+url.PathEscape(a.queueName(sub)), nil)
				if err != nil {
					return 0, err
				}
				request.SetBasicAuth("guest", "guest")
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					return 0, err
				}
				defer response.Body.Close()
				if response.StatusCode != http.StatusOK {
					return 0, fmt.Errorf("management queue status %d", response.StatusCode)
				}
				var state struct {
					Messages int64 `json:"messages"`
				}
				if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
					return 0, err
				}
				return state.Messages, nil
			},
		}
	})
}

func TestBatchPartialResultKeepsFailedMessage(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-batch-consume-" + time.Now().Format("150405.000000000"), RetryMin: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "traces", Name: "clickhouse"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	first, _ := mq.NewMessage(sub.Topic, []byte("first"))
	second, _ := mq.NewMessage(sub.Topic, []byte("second"))
	for _, m := range []mq.Message{first, second} {
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxMessages = 2
	opts.MaxWait = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	batches := make(chan []string, 2)
	go func() {
		done <- a.RunBatch(ctx, sub, opts, func(_ context.Context, batch []mq.Message) ([]error, error) {
			ids := make([]string, len(batch))
			for i, m := range batch {
				ids[i] = m.ID
			}
			batches <- ids
			if len(batch) == 2 {
				return []error{errors.New("retry"), nil}, nil
			}
			return nil, nil
		})
	}()
	select {
	case ids := <-batches:
		if len(ids) != 2 || ids[0] != first.ID || ids[1] != second.ID {
			t.Fatalf("first batch = %v", ids)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not coalesce")
	}
	select {
	case ids := <-batches:
		if len(ids) != 1 || ids[0] != first.ID {
			t.Fatalf("retry batch = %v", ids)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed item not retried")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch did not stop")
	}
}

func TestBatchHardByteLimitRejectsBeforeHandler(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-batch-limit-" + time.Now().Format("150405.000000000")})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "large", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, make([]byte, 1024))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	opts := mq.DefaultBatchOptions()
	opts.MaxBytes = 16
	opts.MaxInFlightBytes = 32
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := a.RunBatch(ctx, sub, opts, func(context.Context, []mq.Message) ([]error, error) {
		t.Error("oversize handler invoked")
		return nil, nil
	})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hard limit error = %v", err)
	}
}

func TestBatchZeroWaitDrainsReadyMessages(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-zero-wait-" + time.Now().Format("150405.000000000")})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "batch-ready", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		m, _ := mq.NewMessage(sub.Topic, []byte{byte(i)})
		if err := a.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	options := mq.DefaultBatchOptions()
	options.MaxMessages = 3
	options.MaxWait = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sizes := make(chan int, 1)
	go func() {
		_ = a.RunBatch(ctx, sub, options, func(_ context.Context, messages []mq.Message) ([]error, error) {
			sizes <- len(messages)
			cancel()
			return nil, nil
		})
	}()
	select {
	case n := <-sizes:
		if n != 3 {
			t.Fatalf("batch size = %d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("batch missing")
	}
}

func TestBatchDoesNotAckLateSuccess(t *testing.T) {
	conn := rabbitConnection(t)
	a, _ := New(conn, Options{Prefix: "mq-v2-batch-late-" + time.Now().Format("150405.000000000"), DrainTimeout: 20 * time.Millisecond})
	defer a.Close(context.Background())
	sub := mq.Subscription{Topic: "batch-late", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("late"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	options := mq.DefaultBatchOptions()
	options.MaxWait = 0
	go func() {
		done <- a.RunBatch(ctx, sub, options, func(context.Context, []mq.Message) ([]error, error) { close(entered); <-release; return nil, nil })
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
		t.Fatal("batch did not stop")
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
			t.Fatal("late batch success was ACKed")
		}
		time.Sleep(time.Millisecond)
	}
}
