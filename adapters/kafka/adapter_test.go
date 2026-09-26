package kafkaadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type closedHook struct{ done chan struct{} }

func (h closedHook) OnClientClosed(*kgo.Client) { close(h.done) }

func TestTimedOutCloseStillReleasesProducerAfterDrain(t *testing.T) {
	closed := make(chan struct{})
	a, err := New([]string{"127.0.0.1:1"}, Options{ClientOptions: []kgo.Opt{kgo.WithHooks(closedHook{done: closed})}})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.begin(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := a.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close with active operation = %v", err)
	}
	a.end()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("producer remained open after final operation drained")
	}
}

func TestClientOptionsCannotDisableReliabilityInvariants(t *testing.T) {
	for name, options := range map[string][]kgo.Opt{
		"idempotency":         {kgo.DisableIdempotentWrite()},
		"auto topic creation": {kgo.AllowAutoTopicCreation()},
		"topic override":      {kgo.DefaultProduceTopic("other"), kgo.DefaultProduceTopicAlways()},
		"transactional":       {kgo.TransactionalID("unexpected")},
		"manual flushing":     {kgo.ManualFlushing()},
	} {
		t.Run(name, func(t *testing.T) {
			a, err := New([]string{"127.0.0.1:1"}, Options{ClientOptions: options})
			if err == nil {
				a.Close(context.Background())
				t.Fatal("option bypassed Kafka reliability invariant")
			}
		})
	}
}

func TestCapacityRequiresDeadLetterHeadroom(t *testing.T) {
	a, err := New([]string{"127.0.0.1:1"}, Options{BatchMaxBytes: 512, MaxMessageBytes: 512, MaxBufferedBytes: 512})
	if err == nil {
		a.Close(context.Background())
		t.Fatal("producer capacity leaves no DLQ header headroom")
	}
}
