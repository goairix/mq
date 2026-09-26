package kafkaadapter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Set MQ_TEST_KAFKA_DOCKER_RESTART to a disposable Kafka container name.
// This test intentionally restarts the broker and must run without parallel tests.
func TestBrokerRestartPreservesConfirmedKafkaMessage(t *testing.T) {
	container := os.Getenv("MQ_TEST_KAFKA_DOCKER_RESTART")
	if container == "" {
		t.Skip("set MQ_TEST_KAFKA_DOCKER_RESTART")
	}
	brokers := testBrokers(t)
	a, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.restart.%d", time.Now().UnixNano())
	sub := mq.Subscription{Topic: topic, Name: "restart"}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic, topic+a.options.DLQSuffix); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(topic, []byte("survive-restart"))
	if err := a.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.CommandContext(ctx, "docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("restart Kafka: %v: %s", err, output)
	}
	probe, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	for {
		pingCtx, pingCancel := context.WithTimeout(ctx, time.Second)
		err := probe.Ping(pingCtx)
		pingCancel()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Kafka did not recover: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	fresh, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close(context.Background())
	runCtx, stop := context.WithCancel(ctx)
	seen := false
	err = fresh.Run(runCtx, sub, func(_ context.Context, got mq.Message) error {
		seen = got.ID == m.ID && string(got.Payload) == string(m.Payload)
		stop()
		return nil
	})
	if !seen {
		t.Fatalf("confirmed message missing after restart: %v", err)
	}
}
