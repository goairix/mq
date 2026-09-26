package kafkaadapter

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSourceTopicWithDeadLetterSuffixKeepsSourceBatchLimit(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{BatchMaxBytes: 512, MaxMessageBytes: 512, MaxBufferedBytes: 8192, ClientOptions: []kgo.Opt{kgo.ProducerLinger(100 * time.Millisecond)}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.suffix.%d.dlq", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request := kmsg.NewCreateTopicsRequest()
	created := kmsg.NewCreateTopicsRequestTopic()
	created.Topic, created.NumPartitions, created.ReplicationFactor = topic, 1, 1
	limit := "512"
	created.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "max.message.bytes", Value: &limit}}
	request.Topics = []kmsg.CreateTopicsRequestTopic{created}
	response, err := request.RequestWith(ctx, a.producer)
	if err != nil || len(response.Topics) != 1 || response.Topics[0].ErrorCode != 0 {
		t.Fatalf("create limited topic: response=%+v error=%v", response, err)
	}
	const count = 4
	start := make(chan struct{})
	results := make([]error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		m, err := mq.NewMessage(topic, make([]byte, 300))
		if err != nil {
			t.Fatal(err)
		}
		m.Key = []byte("same")
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = a.Publish(ctx, m)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range results {
		if err != nil {
			t.Fatalf("suffix source record %d: %v", i, err)
		}
	}
}

func TestConcurrentSourcePublishesRespectBatchCap(t *testing.T) {
	brokers := testBrokers(t)
	a, err := New(brokers, Options{ClientOptions: []kgo.Opt{kgo.ProducerLinger(100 * time.Millisecond)}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	topic := fmt.Sprintf("mq.v2.concurrent.cap.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := createContractTopics(ctx, a.producer, topic); err != nil {
		t.Fatal(err)
	}
	const count = 4
	messages := make([]mq.Message, count)
	for i := range messages {
		payload := make([]byte, 600<<10)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		messages[i], err = mq.NewMessage(topic, payload)
		if err != nil {
			t.Fatal(err)
		}
		messages[i].Key = []byte("one-partition")
	}
	start := make(chan struct{})
	results := make([]error, count)
	var wg sync.WaitGroup
	for i := range messages {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = a.Publish(ctx, messages[i])
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range results {
		if err != nil {
			t.Fatalf("source record %d: %v", i, err)
		}
	}
}

func testBrokers(t *testing.T) []string {
	t.Helper()
	value := os.Getenv("MQ_TEST_KAFKA_BROKERS")
	if value == "" {
		t.Skip("set MQ_TEST_KAFKA_BROKERS")
	}
	return strings.Split(value, ",")
}

func TestPublishBatchPreservesOrderedOutcomes(t *testing.T) {
	brokers := testBrokers(t)
	topic := os.Getenv("MQ_TEST_KAFKA_TOPIC")
	if topic == "" {
		t.Skip("set MQ_TEST_KAFKA_TOPIC to an existing Kafka topic")
	}
	a, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	first, _ := mq.NewMessage(topic, []byte("first"))
	last, _ := mq.NewMessage(topic, []byte("last"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := a.PublishBatch(ctx, []mq.Message{first, {}, last})
	if err := mq.ValidatePublishResults(3, results); err != nil {
		t.Fatal(err)
	}
	if results[0].State != mq.PublishAccepted || results[1].State != mq.PublishRejected || results[2].State != mq.PublishAccepted {
		t.Fatalf("results = %+v", results)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	seen := map[string]bool{}
	for len(seen) < 2 {
		fetches := client.PollRecords(ctx, 100)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fetches.Records() {
			if r.Topic == topic && (string(r.Value) == "first" || string(r.Value) == "last") {
				m, err := decode(r)
				if err != nil {
					t.Fatal(err)
				}
				seen[m.ID] = true
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	if !seen[first.ID] || !seen[last.ID] {
		t.Fatalf("seen IDs = %v", seen)
	}
}
