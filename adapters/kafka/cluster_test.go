package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestClusterLeaderFailover is opt-in because it kills one broker in a
// disposable three-node KRaft cluster created by tests/cluster/kafka.sh.
func TestClusterLeaderFailover(t *testing.T) {
	runID := os.Getenv("MQ_TEST_CLUSTER_RUN_ID")
	seed := os.Getenv("MQ_TEST_KAFKA_CLUSTER_BROKERS")
	if runID == "" && seed == "" {
		t.Skip("run tests/cluster/run.sh kafka to enable cluster fault injection")
	}
	if runID == "" || seed == "" {
		t.Fatal("both MQ_TEST_CLUSTER_RUN_ID and MQ_TEST_KAFKA_CLUSTER_BROKERS are required")
	}
	if !strings.HasPrefix(runID, "mq-v2-cluster-") {
		t.Fatal("refusing to kill a container outside a disposable mq-v2 cluster")
	}
	brokers := strings.Split(seed, ",")
	if len(brokers) != 3 {
		t.Fatalf("need three Kafka broker addresses, got %d", len(brokers))
	}
	adapter, err := New(brokers, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	topic := fmt.Sprintf("mq.v2.cluster.%d", time.Now().UnixNano())
	if err := createClusterTopic(ctx, adapter, topic); err != nil {
		t.Fatal(err)
	}
	if _, err := waitClusterPartition(ctx, adapter, topic, 3, -1); err != nil {
		t.Fatal(err)
	}
	first := clusterMessages(t, topic, "before", 10)
	for _, message := range first {
		if err := adapter.Publish(ctx, message); err != nil {
			t.Fatalf("publish before failover: %v", err)
		}
	}
	if err := warmClusterGroup(ctx, adapter, topic); err != nil {
		t.Fatal(err)
	}
	// Select the victim after all setup and confirmed writes. A leader read
	// before those operations could become stale without a fault injection.
	leader, err := waitClusterPartition(ctx, adapter, topic, 3, -1)
	if err != nil {
		t.Fatal(err)
	}
	container := fmt.Sprintf("%s-k%d", runID, leader)
	if err := killOwnedKafkaNode(ctx, runID, container); err != nil {
		t.Fatal(err)
	}
	if _, err := waitClusterPartition(ctx, adapter, topic, 2, leader); err != nil {
		t.Fatal(err)
	}
	second := clusterMessages(t, topic, "after", 10)
	for _, message := range second {
		if err := adapter.Publish(ctx, message); err != nil {
			t.Fatalf("publish after leader failover: %v", err)
		}
	}
	want := make(map[string]bool, len(first)+len(second))
	for _, message := range append(first, second...) {
		want[message.ID] = false
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var restarts int
	errUnexpected := errors.New("unexpected Kafka message")
consumeLoop:
	for {
		err = adapter.Run(runCtx, mq.Subscription{Topic: topic, Name: "failover"}, func(_ context.Context, message mq.Message) error {
			if _, known := want[message.ID]; !known {
				return fmt.Errorf("%w %s", errUnexpected, message.ID)
			}
			want[message.ID] = true
			for _, seen := range want {
				if !seen {
					return nil
				}
			}
			stop()
			return nil
		})
		if errors.Is(err, context.Canceled) || runCtx.Err() != nil {
			break
		}
		if err == nil {
			t.Fatal("Kafka consumer stopped without completing the subscription")
		}
		if errors.Is(err, errUnexpected) {
			t.Fatal(err)
		}
		restarts++
		select {
		case <-ctx.Done():
			break consumeLoop
		case <-time.After(200 * time.Millisecond):
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		if ctx.Err() == nil {
			t.Fatalf("consume after leader failover: %v", err)
		}
	}
	for id, seen := range want {
		if !seen {
			t.Fatalf("confirmed message %s missing after leader failover (consumer restarts %d, last error %v)", id, restarts, err)
		}
	}
	if restarts > 0 {
		t.Logf("consumer supervisor restarted Run %d times during cluster recovery", restarts)
	}
}

func warmClusterGroup(ctx context.Context, adapter *Adapter, topic string) error {
	sub := mq.Subscription{Topic: topic, Name: "prewarm"}
	consumer, err := adapter.consumer(sub, adapter.options.ConsumerFetchBytes)
	if err != nil {
		return err
	}
	defer consumer.CloseAllowingRebalance()
	warmCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for warmCtx.Err() == nil {
			consumer.PollRecords(warmCtx, 1)
			consumer.AllowRebalance()
		}
	}()
	defer func() { cancel(); <-done }()
	for {
		name := "__consumer_offsets"
		request := kmsg.NewMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
		response, err := request.RequestWith(ctx, adapter.producer)
		if err == nil && len(response.Topics) == 1 && len(response.Topics[0].Partitions) > 0 {
			ready := true
			for _, partition := range response.Topics[0].Partitions {
				if partition.ErrorCode != 0 || len(partition.Replicas) != 3 || len(partition.ISR) != 3 {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for replicated Kafka consumer offsets topic: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func createClusterTopic(ctx context.Context, adapter *Adapter, name string) error {
	request := kmsg.NewCreateTopicsRequest()
	topic := kmsg.NewCreateTopicsRequestTopic()
	topic.Topic, topic.NumPartitions, topic.ReplicationFactor = name, 1, 3
	minimum := "2"
	topic.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "min.insync.replicas", Value: &minimum}}
	request.Topics = []kmsg.CreateTopicsRequestTopic{topic}
	response, err := request.RequestWith(ctx, adapter.producer)
	if err != nil {
		return err
	}
	if len(response.Topics) != 1 {
		return fmt.Errorf("create topic returned %d topics", len(response.Topics))
	}
	return kerr.ErrorForCode(response.Topics[0].ErrorCode)
}

func waitClusterPartition(ctx context.Context, adapter *Adapter, topic string, minISR int, oldLeader int32) (int32, error) {
	for {
		request := kmsg.NewMetadataRequest()
		request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
		response, err := request.RequestWith(ctx, adapter.producer)
		if err == nil && len(response.Topics) == 1 && len(response.Topics[0].Partitions) == 1 {
			partition := response.Topics[0].Partitions[0]
			if partition.ErrorCode == 0 && len(partition.Replicas) == 3 && len(partition.ISR) >= minISR && partition.Leader >= 0 && partition.Leader != oldLeader {
				return partition.Leader, nil
			}
		}
		select {
		case <-ctx.Done():
			return -1, fmt.Errorf("wait for Kafka partition with %d ISR and a new leader: %w", minISR, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func clusterMessages(t *testing.T, topic, phase string, count int) []mq.Message {
	t.Helper()
	messages := make([]mq.Message, count)
	for i := range messages {
		message, err := mq.NewMessage(topic, []byte(fmt.Sprintf("%s-%d", phase, i)))
		if err != nil {
			t.Fatal(err)
		}
		messages[i] = message
	}
	return messages
}

func killOwnedKafkaNode(ctx context.Context, runID, name string) error {
	inspect, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"mq.v2.cluster.run\"}}", name).Output()
	if err != nil {
		return fmt.Errorf("inspect disposable Kafka node %s: %w", name, err)
	}
	if strings.TrimSpace(string(inspect)) != runID {
		return fmt.Errorf("container %s is not owned by cluster run %s", name, runID)
	}
	output, err := exec.CommandContext(ctx, "docker", "kill", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kill Kafka leader %s: %w: %s", name, err, output)
	}
	return nil
}
