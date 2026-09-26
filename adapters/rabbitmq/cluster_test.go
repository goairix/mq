package rabbitadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestClusterQuorumLeaderFailover requires tests/cluster/rabbitmq.sh. It kills
// only a container carrying the runner's ownership label.
func TestClusterQuorumLeaderFailover(t *testing.T) {
	runID := os.Getenv("MQ_TEST_CLUSTER_RUN_ID")
	urls := strings.Split(os.Getenv("MQ_TEST_RABBIT_CLUSTER_URLS"), ",")
	management := os.Getenv("MQ_TEST_RABBIT_CLUSTER_MGMT")
	if runID == "" && management == "" && os.Getenv("MQ_TEST_RABBIT_CLUSTER_URLS") == "" {
		t.Skip("run tests/cluster/run.sh rabbitmq to enable cluster fault injection")
	}
	if runID == "" || management == "" || len(urls) != 3 {
		t.Fatal("MQ_TEST_CLUSTER_RUN_ID, three RabbitMQ URLs and management URL are required")
	}
	if !strings.HasPrefix(runID, "mq-v2-cluster-") {
		t.Fatal("refusing to kill a container outside a disposable mq-v2 cluster")
	}
	conn, err := amqp.Dial(urls[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	prefix := fmt.Sprintf("mq.v2.cluster.%d", time.Now().UnixNano())
	adapter, err := New(conn, Options{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	sub := mq.Subscription{Topic: "confirmed", Name: "failover"}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := adapter.Prepare(ctx, sub); err != nil {
		t.Fatal(err)
	}
	first := rabbitClusterMessages(t, sub.Topic, "before", 10)
	for _, message := range first {
		if err := adapter.Publish(ctx, message); err != nil {
			t.Fatalf("publish before failover: %v", err)
		}
	}
	leader, err := quorumLeader(ctx, management, adapter.queueName(sub))
	if err != nil {
		t.Fatal(err)
	}
	if err := killOwnedRabbitNode(ctx, runID, leader); err != nil {
		t.Fatal(err)
	}
	fresh, freshConn, err := reconnectRabbitAdapter(ctx, urls, leader, Options{Prefix: prefix}, sub)
	if err != nil {
		t.Fatal(err)
	}
	defer freshConn.Close()
	defer fresh.Close(context.Background())
	second := rabbitClusterMessages(t, sub.Topic, "after", 10)
	for _, message := range second {
		if err := fresh.Publish(ctx, message); err != nil {
			t.Fatalf("publish after failover: %v", err)
		}
	}
	want := make(map[string]bool, len(first)+len(second))
	for _, message := range append(first, second...) {
		want[message.ID] = false
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	err = fresh.Run(runCtx, sub, func(_ context.Context, message mq.Message) error {
		if _, known := want[message.ID]; !known {
			return fmt.Errorf("unexpected message %s", message.ID)
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
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consume after quorum leader failover: %v", err)
	}
	for id, seen := range want {
		if !seen {
			t.Fatalf("confirmed message %s missing after quorum leader failover", id)
		}
	}
}

func quorumLeader(ctx context.Context, management, queue string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, management+"/api/queues/%2F/"+url.PathEscape(queue), nil)
	if err != nil {
		return "", err
	}
	request.SetBasicAuth("guest", "guest")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("inspect quorum queue %s: HTTP %d", queue, response.StatusCode)
	}
	var status struct {
		Leader  string   `json:"leader"`
		Members []string `json:"members"`
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		return "", err
	}
	if len(status.Members) != 3 || status.Leader == "" {
		return "", fmt.Errorf("quorum queue %s has leader %q and %d members, need 3 members", queue, status.Leader, len(status.Members))
	}
	return status.Leader, nil
}

func killOwnedRabbitNode(ctx context.Context, runID, leader string) error {
	if !strings.HasPrefix(leader, "rabbit@r") {
		return fmt.Errorf("unexpected RabbitMQ leader %q", leader)
	}
	name := runID + "-" + strings.TrimPrefix(leader, "rabbit@")
	inspect, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"mq.v2.cluster.run\"}}", name).Output()
	if err != nil {
		return fmt.Errorf("inspect RabbitMQ leader %s: %w", name, err)
	}
	if strings.TrimSpace(string(inspect)) != runID {
		return fmt.Errorf("container %s is not owned by cluster run %s", name, runID)
	}
	output, err := exec.CommandContext(ctx, "docker", "kill", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kill RabbitMQ leader %s: %w: %s", name, err, output)
	}
	return nil
}

func reconnectRabbitAdapter(ctx context.Context, urls []string, deadLeader string, options Options, sub mq.Subscription) (*Adapter, *amqp.Connection, error) {
	for {
		for i, endpoint := range urls {
			if strings.HasSuffix(deadLeader, fmt.Sprintf("r%d", i+1)) {
				continue
			}
			conn, err := amqp.Dial(endpoint)
			if err != nil {
				continue
			}
			adapter, err := New(conn, options)
			if err == nil {
				err = adapter.Prepare(ctx, sub)
			}
			if err == nil {
				return adapter, conn, nil
			}
			_ = conn.Close()
		}
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("reconnect to surviving RabbitMQ quorum: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func rabbitClusterMessages(t *testing.T, topic, phase string, count int) []mq.Message {
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
