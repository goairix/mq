package rabbitdelay

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

// TestClusterDelayedQuorumLeaderFailover requires the disposable RabbitMQ
// cluster created by tests/cluster/rabbitmq.sh.
func TestClusterDelayedQuorumLeaderFailover(t *testing.T) {
	runID := os.Getenv("MQ_TEST_CLUSTER_RUN_ID")
	urls := strings.Split(os.Getenv("MQ_TEST_RABBIT_CLUSTER_URLS"), ",")
	management := os.Getenv("MQ_TEST_RABBIT_CLUSTER_MGMT")
	managementURLs := strings.Split(os.Getenv("MQ_TEST_RABBIT_CLUSTER_MGMTS"), ",")
	if runID == "" && management == "" && os.Getenv("MQ_TEST_RABBIT_CLUSTER_URLS") == "" && os.Getenv("MQ_TEST_RABBIT_CLUSTER_MGMTS") == "" {
		t.Skip("run tests/cluster/run.sh rabbitmq to enable cluster fault injection")
	}
	if runID == "" || management == "" || len(urls) != 3 || len(managementURLs) != 3 {
		t.Fatal("MQ_TEST_CLUSTER_RUN_ID, three RabbitMQ URLs and three management URLs are required")
	}
	if !strings.HasPrefix(runID, "mq-v2-cluster-") {
		t.Fatal("refusing to kill a container outside a disposable mq-v2 cluster")
	}
	conn, err := amqp.Dial(urls[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	prefix := fmt.Sprintf("mq.v2.cluster.delay.%d", time.Now().UnixNano())
	target := &recordingTarget{delivered: make(chan mq.Message, 16)}
	scheduler, err := New(conn, target, Options{Prefix: prefix, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := scheduler.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := mq.NewMessage("delayed", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.PublishAt(ctx, first, time.Now().Add(12*time.Second)); err != nil {
		t.Fatalf("schedule before failover: %v", err)
	}
	bucket := scheduler.bucketQueue(10 * time.Second)
	leader, err := delayedQuorumLeader(ctx, management, bucket)
	if err != nil {
		t.Fatal(err)
	}
	inspectChannel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	queue, inspectErr := inspectChannel.QueueInspect(bucket)
	_ = inspectChannel.Close()
	if inspectErr != nil || queue.Messages != 1 {
		t.Fatalf("confirmed delayed record must still be in the targeted bucket before fault: queue=%+v error=%v", queue, inspectErr)
	}
	if err := killOwnedDelayNode(ctx, runID, leader); err != nil {
		t.Fatal(err)
	}
	survivorManagement := ""
	for i, endpoint := range managementURLs {
		if !strings.HasSuffix(leader, fmt.Sprintf("r%d", i+1)) {
			survivorManagement = endpoint
			break
		}
	}
	for _, queueName := range []string{bucket, scheduler.releaseQueue(), scheduler.failedQueue()} {
		if err := waitDelayedLeader(ctx, survivorManagement, queueName, leader); err != nil {
			t.Fatal(err)
		}
	}
	for _, duration := range buckets {
		if err := waitDelayedLeader(ctx, survivorManagement, scheduler.bucketQueue(duration), leader); err != nil {
			t.Fatal(err)
		}
	}
	fresh, freshConn, err := reconnectDelayScheduler(ctx, urls, leader, target, Options{Prefix: prefix, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer freshConn.Close()
	defer fresh.Close(context.Background())
	second, err := mq.NewMessage("delayed", []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.PublishAt(ctx, second, time.Now().Add(500*time.Millisecond)); err != nil {
		t.Fatalf("schedule after failover: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- fresh.Run(runCtx) }()
	want := map[string]bool{first.ID: false, second.ID: false}
	for {
		select {
		case message := <-target.delivered:
			if _, known := want[message.ID]; !known {
				t.Fatalf("unexpected delayed message %s", message.ID)
			}
			want[message.ID] = true
			if want[first.ID] && want[second.ID] {
				stop()
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("delay worker after failover: %v", err)
				}
				return
			}
		case err := <-done:
			t.Fatalf("delay worker stopped before both messages arrived: %v", err)
		case <-ctx.Done():
			t.Fatalf("confirmed delayed message missing after failover: %v", ctx.Err())
		}
	}
}

func waitDelayedLeader(ctx context.Context, management, queue, deadLeader string) error {
	for {
		leader, err := delayedQuorumLeader(ctx, management, queue)
		if err == nil && leader != deadLeader {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for delayed release quorum election: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func delayedQuorumLeader(ctx context.Context, management, queue string) (string, error) {
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
		return "", fmt.Errorf("inspect delay quorum queue %s: HTTP %d", queue, response.StatusCode)
	}
	var status struct {
		Leader  string   `json:"leader"`
		Members []string `json:"members"`
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		return "", err
	}
	if len(status.Members) != 3 || status.Leader == "" {
		return "", fmt.Errorf("delay quorum queue %s has leader %q and %d members, need 3 members", queue, status.Leader, len(status.Members))
	}
	return status.Leader, nil
}

func killOwnedDelayNode(ctx context.Context, runID, leader string) error {
	if !strings.HasPrefix(leader, "rabbit@r") {
		return fmt.Errorf("unexpected RabbitMQ delay leader %q", leader)
	}
	name := runID + "-" + strings.TrimPrefix(leader, "rabbit@")
	inspect, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"mq.v2.cluster.run\"}}", name).Output()
	if err != nil {
		return fmt.Errorf("inspect RabbitMQ delay leader %s: %w", name, err)
	}
	if strings.TrimSpace(string(inspect)) != runID {
		return fmt.Errorf("container %s is not owned by cluster run %s", name, runID)
	}
	output, err := exec.CommandContext(ctx, "docker", "kill", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kill RabbitMQ delay leader %s: %w: %s", name, err, output)
	}
	return nil
}

func reconnectDelayScheduler(ctx context.Context, urls []string, deadLeader string, target mq.Publisher, options Options) (*Scheduler, *amqp.Connection, error) {
	for {
		for i, endpoint := range urls {
			if strings.HasSuffix(deadLeader, fmt.Sprintf("r%d", i+1)) {
				continue
			}
			conn, err := amqp.Dial(endpoint)
			if err != nil {
				continue
			}
			scheduler, err := New(conn, target, options)
			if err == nil {
				err = scheduler.Prepare(ctx)
			}
			if err == nil {
				return scheduler, conn, nil
			}
			_ = conn.Close()
		}
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("reconnect to surviving RabbitMQ delay quorum: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
