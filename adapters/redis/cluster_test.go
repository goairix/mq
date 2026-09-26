package redisadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// TestClusterPrimaryFailover requires the disposable six-node Redis Cluster
// created by tests/cluster/redis.sh. It waits for replica observation before
// killing the primary because Redis replication itself is asynchronous.
func TestClusterPrimaryFailover(t *testing.T) {
	runID := os.Getenv("MQ_TEST_CLUSTER_RUN_ID")
	addresses := os.Getenv("MQ_TEST_REDIS_CLUSTER_ADDR")
	if runID == "" && addresses == "" {
		t.Skip("run tests/cluster/run.sh redis to enable cluster fault injection")
	}
	if runID == "" || addresses == "" {
		t.Fatal("both MQ_TEST_CLUSTER_RUN_ID and MQ_TEST_REDIS_CLUSTER_ADDR are required")
	}
	if !strings.HasPrefix(runID, "mq-v2-cluster-") {
		t.Fatal("refusing to kill a container outside a disposable mq-v2 cluster")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: strings.Split(addresses, ","),
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
		},
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	adapter, err := New(client, Options{Prefix: fmt.Sprintf("mq:v2:cluster:%d", time.Now().UnixNano()), ClaimIdle: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	sub := mq.Subscription{Topic: "confirmed", Name: "failover"}
	if err := adapter.Prepare(ctx, sub); err != nil {
		t.Fatal(err)
	}
	first, err := mq.NewMessage(sub.Topic, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	key := adapter.streamKey(sub.Topic)
	masterPort, replicaPort, err := clusterOwnerPorts(ctx, client, key)
	if err != nil {
		t.Fatal(err)
	}
	replica := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort("127.0.0.1", replicaPort),
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			return conn.Do(ctx, "READONLY").Err()
		},
	})
	defer replica.Close()
	if err := waitRedisReplica(ctx, replica, key); err != nil {
		t.Fatal(err)
	}
	if err := killOwnedRedisMaster(ctx, runID, masterPort); err != nil {
		t.Fatal(err)
	}
	if err := waitRedisPromotion(ctx, replica); err != nil {
		t.Fatal(err)
	}
	client.ReloadState(ctx)
	second, err := mq.NewMessage(sub.Topic, []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Publish(ctx, second); err != nil {
		t.Fatalf("publish after Redis primary failover: %v", err)
	}
	want := map[string]bool{first.ID: false, second.ID: false}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	err = adapter.Run(runCtx, sub, func(_ context.Context, message mq.Message) error {
		if _, known := want[message.ID]; !known {
			return fmt.Errorf("unexpected message %s", message.ID)
		}
		want[message.ID] = true
		if want[first.ID] && want[second.ID] {
			stop()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consume after Redis primary failover: %v", err)
	}
	for id, seen := range want {
		if !seen {
			t.Fatalf("replicated confirmed message %s missing after Redis primary failover", id)
		}
	}
}

func clusterOwnerPorts(ctx context.Context, client *redis.ClusterClient, key string) (string, string, error) {
	slot, err := client.ClusterKeySlot(ctx, key).Result()
	if err != nil {
		return "", "", err
	}
	for {
		slots, err := client.ClusterSlots(ctx).Result()
		if err == nil {
			for _, rangeInfo := range slots {
				if slot < int64(rangeInfo.Start) || slot > int64(rangeInfo.End) || len(rangeInfo.Nodes) != 2 {
					continue
				}
				_, masterPort, err := net.SplitHostPort(rangeInfo.Nodes[0].Addr)
				if err != nil {
					return "", "", err
				}
				_, replicaPort, err := net.SplitHostPort(rangeInfo.Nodes[1].Addr)
				return masterPort, replicaPort, err
			}
		}
		select {
		case <-ctx.Done():
			return "", "", fmt.Errorf("wait for a Redis primary and replica for slot %d: %w", slot, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitRedisReplica(ctx context.Context, replica *redis.Client, key string) error {
	var lastErr error
	for {
		count, err := replica.XLen(ctx, key).Result()
		lastErr = err
		if err == nil && count >= 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Redis replica to observe confirmed stream entry (last error %v): %w", lastErr, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func killOwnedRedisMaster(ctx context.Context, runID, port string) error {
	node, err := strconv.Atoi(port)
	if err != nil || node < 7001 || node > 7006 {
		return fmt.Errorf("unexpected Redis primary port %q", port)
	}
	name := fmt.Sprintf("%s-d%d", runID, node-7000)
	inspect, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"mq.v2.cluster.run\"}}", name).Output()
	if err != nil {
		return fmt.Errorf("inspect Redis primary %s: %w", name, err)
	}
	if strings.TrimSpace(string(inspect)) != runID {
		return fmt.Errorf("container %s is not owned by cluster run %s", name, runID)
	}
	output, err := exec.CommandContext(ctx, "docker", "kill", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kill Redis primary %s: %w: %s", name, err, output)
	}
	return nil
}

func waitRedisPromotion(ctx context.Context, replica *redis.Client) error {
	for {
		info, err := replica.Info(ctx, "replication").Result()
		if err == nil && strings.Contains(info, "role:master") {
			state, stateErr := replica.ClusterInfo(ctx).Result()
			if stateErr == nil && strings.Contains(state, "cluster_state:ok") {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Redis replica promotion: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
