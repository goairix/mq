package redisdelay

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

// TestClusterDelayedPrimaryFailover verifies a replicated, confirmed delay
// task across Redis Cluster primary failover. Redis asynchronous replication
// can still lose writes that have not reached a replica.
func TestClusterDelayedPrimaryFailover(t *testing.T) {
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
	prefix := fmt.Sprintf("mq:v2:cluster:delay:%d", time.Now().UnixNano())
	target := &recordingTarget{seen: make(chan mq.Message, 16)}
	scheduler, err := New(client, target, Options{Prefix: prefix, Shards: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close(context.Background())
	first, err := mq.NewMessage("delayed", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.PublishAt(ctx, first, time.Now().Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	keys := scheduler.keys(0)
	masterPort, replicaPort, err := delayedOwnerPorts(ctx, client, keys.records)
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
	if err := waitDelayedReplica(ctx, replica, keys); err != nil {
		t.Fatal(err)
	}
	if err := killOwnedDelayedMaster(ctx, runID, masterPort); err != nil {
		t.Fatal(err)
	}
	if err := waitDelayedPromotion(ctx, replica); err != nil {
		t.Fatal(err)
	}
	client.ReloadState(ctx)
	second, err := mq.NewMessage("delayed", []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.PublishAt(ctx, second, time.Now().Add(500*time.Millisecond)); err != nil {
		t.Fatalf("schedule after Redis primary failover: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(runCtx) }()
	want := map[string]bool{first.ID: false, second.ID: false}
	for {
		select {
		case message := <-target.seen:
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
			t.Fatalf("replicated confirmed delay task missing after Redis failover: %v", ctx.Err())
		}
	}
}

func delayedOwnerPorts(ctx context.Context, client *redis.ClusterClient, key string) (string, string, error) {
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

func waitDelayedReplica(ctx context.Context, replica *redis.Client, keys laneKeys) error {
	var lastErr error
	for {
		records, recordsErr := replica.HLen(ctx, keys.records).Result()
		due, dueErr := replica.ZCard(ctx, keys.due).Result()
		lastErr = errors.Join(recordsErr, dueErr)
		if recordsErr == nil && dueErr == nil && records == 1 && due == 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Redis replica to observe confirmed delay task (last error %v): %w", lastErr, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func killOwnedDelayedMaster(ctx context.Context, runID, port string) error {
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

func waitDelayedPromotion(ctx context.Context, replica *redis.Client) error {
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
