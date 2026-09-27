package redisadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// TestSentinelPrimaryFailover runs only against tests/cluster/sentinel.sh. It
// verifies records already observed on both replicas across Sentinel failover.
func TestSentinelPrimaryFailover(t *testing.T) {
	runID := os.Getenv("MQ_TEST_CLUSTER_RUN_ID")
	addresses := os.Getenv("MQ_TEST_REDIS_SENTINEL_ADDRS")
	masterName := os.Getenv("MQ_TEST_REDIS_SENTINEL_MASTER")
	if runID == "" && addresses == "" && masterName == "" {
		t.Skip("run tests/cluster/run.sh sentinel to enable Sentinel fault injection")
	}
	if !strings.HasPrefix(runID, "mq-v2-cluster-") || masterName == "" {
		t.Fatal("disposable run ID and Sentinel master name are required")
	}
	seeds := strings.Split(addresses, ",")
	if len(seeds) != 3 {
		t.Fatalf("need three Sentinel addresses, got %d", len(seeds))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client := newSentinelFailoverClient(masterName, seeds)
	defer client.Close()
	adapter, err := New(client, Options{Prefix: fmt.Sprintf("mq:v2:sentinel:%d", time.Now().UnixNano()), ClaimIdle: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	sub := mq.Subscription{Topic: "confirmed", Name: "failover"}
	if err := adapter.Prepare(ctx, sub); err != nil {
		t.Fatal(err)
	}
	topologyCtx, stopTopology := context.WithTimeout(ctx, 30*time.Second)
	defer stopTopology()
	if err := waitSentinelTopology(topologyCtx, seeds, masterName); err != nil {
		t.Fatal(err)
	}
	first := makeSentinelMessages(t, sub.Topic, "before", 10)
	for _, message := range first {
		if err := adapter.Publish(ctx, message); err != nil {
			t.Fatalf("publish before Sentinel failover: %v", err)
		}
	}
	if err := waitSentinelStreamReplicas(ctx, adapter.streamKey(sub.Topic), int64(len(first))); err != nil {
		t.Fatal(err)
	}
	if err := requireSentinelMasterBeforeKill(ctx, seeds, masterName); err != nil {
		t.Fatal(err)
	}
	if err := killOwnedSentinelMaster(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := waitSentinelPromotion(ctx, seeds, masterName, client); err != nil {
		t.Fatal(err)
	}
	second := makeSentinelMessages(t, sub.Topic, "after", 10)
	for _, message := range second {
		for {
			if err := adapter.Publish(ctx, message); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("publish after Sentinel failover: %v", ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	want := make(map[string]bool, len(first)+len(second))
	for _, message := range append(first, second...) {
		want[message.ID] = false
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	errUnexpected := errors.New("unexpected Sentinel message")
	for runCtx.Err() == nil {
		err = adapter.Run(runCtx, sub, func(_ context.Context, message mq.Message) error {
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
		if runCtx.Err() != nil {
			break
		}
		if err == nil {
			t.Fatal("Sentinel consumer stopped before all messages arrived")
		}
		if errors.Is(err, errUnexpected) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	for id, seen := range want {
		if !seen {
			t.Fatalf("confirmed Sentinel message %s missing after failover (last error %v)", id, err)
		}
	}
}

func newSentinelFailoverClient(masterName string, seeds []string) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: seeds,
		Dialer:        sentinelTestDialer,
	})
}

func sentinelTestDialer(ctx context.Context, network, address string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	switch port {
	case "7101", "7102", "7103", "27101", "27102", "27103":
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
	default:
		return nil, fmt.Errorf("unexpected disposable Sentinel endpoint %q", address)
	}
}

func waitSentinelTopology(ctx context.Context, seeds []string, masterName string) error {
	for {
		allReady := true
		for _, seed := range seeds {
			sentinel := redis.NewSentinelClient(&redis.Options{Addr: seed})
			master, masterErr := sentinel.Master(ctx, masterName).Result()
			peers, peersErr := sentinel.Sentinels(ctx, masterName).Result()
			quorum, quorumErr := sentinel.CkQuorum(ctx, masterName).Result()
			addr, addrErr := sentinel.GetMasterAddrByName(ctx, masterName).Result()
			_ = sentinel.Close()
			if masterErr != nil || peersErr != nil || quorumErr != nil || addrErr != nil ||
				len(addr) != 2 || addr[1] != "7101" || master["num-slaves"] != "2" || len(peers) != 2 || !strings.HasPrefix(quorum, "OK") {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for three Sentinels, two replicas and quorum: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitSentinelStreamReplicas(ctx context.Context, key string, count int64) error {
	for _, port := range []string{"7102", "7103"} {
		replica := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", port)})
		for {
			length, lengthErr := replica.XLen(ctx, key).Result()
			info, infoErr := replica.Info(ctx, "replication").Result()
			if lengthErr == nil && infoErr == nil && length >= count && strings.Contains(info, "role:slave") && strings.Contains(info, "master_link_status:up") {
				break
			}
			select {
			case <-ctx.Done():
				_ = replica.Close()
				return fmt.Errorf("wait for Sentinel replica %s to observe %d stream entries: %w", port, count, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
		_ = replica.Close()
	}
	return nil
}

func requireSentinelMasterBeforeKill(ctx context.Context, seeds []string, masterName string) error {
	for _, seed := range seeds {
		sentinel := redis.NewSentinelClient(&redis.Options{Addr: seed})
		addr, err := sentinel.GetMasterAddrByName(ctx, masterName).Result()
		_ = sentinel.Close()
		if err != nil || len(addr) != 2 || addr[1] != "7101" {
			return fmt.Errorf("Sentinel master changed before fault injection: seed %s address %v error %v", seed, addr, err)
		}
	}
	master := redis.NewClient(&redis.Options{Addr: "127.0.0.1:7101"})
	defer master.Close()
	info, err := master.Info(ctx, "replication").Result()
	if err != nil || !strings.Contains(info, "role:master") {
		return fmt.Errorf("disposable Redis d1 is no longer master before fault injection: %v", err)
	}
	return nil
}

func killOwnedSentinelMaster(ctx context.Context, runID string) error {
	name := runID + "-d1"
	inspect, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Config.Labels \"mq.v2.cluster.run\"}}", name).Output()
	if err != nil {
		return fmt.Errorf("inspect disposable Sentinel master %s: %w", name, err)
	}
	if strings.TrimSpace(string(inspect)) != runID {
		return fmt.Errorf("container %s is not owned by run %s", name, runID)
	}
	output, err := exec.CommandContext(ctx, "docker", "kill", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kill Sentinel master %s: %w: %s", name, err, output)
	}
	return nil
}

func waitSentinelPromotion(ctx context.Context, seeds []string, masterName string, client *redis.Client) (string, error) {
	for {
		promotedPort := ""
		ready := true
		for _, seed := range seeds {
			sentinel := redis.NewSentinelClient(&redis.Options{Addr: seed})
			addr, err := sentinel.GetMasterAddrByName(ctx, masterName).Result()
			_ = sentinel.Close()
			if err != nil || len(addr) != 2 || (addr[1] != "7102" && addr[1] != "7103") {
				ready = false
				break
			}
			if promotedPort != "" && promotedPort != addr[1] {
				ready = false
				break
			}
			promotedPort = addr[1]
		}
		if ready {
			infoClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", promotedPort)})
			info, infoErr := infoClient.Info(ctx, "replication").Result()
			_ = infoClient.Close()
			if infoErr == nil && strings.Contains(info, "role:master") && client.Ping(ctx).Err() == nil {
				return promotedPort, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for Sentinel promotion and FailoverClient reconnection: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func makeSentinelMessages(t *testing.T, topic, phase string, count int) []mq.Message {
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
