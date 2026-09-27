package redisdelay

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

// TestSentinelDelayedPrimaryFailover requires tests/cluster/sentinel.sh. It
// tests a confirmed task after both asynchronous replicas have observed it.
func TestSentinelDelayedPrimaryFailover(t *testing.T) {
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
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: seeds,
		Dialer:        delayedSentinelDialer,
	})
	defer client.Close()
	target := &recordingTarget{seen: make(chan mq.Message, 16)}
	scheduler, err := New(client, target, Options{Prefix: fmt.Sprintf("mq:v2:sentinel:delay:%d", time.Now().UnixNano()), Shards: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close(context.Background())
	topologyCtx, stopTopology := context.WithTimeout(ctx, 30*time.Second)
	defer stopTopology()
	if err := waitDelayedSentinelTopology(topologyCtx, seeds, masterName); err != nil {
		t.Fatal(err)
	}
	first, err := mq.NewMessage("delayed", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.PublishAt(ctx, first, time.Now().Add(2*time.Second)); err != nil {
		t.Fatalf("schedule before Sentinel failover: %v", err)
	}
	if err := waitDelayedSentinelReplicas(ctx, scheduler.keys(0)); err != nil {
		t.Fatal(err)
	}
	if err := requireDelayedSentinelMasterBeforeKill(ctx, seeds, masterName); err != nil {
		t.Fatal(err)
	}
	if err := killOwnedDelayedSentinelMaster(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := waitDelayedSentinelPromotion(ctx, seeds, masterName, client); err != nil {
		t.Fatal(err)
	}
	second, err := mq.NewMessage("delayed", []byte("after"))
	if err != nil {
		t.Fatal(err)
	}
	secondDue := time.Now().Add(500 * time.Millisecond)
	for {
		if err := scheduler.PublishAt(ctx, second, secondDue); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("schedule after Sentinel failover: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		for runCtx.Err() == nil {
			err := scheduler.Run(runCtx)
			if runCtx.Err() != nil || err == nil {
				done <- err
				return
			}
			select {
			case <-runCtx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
		done <- runCtx.Err()
	}()
	want := map[string]bool{first.ID: false, second.ID: false}
	for {
		select {
		case message := <-target.seen:
			if _, known := want[message.ID]; !known {
				t.Fatalf("unexpected Sentinel delayed message %s", message.ID)
			}
			want[message.ID] = true
			if want[first.ID] && want[second.ID] {
				stop()
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("delay worker after Sentinel failover: %v", err)
				}
				return
			}
		case err := <-done:
			t.Fatalf("delay worker stopped before both Sentinel messages arrived: %v", err)
		case <-ctx.Done():
			t.Fatalf("confirmed Sentinel delayed message missing: %v", ctx.Err())
		}
	}
}

func delayedSentinelDialer(ctx context.Context, network, address string) (net.Conn, error) {
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

func waitDelayedSentinelTopology(ctx context.Context, seeds []string, masterName string) error {
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

func waitDelayedSentinelReplicas(ctx context.Context, keys laneKeys) error {
	for _, port := range []string{"7102", "7103"} {
		replica := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", port)})
		for {
			records, recordsErr := replica.HLen(ctx, keys.records).Result()
			due, dueErr := replica.ZCard(ctx, keys.due).Result()
			info, infoErr := replica.Info(ctx, "replication").Result()
			if recordsErr == nil && dueErr == nil && infoErr == nil && records == 1 && due == 1 && strings.Contains(info, "role:slave") && strings.Contains(info, "master_link_status:up") {
				break
			}
			select {
			case <-ctx.Done():
				_ = replica.Close()
				return fmt.Errorf("wait for Sentinel replica %s to observe delayed task: %w", port, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
		_ = replica.Close()
	}
	return nil
}

func requireDelayedSentinelMasterBeforeKill(ctx context.Context, seeds []string, masterName string) error {
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

func killOwnedDelayedSentinelMaster(ctx context.Context, runID string) error {
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

func waitDelayedSentinelPromotion(ctx context.Context, seeds []string, masterName string, client *redis.Client) (string, error) {
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
