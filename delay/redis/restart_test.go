package redisdelay

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// Opt-in because it creates a private Docker volume and kills Redis to test
// recovery from an acknowledged AOF write, not just worker-process recovery.
func TestAOFRecoveryAfterRedisCrash(t *testing.T) {
	if os.Getenv("MQ_TEST_DOCKER_RESTART") != "1" {
		t.Skip("set MQ_TEST_DOCKER_RESTART=1 for AOF crash test")
	}
	suffix := time.Now().Format("150405000000000")
	volume, name := "mq-v2-delay-aof-"+suffix, "mq-v2-delay-aof-"+suffix
	docker := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	docker("volume", "create", volume)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput()
		_, _ = exec.CommandContext(ctx, "docker", "volume", "rm", volume).CombinedOutput()
	})
	start := func() *redis.Client {
		docker("run", "-d", "--name", name, "-p", "127.0.0.1::6379", "-v", volume+":/data", "redis:7.2", "redis-server", "--appendonly", "yes", "--appendfsync", "always")
		address := docker("port", name, "6379")
		client := redis.NewClient(&redis.Options{Addr: address})
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := client.Ping(context.Background()).Err(); err == nil {
				return client
			}
			if time.Now().After(deadline) {
				t.Fatal("Redis did not start")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	firstClient := start()
	opts := Options{Prefix: testPrefix(t), Shards: 1, PollInterval: 10 * time.Millisecond}
	first, _ := New(firstClient, targetStub{}, opts)
	m, _ := mq.NewMessage("aof-crash", []byte("persisted"))
	if err := first.PublishAt(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_ = firstClient.Close()
	docker("kill", name)
	docker("rm", name)
	secondClient := start()
	defer secondClient.Close()
	target := &recordingTarget{seen: make(chan mq.Message, 1)}
	second, _ := New(secondClient, target, opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx) }()
	select {
	case got := <-target.seen:
		if got.ID != m.ID || string(got.Payload) != "persisted" {
			t.Fatalf("recovered message = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("confirmed AOF task not recovered after Redis crash")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
