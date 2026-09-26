package rabbitdelay

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestBrokerRestartPreservesDelayedMessage(t *testing.T) {
	if os.Getenv("MQ_TEST_RABBIT_DOCKER_RESTART") != "1" {
		t.Skip("set MQ_TEST_RABBIT_DOCKER_RESTART=1")
	}
	image := os.Getenv("MQ_TEST_RABBIT_IMAGE")
	if image == "" {
		image = "rabbitmq:3.13.3-management"
	}
	name := fmt.Sprintf("mq-v2-delay-restart-%d", time.Now().UnixNano())
	volume := name + "-data"
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	docker("volume", "create", volume)
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
		_ = exec.Command("docker", "volume", "rm", "-f", volume).Run()
	})
	start := func() *amqp.Connection {
		t.Helper()
		docker("run", "-d", "--name", name, "--hostname", name, "-e", "RABBITMQ_NODENAME=rabbit@"+name, "-v", volume+":/var/lib/rabbitmq", "-p", "127.0.0.1::5672", image)
		url := "amqp://guest:guest@" + docker("port", name, "5672/tcp") + "/"
		deadline := time.Now().Add(30 * time.Second)
		for {
			conn, err := amqp.Dial(url)
			if err == nil {
				return conn
			}
			if time.Now().After(deadline) {
				t.Fatalf("RabbitMQ did not start: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	conn := start()
	target := &recordingTarget{delivered: make(chan mq.Message, 1)}
	s, err := New(conn, target, Options{Prefix: name, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage("restart-delay", []byte("durable"))
	if err := s.PublishAt(context.Background(), m, time.Now().Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	docker("stop", "-t", "1", name)
	docker("rm", name)
	conn = start()
	defer conn.Close()
	s, err = New(conn, target, Options{Prefix: name, PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	select {
	case got := <-target.delivered:
		if got.ID != m.ID || string(got.Payload) != "durable" {
			t.Fatalf("message = %+v", got)
		}
	case err := <-done:
		t.Fatalf("Run: %v", err)
	case <-ctx.Done():
		t.Fatal("scheduled message missing after restart")
	}
	cancel()
	<-done
}
