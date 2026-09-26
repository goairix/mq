package rabbitadapter

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

// Run with MQ_TEST_RABBIT_DOCKER_RESTART=1. The test owns a unique container
// and volume, and restarts the same RabbitMQ node against its persisted data.
func TestBrokerRestartPreservesConfirmedMessage(t *testing.T) {
	if os.Getenv("MQ_TEST_RABBIT_DOCKER_RESTART") != "1" {
		t.Skip("set MQ_TEST_RABBIT_DOCKER_RESTART=1")
	}
	image := os.Getenv("MQ_TEST_RABBIT_IMAGE")
	if image == "" {
		image = "rabbitmq:3.13.3-management"
	}
	name := fmt.Sprintf("mq-v2-rabbit-restart-%d", time.Now().UnixNano())
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
		address := docker("port", name, "5672/tcp")
		url := "amqp://guest:guest@" + address + "/"
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
	a, err := New(conn, Options{Prefix: name})
	if err != nil {
		t.Fatal(err)
	}
	sub := mq.Subscription{Topic: "restart", Name: "group"}
	if err := a.Prepare(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	m, _ := mq.NewMessage(sub.Topic, []byte("durable"))
	if err := a.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	docker("stop", "-t", "1", name)
	docker("rm", name)
	conn = start()
	defer conn.Close()
	a, err = New(conn, Options{Prefix: name})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := make(chan mq.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- a.Run(ctx, sub, func(_ context.Context, message mq.Message) error { got <- message; return nil })
	}()
	select {
	case message := <-got:
		if message.ID != m.ID || string(message.Payload) != "durable" {
			t.Fatalf("message after restart = %+v", message)
		}
	case err := <-done:
		t.Fatalf("consume after restart: %v", err)
	case <-ctx.Done():
		t.Fatal("confirmed message missing after restart")
	}
	cancel()
	<-done
}
