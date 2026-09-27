// This program demonstrates the v2 message flow without a broker. Memory is
// nonpersistent and is intended for examples and tests only.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/goairix/mq/adapters/memory/v2"
	mq "github.com/goairix/mq/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	broker, err := memory.New(16)
	if err != nil {
		return err
	}
	defer broker.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub := mq.Subscription{Topic: "jobs.email", Name: "workers"}
	received := make(chan mq.Message, 1)
	finished := make(chan error, 1)
	go func() {
		finished <- broker.Run(ctx, sub, func(_ context.Context, message mq.Message) error {
			received <- message
			return nil // 处理成功，确认这条消息。
		})
	}()

	message, err := mq.NewMessage(sub.Topic, []byte(`{"to":"user@example.com"}`))
	if err != nil {
		return err
	}
	if err := broker.Publish(ctx, message); err != nil {
		return err
	}
	select {
	case got := <-received:
		fmt.Printf("%s: %s\n", got.Topic, got.Payload)
	case err := <-finished:
		return fmt.Errorf("consumer stopped before delivery: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	cancel()
	if err := <-finished; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
