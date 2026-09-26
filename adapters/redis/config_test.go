package redisadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func TestNewOptionsAndClose(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("accepted nil client")
	}
	var typedNil *redis.Client
	if _, err := New(typedNil, Options{}); err == nil {
		t.Fatal("accepted typed nil client")
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	cases := []Options{
		{ReadCount: -1},
		{PublishBatchSize: -1},
		{Block: -time.Second},
		{Block: time.Nanosecond},
		{DrainTimeout: -time.Second},
		{ClaimIdle: -time.Second},
		{RetryMin: -time.Second},
		{RetryMin: 2 * time.Second, RetryMax: time.Second},
	}
	for _, options := range cases {
		if _, err := New(client, options); err == nil {
			t.Fatalf("accepted invalid options: %+v", options)
		}
	}
	adapter, err := New(client, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.options.ReadCount <= 0 || adapter.options.Block <= 0 || adapter.options.DrainTimeout <= 0 || adapter.options.ClaimIdle <= 0 || adapter.options.RetryMin <= 0 || adapter.options.RetryMax < adapter.options.RetryMin {
		t.Fatalf("invalid defaults: %+v", adapter.options)
	}
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := adapter.begin(); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("begin after close = %v", err)
	}
}
