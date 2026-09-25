// Package redisadapter implements MQ v2 with Redis Streams consumer groups.
package redisadapter

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// Options controls only the Redis adapter. Zero values select safe defaults.
type Options struct {
	Prefix           string
	Consumer         string
	StartLatest      bool
	ReadCount        int64
	PublishBatchSize int
	Block            time.Duration
	ClaimIdle        time.Duration
	RetryMin         time.Duration
	RetryMax         time.Duration
}

func (o Options) withDefaults() (Options, error) {
	if o.ReadCount < 0 || o.PublishBatchSize < 0 || o.Block < 0 || o.ClaimIdle < 0 || o.RetryMin < 0 || o.RetryMax < 0 {
		return o, errors.New("negative Redis transport option")
	}
	if o.Prefix == "" {
		o.Prefix = "mq:v2"
	}
	if strings.TrimSpace(o.Prefix) == "" {
		return o, errors.New("Redis key prefix is blank")
	}
	if o.ReadCount == 0 {
		o.ReadCount = 128
	}
	if o.PublishBatchSize == 0 {
		o.PublishBatchSize = 256
	}
	if o.Block == 0 {
		o.Block = time.Second
	}
	if o.ClaimIdle == 0 {
		o.ClaimIdle = 30 * time.Second
	}
	if o.RetryMin == 0 {
		o.RetryMin = 100 * time.Millisecond
	}
	if o.RetryMax == 0 {
		o.RetryMax = 5 * time.Second
	}
	if o.RetryMax < o.RetryMin {
		return o, errors.New("RetryMax must be at least RetryMin")
	}
	if o.Consumer == "" {
		var id [8]byte
		if _, err := rand.Read(id[:]); err != nil {
			return o, fmt.Errorf("generate consumer name: %w", err)
		}
		host, _ := os.Hostname()
		o.Consumer = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(id[:]))
	}
	return o, nil
}

// Adapter owns no Redis client; callers close the supplied client separately.
type Adapter struct {
	client       redis.UniversalClient
	options      Options
	mu           sync.Mutex
	closed       bool
	active       int
	drained      chan struct{}
	nextConsumer atomic.Uint64
}

func New(client redis.UniversalClient, options Options) (*Adapter, error) {
	if client == nil {
		return nil, errors.New("Redis client is required")
	}
	value := reflect.ValueOf(client)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, errors.New("Redis client is nil")
	}
	configured, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Adapter{client: client, options: configured, drained: make(chan struct{})}, nil
}

func (a *Adapter) begin() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return mq.ErrClosed
	}
	a.active++
	return nil
}

func (a *Adapter) end() {
	a.mu.Lock()
	a.active--
	if a.closed && a.active == 0 {
		close(a.drained)
	}
	a.mu.Unlock()
}

func (a *Adapter) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

func (a *Adapter) Close(ctx context.Context) error {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		if a.active == 0 {
			close(a.drained)
		}
	}
	drained := a.drained
	a.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Adapter) streamKey(topic string) string {
	return a.options.Prefix + ":s:" + base64.RawURLEncoding.EncodeToString([]byte(topic))
}

func (a *Adapter) deadLetterKey(sub mq.Subscription) string {
	return a.options.Prefix + ":dlq:" + base64.RawURLEncoding.EncodeToString([]byte(sub.Topic)) + ":" + base64.RawURLEncoding.EncodeToString([]byte(sub.Name))
}

// DeadLetterStream returns the Redis Stream key used for permanent failures.
func (a *Adapter) DeadLetterStream(sub mq.Subscription) string { return a.deadLetterKey(sub) }
