// Package rabbitadapter implements MQ v2 on RabbitMQ 3.13 quorum queues.
package rabbitadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Options struct {
	Prefix           string
	Exchange         string
	PublishChannels  int
	PublishBatchSize int
	Prefetch         int
	PollInterval     time.Duration
	DrainTimeout     time.Duration
	RetryMin         time.Duration
	RetryMax         time.Duration
}

func (o Options) withDefaults() (Options, error) {
	if o.PublishChannels < 0 || o.PublishBatchSize < 0 || o.Prefetch < 0 || o.PollInterval < 0 || o.DrainTimeout < 0 || o.RetryMin < 0 || o.RetryMax < 0 {
		return o, errors.New("negative RabbitMQ option")
	}
	if o.Prefix == "" {
		o.Prefix = "mq.v2"
	}
	if strings.TrimSpace(o.Prefix) == "" {
		return o, errors.New("RabbitMQ prefix is blank")
	}
	if o.Exchange == "" {
		o.Exchange = o.Prefix + ".topic"
	}
	if strings.TrimSpace(o.Exchange) == "" {
		return o, errors.New("RabbitMQ exchange is blank")
	}
	if o.PublishChannels == 0 {
		o.PublishChannels = 8
	}
	if o.PublishBatchSize == 0 {
		o.PublishBatchSize = 256
	}
	if o.Prefetch == 0 {
		o.Prefetch = 128
	}
	if o.PollInterval == 0 {
		o.PollInterval = 20 * time.Millisecond
	}
	if o.PollInterval < time.Millisecond {
		return o, errors.New("PollInterval must be at least one millisecond")
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 30 * time.Second
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
	return o, nil
}

// Adapter owns AMQP channels opened on the caller-owned connection.
type Adapter struct {
	conn      *amqp.Connection
	options   Options
	mu        sync.Mutex
	closed    bool
	active    int
	drained   chan struct{}
	closedCh  chan struct{}
	poolOnce  sync.Once
	pool      chan *publishChannel
	poolClose sync.Once
}

func New(conn *amqp.Connection, options Options) (*Adapter, error) {
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("open RabbitMQ connection is required")
	}
	configured, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Adapter{conn: conn, options: configured, drained: make(chan struct{}), closedCh: make(chan struct{})}, nil
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
	defer a.mu.Unlock()
	a.active--
	if a.closed && a.active == 0 {
		close(a.drained)
	}
}

func (a *Adapter) Close(ctx context.Context) error {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		close(a.closedCh)
		if a.active == 0 {
			close(a.drained)
		}
	}
	drained := a.drained
	a.mu.Unlock()
	select {
	case <-drained:
		a.poolClose.Do(func() {
			if a.pool != nil {
				for i := 0; i < a.options.PublishChannels; i++ {
					if slot := <-a.pool; slot != nil {
						_ = slot.ch.Close()
					}
				}
			}
		})
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func hashName(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (a *Adapter) routeKey(topic string) string { return hashName(topic) }
func (a *Adapter) queueName(sub mq.Subscription) string {
	return a.options.Prefix + ".q." + hashName(sub.Topic, sub.Name)
}
func (a *Adapter) deadLetterName(sub mq.Subscription) string {
	return a.options.Prefix + ".dlq." + hashName(sub.Topic, sub.Name)
}

// DeadLetterQueue returns the durable quorum queue for permanent failures.
func (a *Adapter) DeadLetterQueue(sub mq.Subscription) string { return a.deadLetterName(sub) }

// Prepare declares the subscription topology before publishers send messages.
func (a *Adapter) Prepare(ctx context.Context, sub mq.Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sub.Validate(); err != nil {
		return err
	}
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	ch, err := a.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(a.options.Exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	args := amqp.Table{"x-queue-type": "quorum"}
	if _, err := ch.QueueDeclare(a.queueName(sub), true, false, false, false, args); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(a.deadLetterName(sub), true, false, false, false, args); err != nil {
		return err
	}
	if err := ch.QueueBind(a.queueName(sub), a.routeKey(sub.Topic), a.options.Exchange, false, nil); err != nil {
		return err
	}
	return nil
}
