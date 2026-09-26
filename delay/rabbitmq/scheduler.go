// Package rabbitdelay implements optional durable delayed publication on RabbitMQ 3.13.
package rabbitdelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

var buckets = [...]time.Duration{100 * time.Millisecond, time.Second, 10 * time.Second, time.Minute, 10 * time.Minute, time.Hour}

func chooseBucket(remaining time.Duration) time.Duration {
	for i := len(buckets) - 1; i >= 0; i-- {
		if remaining >= buckets[i] {
			return buckets[i]
		}
	}
	return buckets[0]
}

type Options struct {
	Prefix          string
	PublishChannels int
	PollInterval    time.Duration
	RetryMin        time.Duration
	RetryMax        time.Duration
	DrainTimeout    time.Duration
	MaxMessageBytes int
}

func (o Options) withDefaults() (Options, error) {
	if o.PublishChannels < 0 || o.PollInterval < 0 || o.RetryMin < 0 || o.RetryMax < 0 || o.DrainTimeout < 0 || o.MaxMessageBytes < 0 {
		return o, errors.New("negative RabbitMQ delay option")
	}
	if o.Prefix == "" {
		o.Prefix = "mq.v2.delay"
	}
	if strings.TrimSpace(o.Prefix) == "" {
		return o, errors.New("blank RabbitMQ delay prefix")
	}
	if o.PollInterval == 0 {
		o.PollInterval = 20 * time.Millisecond
	}
	if o.PublishChannels == 0 {
		o.PublishChannels = 8
	}
	if o.RetryMin == 0 {
		o.RetryMin = 100 * time.Millisecond
	}
	if o.RetryMax == 0 {
		o.RetryMax = 5 * time.Second
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = 8 << 20
	}
	if o.PollInterval < time.Millisecond || o.RetryMin < time.Millisecond || o.RetryMax < o.RetryMin || o.DrainTimeout < time.Millisecond {
		return o, errors.New("invalid RabbitMQ delay durations")
	}
	return o, nil
}

type Scheduler struct {
	conn      *amqp.Connection
	target    mq.Publisher
	options   Options
	prepareMu sync.Mutex
	prepared  bool
	poolOnce  sync.Once
	poolClose sync.Once
	pool      chan *publishChannel
	mu        sync.Mutex
	active    int
	closed    bool
	closedCh  chan struct{}
	drained   chan struct{}
}

// New uses a caller-owned connection and target publisher. Close never closes either.
func New(conn *amqp.Connection, target mq.Publisher, options Options) (*Scheduler, error) {
	if conn == nil || conn.IsClosed() || isNil(target) {
		return nil, errors.New("open RabbitMQ connection and target publisher are required")
	}
	configured, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Scheduler{conn: conn, target: target, options: configured, closedCh: make(chan struct{}), drained: make(chan struct{})}, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func (s *Scheduler) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return mq.ErrClosed
	}
	s.active++
	return nil
}
func (s *Scheduler) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.closed && s.active == 0 {
		close(s.drained)
	}
}

func (s *Scheduler) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.closedCh)
		if s.active == 0 {
			close(s.drained)
		}
	}
	drained := s.drained
	s.mu.Unlock()
	select {
	case <-drained:
		s.poolClose.Do(func() {
			if s.pool != nil {
				for i := 0; i < s.options.PublishChannels; i++ {
					if pc := <-s.pool; pc != nil {
						_ = pc.ch.Close()
					}
				}
			}
		})
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) releaseExchange() string { return s.options.Prefix + ".release.exchange" }
func (s *Scheduler) releaseQueue() string    { return s.options.Prefix + ".release" }
func (s *Scheduler) failedQueue() string     { return s.options.Prefix + ".failed" }
func (s *Scheduler) bucketQueue(duration time.Duration) string {
	return fmt.Sprintf("%s.bucket.%d", s.options.Prefix, duration.Milliseconds())
}

// Prepare declares all delay topology before scheduling. PublishAt also calls it lazily.
func (s *Scheduler) Prepare(ctx context.Context) error {
	if err := s.begin(); err != nil {
		return err
	}
	defer s.end()
	return s.prepare(ctx)
}

func (s *Scheduler) prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.prepareMu.Lock()
	defer s.prepareMu.Unlock()
	if s.prepared {
		return nil
	}
	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(s.releaseExchange(), "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare delay release exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(s.releaseQueue(), true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish"}); err != nil {
		return fmt.Errorf("declare delay release queue: %w", err)
	}
	if _, err := ch.QueueDeclare(s.failedQueue(), true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish"}); err != nil {
		return fmt.Errorf("declare delay failed queue: %w", err)
	}
	if err := ch.QueueBind(s.releaseQueue(), "ready", s.releaseExchange(), false, nil); err != nil {
		return fmt.Errorf("bind delay release queue: %w", err)
	}
	for _, bucket := range buckets {
		args := amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish", "x-message-ttl": int32(bucket.Milliseconds()), "x-dead-letter-strategy": "at-least-once", "x-dead-letter-exchange": s.releaseExchange(), "x-dead-letter-routing-key": "ready"}
		if _, err := ch.QueueDeclare(s.bucketQueue(bucket), true, false, false, false, args); err != nil {
			return fmt.Errorf("declare %s delay bucket: %w", bucket, err)
		}
	}
	s.prepared = true
	return nil
}

type record struct {
	Version  int        `json:"v"`
	Due      time.Time  `json:"due"`
	Attempts int        `json:"attempts,omitempty"`
	Message  mq.Message `json:"message"`
}

func encode(message mq.Message, due time.Time) ([]byte, error) {
	return json.Marshal(record{Version: 1, Due: due.UTC(), Message: message})
}
func decode(body []byte) (record, error) {
	var r record
	if err := json.Unmarshal(body, &r); err != nil {
		return record{}, err
	}
	if r.Version != 1 || r.Due.IsZero() || r.Attempts < 0 || r.Attempts > 1_000_000_000 {
		return record{}, errors.New("invalid RabbitMQ delay envelope")
	}
	if err := r.Message.Validate(); err != nil {
		return record{}, err
	}
	return r, nil
}

func (s *Scheduler) PublishAt(ctx context.Context, message mq.Message, due time.Time) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if due.IsZero() {
		return errors.New("due time is required")
	}
	if message.SizeBytes() > s.options.MaxMessageBytes {
		return fmt.Errorf("message exceeds MaxMessageBytes %d", s.options.MaxMessageBytes)
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.end()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !due.After(time.Now()) {
		return s.target.Publish(ctx, message)
	}
	if err := s.prepare(ctx); err != nil {
		return err
	}
	body, err := encode(message, due)
	if err != nil {
		return err
	}
	return s.publish(ctx, s.bucketQueue(chooseBucket(time.Until(due))), message.ID, body)
}

type publishChannel struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

func (s *Scheduler) initPool() {
	s.poolOnce.Do(func() {
		s.pool = make(chan *publishChannel, s.options.PublishChannels)
		for i := 0; i < s.options.PublishChannels; i++ {
			s.pool <- nil
		}
	})
}

func (s *Scheduler) borrow(ctx context.Context) (*publishChannel, error) {
	s.initPool()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closedCh:
		return nil, mq.ErrClosed
	case pc := <-s.pool:
		if pc != nil {
			return pc, nil
		}
		ch, err := s.conn.Channel()
		if err != nil {
			s.pool <- nil
			return nil, err
		}
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			s.pool <- nil
			return nil, err
		}
		return &publishChannel{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 1))}, nil
	}
}

func (s *Scheduler) release(pc *publishChannel, healthy bool) {
	if !healthy {
		if pc != nil {
			_ = pc.ch.Close()
		}
		pc = nil
	}
	s.pool <- pc
}

func (s *Scheduler) publish(ctx context.Context, route, id string, body []byte) error {
	return s.publishWithHeaders(ctx, route, id, body, nil)
}

// One publish uses one pooled AMQP channel until its confirm and return arrive.
func (s *Scheduler) publishWithHeaders(ctx context.Context, route, id string, body []byte, headers amqp.Table) error {
	pc, err := s.borrow(ctx)
	if err != nil {
		return mq.OutcomeUnknown(err)
	}
	healthy := true
	defer func() { s.release(pc, healthy) }()
	confirmation, err := pc.ch.PublishWithDeferredConfirmWithContext(ctx, "", route, true, false, amqp.Publishing{Headers: headers, Body: body, DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: id, Timestamp: time.Now().UTC()})
	if err != nil || confirmation == nil {
		healthy = false
		if err == nil {
			err = errors.New("confirm unavailable")
		}
		return mq.OutcomeUnknown(err)
	}
	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		healthy = false
		return mq.OutcomeUnknown(err)
	}
	if !ack {
		if pc.ch.IsClosed() {
			healthy = false
			return mq.OutcomeUnknown(errors.New("channel closed before confirmation"))
		}
		return errors.New("RabbitMQ delay publish NACK")
	}
	select {
	case returned, open := <-pc.returns:
		if !open {
			healthy = false
			return mq.OutcomeUnknown(errors.New("return channel closed"))
		}
		return fmt.Errorf("%w: delay route %s (code %d)", mq.ErrNoRoute, route, returned.ReplyCode)
	default:
		return nil
	}
}

var _ mq.ScheduledPublisher = (*Scheduler)(nil)
