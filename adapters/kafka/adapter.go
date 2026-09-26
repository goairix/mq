// Package kafkaadapter implements ordinary MQ v2 traffic on Apache Kafka 4.x.
package kafkaadapter

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Options struct {
	ClientOptions       []kgo.Opt
	BatchMaxMessages    int
	BatchMaxBytes       int
	MaxMessageBytes     int
	MaxBufferedBytes    int
	MaxBufferedRecords  int
	ConsumerFetchBytes  int
	ConsumerPollRecords int
	DrainTimeout        time.Duration
	RebalanceTimeout    time.Duration
	MaxPollWork         time.Duration
	RetryMin            time.Duration
	RetryMax            time.Duration
	DLQSuffix           string
}

func (o Options) withDefaults() (Options, error) {
	if o.BatchMaxMessages < 0 || o.BatchMaxBytes < 0 || o.MaxMessageBytes < 0 || o.MaxBufferedBytes < 0 || o.MaxBufferedRecords < 0 || o.ConsumerFetchBytes < 0 || o.ConsumerPollRecords < 0 || o.DrainTimeout < 0 || o.RebalanceTimeout < 0 || o.MaxPollWork < 0 || o.RetryMin < 0 || o.RetryMax < 0 {
		return o, errors.New("negative Kafka option")
	}
	if o.BatchMaxMessages == 0 {
		o.BatchMaxMessages = 256
	}
	if o.BatchMaxBytes == 0 {
		o.BatchMaxBytes = 1 << 20
	}
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = 1 << 20
	}
	if o.MaxBufferedBytes == 0 {
		o.MaxBufferedBytes = 64 << 20
	}
	if o.MaxBufferedRecords == 0 {
		o.MaxBufferedRecords = 10_000
	}
	if o.ConsumerFetchBytes == 0 {
		o.ConsumerFetchBytes = 8 << 20
	}
	if o.ConsumerPollRecords == 0 {
		o.ConsumerPollRecords = 256
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.RebalanceTimeout == 0 {
		o.RebalanceTimeout = 5 * time.Minute
	}
	if o.MaxPollWork == 0 {
		o.MaxPollWork = o.RebalanceTimeout * 4 / 5
	}
	if o.RetryMin == 0 {
		o.RetryMin = 100 * time.Millisecond
	}
	if o.RetryMax == 0 {
		o.RetryMax = 5 * time.Second
	}
	if o.DLQSuffix == "" {
		o.DLQSuffix = ".dlq"
	}
	if o.MaxMessageBytes > o.BatchMaxBytes || o.BatchMaxBytes < 512 || o.BatchMaxBytes > math.MaxInt32-4096 || o.MaxBufferedBytes-o.BatchMaxBytes < 4096 || o.ConsumerFetchBytes > math.MaxInt32 || o.RetryMax < o.RetryMin || o.DrainTimeout < time.Millisecond || o.RetryMin < time.Millisecond || o.MaxPollWork < time.Millisecond || o.MaxPollWork >= o.RebalanceTimeout {
		return o, errors.New("invalid Kafka capacity or duration options")
	}
	return o, nil
}

type Adapter struct {
	brokers   []string
	options   Options
	producer  *kgo.Client
	dlqOnce   sync.Once
	dlqClient *kgo.Client
	dlqErr    error
	mu        sync.Mutex
	active    int
	closed    bool
	closedCh  chan struct{}
	drained   chan struct{}
	closeOnce sync.Once
}

func New(brokers []string, options Options) (*Adapter, error) {
	if len(brokers) == 0 {
		return nil, errors.New("Kafka seed brokers are required")
	}
	for _, broker := range brokers {
		if broker == "" {
			return nil, errors.New("blank Kafka broker")
		}
	}
	configured, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	producer, err := newProducer(brokers, configured, configured.BatchMaxBytes, configured.MaxBufferedBytes)
	if err != nil {
		return nil, err
	}
	return &Adapter{brokers: append([]string(nil), brokers...), options: configured, producer: producer, closedCh: make(chan struct{}), drained: make(chan struct{})}, nil
}

func newProducer(brokers []string, options Options, batchBytes, bufferedBytes int) (*kgo.Client, error) {
	opts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, options.ClientOptions...)
	opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation(), kgo.ProducerBatchMaxBytes(int32(batchBytes)), kgo.MaxBufferedBytes(bufferedBytes), kgo.MaxBufferedRecords(options.MaxBufferedRecords))
	producer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	if producer.OptValue(kgo.DisableIdempotentWrite).(bool) {
		producer.Close()
		return nil, errors.New("Kafka idempotent writes cannot be disabled")
	}
	if producer.OptValue(kgo.AllowAutoTopicCreation).(bool) {
		producer.Close()
		return nil, errors.New("Kafka topics must be provisioned explicitly")
	}
	if producer.OptValue(kgo.DefaultProduceTopicAlways).(bool) {
		producer.Close()
		return nil, errors.New("Kafka record topics cannot be overridden")
	}
	if producer.OptValue(kgo.ManualFlushing).(bool) {
		producer.Close()
		return nil, errors.New("Kafka producer requires automatic flushing")
	}
	return producer, nil
}

func (a *Adapter) deadLetterProducer() (*kgo.Client, error) {
	a.dlqOnce.Do(func() {
		cap := a.options.BatchMaxBytes + 4096
		a.dlqClient, a.dlqErr = newProducer(a.brokers, a.options, cap, cap)
	})
	return a.dlqClient, a.dlqErr
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
	finish := a.closed && a.active == 0
	a.mu.Unlock()
	if finish {
		a.finishClose()
	}
}

func (a *Adapter) finishClose() {
	a.closeOnce.Do(func() {
		a.producer.Close()
		if a.dlqClient != nil {
			a.dlqClient.Close()
		}
		close(a.drained)
	})
}

func (a *Adapter) Close(ctx context.Context) error {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		close(a.closedCh)
	}
	finish := a.active == 0
	drained := a.drained
	a.mu.Unlock()
	if finish {
		a.finishClose()
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Adapter) Prepare(ctx context.Context, sub mq.Subscription) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sub.Validate(); err != nil {
		return err
	}
	return a.beginAndEnd()
}

func (a *Adapter) beginAndEnd() error {
	if err := a.begin(); err != nil {
		return err
	}
	a.end()
	return nil
}
