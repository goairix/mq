package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (a *Adapter) consumer(sub mq.Subscription, fetchBytes int) (*kgo.Client, error) {
	opts := append([]kgo.Opt{kgo.SeedBrokers(a.brokers...)}, a.options.ClientOptions...)
	opts = append(opts, kgo.ConsumerGroup(sub.Name), kgo.ConsumeTopics(sub.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(int32(fetchBytes)), kgo.FetchMaxPartitionBytes(int32(fetchBytes)), kgo.MaxConcurrentFetches(1), kgo.RebalanceTimeout(a.options.RebalanceTimeout))
	return kgo.NewClient(opts...)
}

func (a *Adapter) Run(ctx context.Context, sub mq.Subscription, handler mq.Handler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sub.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil Kafka handler")
	}
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	consumer, err := a.consumer(sub, a.options.ConsumerFetchBytes)
	if err != nil {
		return err
	}
	defer consumer.CloseAllowingRebalance()
	pollCtx, stopPoll := a.pollContext(ctx)
	defer stopPoll()
	workCtx, finish := a.workContext(ctx)
	defer finish()
	for {
		if stopErr := a.stopped(ctx); stopErr != nil {
			return stopErr
		}
		fetches := consumer.PollRecords(pollCtx, a.options.ConsumerPollRecords)
		if err := fetches.Err(); err != nil {
			consumer.AllowRebalance()
			if stopErr := a.stopped(ctx); stopErr != nil {
				return stopErr
			}
			return fmt.Errorf("poll Kafka records: %w", err)
		}
		processed := make([]*kgo.Record, 0, fetches.NumRecords())
		pollWorkCtx, finishPoll := context.WithTimeout(workCtx, a.options.MaxPollWork)
		var processErr error
		for _, record := range fetches.Records() {
			if stopErr := a.stopped(ctx); stopErr != nil {
				processErr = stopErr
				break
			}
			if err := a.handleRecord(ctx, pollWorkCtx, sub, record, handler); err != nil {
				processErr = err
				break
			}
			if pollWorkCtx.Err() != nil {
				processErr = a.workExpired(ctx, pollWorkCtx)
				break
			}
			processed = append(processed, record)
		}
		if len(processed) > 0 && pollWorkCtx.Err() == nil {
			if err := consumer.CommitRecords(pollWorkCtx, processed...); err != nil {
				processErr = fmt.Errorf("commit Kafka offsets: %w", err)
			}
		}
		if processErr == nil && pollWorkCtx.Err() != nil {
			processErr = a.workExpired(ctx, pollWorkCtx)
		}
		finishPoll()
		consumer.AllowRebalance()
		if processErr != nil {
			return processErr
		}
	}
}

func (a *Adapter) handleRecord(runCtx, workCtx context.Context, sub mq.Subscription, record *kgo.Record, handler mq.Handler) error {
	message, err := decode(record)
	if err != nil {
		return a.deadLetter(workCtx, sub, record, "invalid-envelope", err)
	}
	if message.Topic != sub.Topic {
		return a.deadLetter(workCtx, sub, record, "wrong-topic", errors.New("record topic differs from subscription"))
	}
	for attempt := 1; ; attempt++ {
		if workCtx.Err() != nil {
			return a.workExpired(runCtx, workCtx)
		}
		err := handler(workCtx, message)
		if workCtx.Err() != nil {
			return a.workExpired(runCtx, workCtx)
		}
		if err == nil {
			return nil
		}
		if mq.IsPermanent(err) {
			return a.deadLetter(workCtx, sub, record, "permanent", err)
		}
		if waitErr := a.waitRetry(runCtx, workCtx, a.retryDelay(attempt)); waitErr != nil {
			return waitErr
		}
	}
}

func (a *Adapter) deadLetter(ctx context.Context, sub mq.Subscription, record *kgo.Record, category string, cause error) error {
	headers := make([]kgo.RecordHeader, 0, len(record.Headers)+3)
	headers = append(headers, record.Headers...)
	reason := cause.Error()
	if len(reason) > 256 {
		reason = reason[:256]
	}
	headers = append(headers, kgo.RecordHeader{Key: "mq.dead.category", Value: []byte(category)}, kgo.RecordHeader{Key: "mq.dead.reason", Value: []byte(reason)}, kgo.RecordHeader{Key: "mq.dead.source", Value: []byte(sub.Topic)})
	dlq := &kgo.Record{Topic: sub.Topic + a.options.DLQSuffix, Key: record.Key, Value: record.Value, Headers: headers, Timestamp: record.Timestamp}
	producer, err := a.deadLetterProducer()
	if err != nil {
		return err
	}
	results := producer.ProduceSync(ctx, dlq)
	if len(results) != 1 {
		return mq.OutcomeUnknown(errors.New("Kafka DLQ confirmation missing"))
	}
	if err := results[0].Err; err != nil {
		return mq.OutcomeUnknown(fmt.Errorf("Kafka DLQ publish before commit: %w", err))
	}
	return nil
}

func (a *Adapter) retryDelay(attempt int) time.Duration {
	delay := a.options.RetryMin
	for i := 1; i < attempt && delay < a.options.RetryMax; i++ {
		if delay >= a.options.RetryMax/2 {
			return a.options.RetryMax
		}
		delay *= 2
	}
	if delay > a.options.RetryMax {
		return a.options.RetryMax
	}
	return delay
}

func (a *Adapter) waitRetry(runCtx, workCtx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-runCtx.Done():
		return runCtx.Err()
	case <-workCtx.Done():
		return workCtx.Err()
	case <-a.closedCh:
		return mq.ErrClosed
	case <-timer.C:
		return nil
	}
}

func (a *Adapter) pollContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	go func() {
		select {
		case <-a.closedCh:
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}

func (a *Adapter) workContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
		case <-a.closedCh:
		case <-stop:
			return
		}
		timer := time.NewTimer(a.options.DrainTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}

func (a *Adapter) stopped(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	select {
	case <-a.closedCh:
		return mq.ErrClosed
	default:
		return nil
	}
}

func (a *Adapter) workExpired(parent, workCtx context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	select {
	case <-a.closedCh:
		return mq.ErrClosed
	default:
		return workCtx.Err()
	}
}

var _ mq.Subscriber = (*Adapter)(nil)
