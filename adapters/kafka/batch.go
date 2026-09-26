package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (a *Adapter) RunBatch(ctx context.Context, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sub.Validate(); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil Kafka batch handler")
	}
	if options.MaxInFlightBytes > math.MaxInt32 {
		return errors.New("Kafka MaxInFlightBytes exceeds fetch protocol limit")
	}
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	fetchBytes := min(options.MaxInFlightBytes, a.options.ConsumerFetchBytes)
	consumer, err := a.consumer(sub, fetchBytes)
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
		fetches := consumer.PollRecords(pollCtx, options.MaxMessages)
		if err := fetches.Err(); err != nil {
			consumer.AllowRebalance()
			if stopErr := a.stopped(ctx); stopErr != nil {
				return stopErr
			}
			return fmt.Errorf("poll Kafka batch: %w", err)
		}
		pending := fetches.Records()
		if len(pending) == 0 {
			consumer.AllowRebalance()
			continue
		}
		if err := a.processPoll(ctx, pollCtx, workCtx, consumer, sub, options, handler, pending); err != nil {
			return err
		}
	}
}

func (a *Adapter) processPoll(ctx, pollCtx, workCtx context.Context, consumer *kgo.Client, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler, pending []*kgo.Record) error {
	pollWorkCtx, finishPoll := context.WithTimeout(workCtx, a.options.MaxPollWork)
	defer finishPoll()
	defer consumer.AllowRebalance()
	if options.MaxWait > 0 && len(pending) < options.MaxMessages {
		deadline := time.Now().Add(options.MaxWait)
		if limit, ok := pollWorkCtx.Deadline(); ok && limit.Before(deadline) {
			deadline = limit
		}
		for len(pending) < options.MaxMessages && time.Now().Before(deadline) {
			waitCtx, cancel := context.WithDeadline(pollCtx, deadline)
			more := consumer.PollRecords(waitCtx, options.MaxMessages-len(pending))
			cancel()
			if err := more.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				if stopErr := a.stopped(ctx); stopErr != nil {
					return stopErr
				}
				return fmt.Errorf("coalesce Kafka batch: %w", err)
			}
			pending = append(pending, more.Records()...)
		}
	}
	for len(pending) > 0 {
		if pollWorkCtx.Err() != nil {
			return a.workExpired(ctx, pollWorkCtx)
		}
		if stopErr := a.stopped(ctx); stopErr != nil {
			return stopErr
		}
		selected := make([]*kgo.Record, 0, min(len(pending), options.MaxMessages))
		messages := make([]mq.Message, 0, cap(selected))
		bytes := 0
		for len(selected) < options.MaxMessages && len(selected) < len(pending) {
			record := pending[len(selected)]
			message, decodeErr := decode(record)
			if decodeErr != nil || message.Topic != sub.Topic {
				if len(selected) > 0 {
					break
				}
				category := "invalid-envelope"
				if decodeErr == nil {
					category = "wrong-topic"
					decodeErr = errors.New("record topic differs from subscription")
				}
				if err := a.deadLetter(pollWorkCtx, sub, record, category, decodeErr); err != nil {
					return err
				}
				if pollWorkCtx.Err() != nil {
					return a.workExpired(ctx, pollWorkCtx)
				}
				if err := consumer.CommitRecords(pollWorkCtx, record); err != nil {
					return fmt.Errorf("commit Kafka invalid record: %w", err)
				}
				pending = pending[1:]
				break
			}
			size := message.SizeBytes()
			if size > options.MaxInFlightBytes {
				if len(selected) > 0 {
					break
				}
				return fmt.Errorf("message %s size %d exceeds MaxInFlightBytes %d", message.ID, size, options.MaxInFlightBytes)
			}
			if len(selected) > 0 && (size > options.MaxBytes-bytes || size > options.MaxInFlightBytes-bytes) {
				break
			}
			selected = append(selected, record)
			messages = append(messages, message)
			bytes += size
			if bytes >= options.MaxBytes {
				break
			}
		}
		if len(selected) == 0 {
			continue
		}
		if stopErr := a.stopped(ctx); stopErr != nil {
			return stopErr
		}
		if err := a.handleBatch(ctx, pollWorkCtx, sub, selected, messages, handler); err != nil {
			return err
		}
		if pollWorkCtx.Err() != nil {
			return a.workExpired(ctx, pollWorkCtx)
		}
		if err := consumer.CommitRecords(pollWorkCtx, selected...); err != nil {
			return fmt.Errorf("commit Kafka batch offsets: %w", err)
		}
		pending = pending[len(selected):]
	}
	return nil
}

func (a *Adapter) handleBatch(runCtx, workCtx context.Context, sub mq.Subscription, records []*kgo.Record, messages []mq.Message, handler mq.BatchHandler) error {
	resolved := make([]bool, len(messages))
	remaining := len(messages)
	for attempt := 1; remaining > 0; attempt++ {
		if workCtx.Err() != nil {
			return a.workExpired(runCtx, workCtx)
		}
		current := make([]mq.Message, 0, remaining)
		indices := make([]int, 0, remaining)
		for i, message := range messages {
			if !resolved[i] {
				current = append(current, message)
				indices = append(indices, i)
			}
		}
		results, handlerErr := handler(workCtx, current)
		if workCtx.Err() != nil {
			return a.workExpired(runCtx, workCtx)
		}
		if handlerErr == nil && results != nil && len(results) != len(current) {
			return fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(current))
		}
		if handlerErr == nil {
			for j, index := range indices {
				var result error
				if results != nil {
					result = results[j]
				}
				if result == nil {
					resolved[index] = true
					remaining--
					continue
				}
				if mq.IsPermanent(result) {
					if err := a.deadLetter(workCtx, sub, records[index], "permanent", result); err != nil {
						return err
					}
					if workCtx.Err() != nil {
						return a.workExpired(runCtx, workCtx)
					}
					resolved[index] = true
					remaining--
				}
			}
		}
		if remaining > 0 {
			if err := a.waitRetry(runCtx, workCtx, a.retryDelay(attempt)); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ mq.BatchSubscriber = (*Adapter)(nil)
