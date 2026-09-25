package redisadapter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func (a *Adapter) RunBatch(ctx context.Context, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler) error {
	if err := options.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil Redis batch handler")
	}
	if err := a.Prepare(ctx, sub); err != nil {
		return err
	}
	consumer := a.options.Consumer + "-" + strconv.FormatUint(a.nextConsumer.Add(1), 10)
	stream := a.streamKey(sub.Topic)
	claimStart := "0-0"
	var pending []redis.XMessage
	var attempt int
	readCount := min(a.options.ReadCount, int64(options.MaxMessages))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.isClosed() {
			return mq.ErrClosed
		}
		if len(pending) == 0 {
			claimed, next, err := a.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: sub.Name, Consumer: consumer, MinIdle: a.options.ClaimIdle, Start: claimStart, Count: readCount}).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return fmt.Errorf("claim batch pending: %w", err)
			}
			if next == "" || next == "0-0" {
				claimStart = "0-0"
			} else {
				claimStart = next
			}
			pending = append(pending, claimed...)
			if len(pending) == 0 {
				fresh, err := a.readNew(ctx, sub, consumer, readCount, a.options.Block)
				if err != nil {
					return err
				}
				pending = append(pending, fresh...)
				if len(pending) == 0 {
					continue
				}
			}
		}
		var selected []redis.XMessage
		var messages []mq.Message
		var totalBytes int
		var limitReached bool
		deadline := time.Now().Add(options.MaxWait)
		for {
			for len(pending) > len(selected) && len(selected) < options.MaxMessages {
				entry := pending[len(selected)]
				message, err := decode(entry)
				if err != nil || message.Topic != sub.Topic {
					if err == nil {
						err = errors.New("message topic differs from stream topic")
					}
					category := "invalid-envelope"
					if message.ID != "" {
						category = "wrong-topic"
					}
					if writeErr := a.deadLetter(ctx, sub, entry, message, category, err); writeErr != nil {
						return writeErr
					}
					if ackErr := a.ack(ctx, sub, entry.ID); ackErr != nil {
						return ackErr
					}
					pending = append(pending[:len(selected)], pending[len(selected)+1:]...)
					continue
				}
				size := message.SizeBytes()
				if size > options.MaxInFlightBytes {
					return fmt.Errorf("message %s size %d exceeds MaxInFlightBytes %d", message.ID, size, options.MaxInFlightBytes)
				}
				if len(selected) > 0 && (totalBytes+size > options.MaxBytes || totalBytes+size > options.MaxInFlightBytes) {
					limitReached = true
					break
				}
				selected = append(selected, entry)
				messages = append(messages, message)
				totalBytes += size
				if totalBytes >= options.MaxBytes {
					limitReached = true
					break
				}
			}
			if len(selected) == options.MaxMessages {
				limitReached = true
			}
			if len(selected) == 0 {
				break
			}
			if limitReached || options.MaxWait == 0 || !time.Now().Before(deadline) {
				break
			}
			block := min(a.options.Block, time.Until(deadline))
			if block < time.Millisecond {
				block = time.Millisecond
			}
			fresh, err := a.readNew(ctx, sub, consumer, readCount, block)
			if err != nil {
				return err
			}
			pending = append(pending, fresh...)
		}
		if len(selected) == 0 {
			if len(pending) == 0 {
				continue
			}
			return errors.New("Redis batch could not select pending messages")
		}
		results, handlerErr := handler(ctx, messages)
		if handlerErr == nil && results != nil && len(results) != len(messages) {
			return fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(messages))
		}
		if handlerErr != nil {
			attempt++
			if err := waitRetry(ctx, a.retryDelay(attempt)); err != nil {
				return err
			}
			continue
		}
		ackIDs := make([]string, 0, len(selected))
		failed := make([]redis.XMessage, 0, len(selected))
		for i, entry := range selected {
			var result error
			if results != nil {
				result = results[i]
			}
			switch {
			case result == nil:
				ackIDs = append(ackIDs, entry.ID)
			case mq.IsPermanent(result):
				if err := a.deadLetter(ctx, sub, entry, messages[i], "permanent", result); err != nil {
					return err
				}
				ackIDs = append(ackIDs, entry.ID)
			default:
				failed = append(failed, entry)
			}
		}
		if len(ackIDs) > 0 {
			if err := a.client.XAck(ctx, stream, sub.Name, ackIDs...).Err(); err != nil {
				return fmt.Errorf("ACK Redis batch: %w", err)
			}
		}
		pending = append(failed, pending[len(selected):]...)
		if len(failed) > 0 {
			attempt++
			if err := waitRetry(ctx, a.retryDelay(attempt)); err != nil {
				return err
			}
		} else {
			attempt = 0
		}
	}
}

func (a *Adapter) readNew(ctx context.Context, sub mq.Subscription, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
	streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: sub.Name, Consumer: consumer, Streams: []string{a.streamKey(sub.Topic), ">"}, Count: count, Block: block}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read Redis batch: %w", err)
	}
	var entries []redis.XMessage
	for _, stream := range streams {
		entries = append(entries, stream.Messages...)
	}
	return entries, nil
}

var _ mq.BatchSubscriber = (*Adapter)(nil)
