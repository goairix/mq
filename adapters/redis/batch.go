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
	deliveryCtx, finish := a.deliveryContext(ctx)
	defer finish()
	consumer := a.options.Consumer + "-" + strconv.FormatUint(a.nextConsumer.Add(1), 10)
	stream := a.streamKey(sub.Topic)
	claimStart := "0-0"
	var pending []redis.XMessage
	var attempt int
	// Redis returns full payloads without a byte cap. Fetch one at a time so
	// a blocked handler cannot retain ReadCount arbitrarily large envelopes.
	readCount := int64(1)
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
				if entry.Values == nil {
					reloaded, claimErr := a.client.XClaim(ctx, &redis.XClaimArgs{Stream: stream, Group: sub.Name, Consumer: consumer, Messages: []string{entry.ID}}).Result()
					if claimErr != nil {
						return fmt.Errorf("reload pending message %s: %w", entry.ID, claimErr)
					}
					if len(reloaded) != 1 {
						return fmt.Errorf("pending message %s disappeared before reload", entry.ID)
					}
					entry = reloaded[0]
					pending[len(selected)] = entry
				}
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
					// The entry remains pending in Redis. Retain only its ID
					// while the current batch is handled; reload it later.
					pending[len(selected)].Values = nil
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
		failed, retryAll, err := a.processBatch(ctx, deliveryCtx, sub, selected, messages, handler)
		if err != nil {
			return err
		}
		if retryAll {
			attempt++
			if err := waitRetry(ctx, a.retryDelay(attempt)); err != nil {
				return err
			}
			continue
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

func (a *Adapter) processBatch(ctx, deliveryCtx context.Context, sub mq.Subscription, selected []redis.XMessage, messages []mq.Message, handler mq.BatchHandler) ([]redis.XMessage, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := a.begin(); err != nil {
		return nil, false, err
	}
	defer a.end()
	results, handlerErr := handler(deliveryCtx, messages)
	if handlerErr != nil {
		return nil, true, nil
	}
	if results != nil && len(results) != len(messages) {
		return nil, false, fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(messages))
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
			if err := a.deadLetter(deliveryCtx, sub, entry, messages[i], "permanent", result); err != nil {
				return nil, false, err
			}
			ackIDs = append(ackIDs, entry.ID)
		default:
			failed = append(failed, entry)
		}
	}
	if len(ackIDs) > 0 {
		if err := a.client.XAck(deliveryCtx, a.streamKey(sub.Topic), sub.Name, ackIDs...).Err(); err != nil {
			return nil, false, fmt.Errorf("ACK Redis batch: %w", err)
		}
	}
	return failed, false, nil
}

func (a *Adapter) readNew(ctx context.Context, sub mq.Subscription, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
	streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: sub.Name, Consumer: consumer, Streams: []string{a.streamKey(sub.Topic), ">"}, Count: count, Block: a.readBlock(block)}).Result()
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
