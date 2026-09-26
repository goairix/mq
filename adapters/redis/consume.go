package redisadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

// Prepare creates the durable consumer group before producers publish.
// RabbitMQ needs stronger topology pre-creation; Redis Streams can replay
// retained entries for a group created later with the default start position.
func (a *Adapter) Prepare(ctx context.Context, sub mq.Subscription) error {
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	if err := sub.Validate(); err != nil {
		return err
	}
	start := "0"
	if a.options.StartLatest {
		start = "$"
	}
	err := a.client.XGroupCreateMkStream(ctx, a.streamKey(sub.Topic), sub.Name, start).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}
	return nil
}

func (a *Adapter) Run(ctx context.Context, sub mq.Subscription, handler mq.Handler) error {
	if handler == nil {
		return errors.New("nil Redis handler")
	}
	if err := a.Prepare(ctx, sub); err != nil {
		return err
	}
	deliveryCtx, finish := a.deliveryContext(ctx)
	defer finish()
	consumer := a.options.Consumer + "-" + strconv.FormatUint(a.nextConsumer.Add(1), 10)
	stream := a.streamKey(sub.Topic)
	claimStart := "0-0"
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.isClosed() {
			return mq.ErrClosed
		}
		claimed, next, err := a.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: sub.Name, Consumer: consumer, MinIdle: a.options.ClaimIdle, Start: claimStart, Count: a.options.ReadCount}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("claim pending messages: %w", err)
		}
		if next == "" || next == "0-0" {
			claimStart = "0-0"
		} else {
			claimStart = next
		}
		if len(claimed) > 0 {
			for _, entry := range claimed {
				if err := a.deliverEntry(ctx, deliveryCtx, sub, entry, handler); err != nil {
					return err
				}
			}
			continue
		}
		streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: sub.Name, Consumer: consumer, Streams: []string{stream, ">"}, Count: a.options.ReadCount, Block: a.readBlock(a.options.Block)}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read Redis group: %w", err)
		}
		for _, group := range streams {
			for _, entry := range group.Messages {
				if err := a.deliverEntry(ctx, deliveryCtx, sub, entry, handler); err != nil {
					return err
				}
			}
		}
	}
}

func (a *Adapter) deliverEntry(ctx, deliveryCtx context.Context, sub mq.Subscription, entry redis.XMessage, handler mq.Handler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	return a.handleEntry(ctx, deliveryCtx, sub, entry, handler)
}

func (a *Adapter) handleEntry(ctx, deliveryCtx context.Context, sub mq.Subscription, entry redis.XMessage, handler mq.Handler) error {
	message, err := decode(entry)
	if err != nil {
		if writeErr := a.deadLetter(deliveryCtx, sub, entry, mq.Message{}, "invalid-envelope", err); writeErr != nil {
			return writeErr
		}
		return a.ack(deliveryCtx, sub, entry.ID)
	}
	if message.Topic != sub.Topic {
		if writeErr := a.deadLetter(deliveryCtx, sub, entry, message, "wrong-topic", errors.New("message topic differs from stream topic")); writeErr != nil {
			return writeErr
		}
		return a.ack(deliveryCtx, sub, entry.ID)
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := handler(deliveryCtx, message)
		switch {
		case err == nil:
			return a.ack(deliveryCtx, sub, entry.ID)
		case mq.IsPermanent(err):
			if writeErr := a.deadLetter(deliveryCtx, sub, entry, message, "permanent", err); writeErr != nil {
				return writeErr
			}
			return a.ack(deliveryCtx, sub, entry.ID)
		default:
			if waitErr := waitRetry(ctx, a.retryDelay(attempt)); waitErr != nil {
				return waitErr
			}
		}
	}
}

func (a *Adapter) deliveryContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
			timer := time.NewTimer(a.options.DrainTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancel()
			case <-stop:
			}
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}

func (a *Adapter) deadLetter(ctx context.Context, sub mq.Subscription, entry redis.XMessage, message mq.Message, category string, cause error) error {
	var values map[string]any
	if message.ID != "" {
		values = encode(message)
	} else {
		raw, _ := json.Marshal(entry.Values)
		values = map[string]any{"v": wireVersion, "raw": string(raw)}
	}
	values["source_stream"] = a.streamKey(sub.Topic)
	values["source_entry"] = entry.ID
	values["reason_category"] = category
	values["reason_text"] = cause.Error()
	if err := a.client.XAdd(ctx, &redis.XAddArgs{Stream: a.deadLetterKey(sub), Values: values}).Err(); err != nil {
		return fmt.Errorf("publish dead letter before ACK: %w", err)
	}
	return nil
}

func (a *Adapter) ack(ctx context.Context, sub mq.Subscription, id string) error {
	if err := a.client.XAck(ctx, a.streamKey(sub.Topic), sub.Name, id).Err(); err != nil {
		return fmt.Errorf("ACK Redis message %s: %w", id, err)
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

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ mq.Subscriber = (*Adapter)(nil)
