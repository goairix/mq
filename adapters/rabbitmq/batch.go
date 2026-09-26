package rabbitadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RunBatch uses synchronous basic.get to keep message bytes out of client-side
// prefetch buffers while enforcing the hard in-flight byte bound.
func (a *Adapter) RunBatch(ctx context.Context, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler) error {
	if err := options.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil RabbitMQ batch handler")
	}
	if err := a.Prepare(ctx, sub); err != nil {
		return err
	}
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	ch, err := a.conn.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ batch channel: %w", err)
	}
	defer ch.Close()
	deliveryCtx, finish := a.deliveryContext(ctx)
	defer finish()
	var pending []amqp.Delivery
	var attempt int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-a.closedCh:
			return mq.ErrClosed
		default:
		}
		selected := make([]amqp.Delivery, 0, options.MaxMessages)
		messages := make([]mq.Message, 0, options.MaxMessages)
		var totalBytes int
		var deadline time.Time
		for len(selected) < options.MaxMessages {
			if err := ctx.Err(); err != nil {
				return err
			}
			var delivery amqp.Delivery
			var available bool
			if len(pending) > 0 {
				delivery = pending[0]
				pending = pending[1:]
				available = true
			} else {
				var getErr error
				delivery, available, getErr = ch.Get(a.queueName(sub), false)
				if getErr != nil {
					return fmt.Errorf("get RabbitMQ batch message: %w", getErr)
				}
			}
			if !available {
				if len(selected) == 0 {
					if err := a.waitPoll(ctx, a.options.PollInterval); err != nil {
						return err
					}
					continue
				}
				if options.MaxWait == 0 || !time.Now().Before(deadline) {
					break
				}
				if err := a.waitPoll(ctx, min(a.options.PollInterval, time.Until(deadline))); err != nil {
					return err
				}
				continue
			}
			message, decodeErr := decode(delivery)
			if decodeErr != nil || message.Topic != sub.Topic {
				category := "invalid-envelope"
				if decodeErr == nil {
					category = "wrong-topic"
					decodeErr = errors.New("message topic differs from subscription")
				}
				if err := a.deadLetter(deliveryCtx, sub, delivery, category, decodeErr); err != nil {
					return err
				}
				if err := delivery.Ack(false); err != nil {
					return err
				}
				continue
			}
			size := message.SizeBytes()
			if size > options.MaxInFlightBytes {
				if err := delivery.Nack(false, true); err != nil {
					return err
				}
				return fmt.Errorf("message %s size %d exceeds MaxInFlightBytes %d", message.ID, size, options.MaxInFlightBytes)
			}
			if len(selected) > 0 && (totalBytes+size > options.MaxBytes || totalBytes+size > options.MaxInFlightBytes) {
				if err := delivery.Nack(false, true); err != nil {
					return err
				}
				break
			}
			if len(selected) == 0 {
				deadline = time.Now().Add(options.MaxWait)
			}
			selected = append(selected, delivery)
			messages = append(messages, message)
			totalBytes += size
			if totalBytes >= options.MaxBytes {
				break
			}
			if options.MaxWait == 0 {
				break
			}
			if !time.Now().Before(deadline) {
				break
			}
		}
		if len(selected) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		results, handlerErr := handler(deliveryCtx, messages)
		if handlerErr == nil && results != nil && len(results) != len(messages) {
			return fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(messages))
		}
		if handlerErr != nil {
			pending = append(selected, pending...)
			attempt++
			if err := waitRetry(ctx, a.retryDelay(attempt)); err != nil {
				return err
			}
			continue
		}
		var failed []amqp.Delivery
		for i, delivery := range selected {
			var result error
			if results != nil {
				result = results[i]
			}
			switch {
			case result == nil:
				if err := delivery.Ack(false); err != nil {
					return err
				}
			case mq.IsPermanent(result):
				if err := a.deadLetter(deliveryCtx, sub, delivery, "permanent", result); err != nil {
					return err
				}
				if err := delivery.Ack(false); err != nil {
					return err
				}
			default:
				failed = append(failed, delivery)
			}
		}
		pending = append(failed, pending...)
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

func (a *Adapter) waitPoll(ctx context.Context, wait time.Duration) error {
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closedCh:
		return mq.ErrClosed
	case <-timer.C:
		return nil
	}
}

var _ mq.BatchSubscriber = (*Adapter)(nil)
