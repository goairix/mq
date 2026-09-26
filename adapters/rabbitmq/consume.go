package rabbitadapter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrConsumerClosed = errors.New("RabbitMQ consumer channel closed")

func (a *Adapter) Run(ctx context.Context, sub mq.Subscription, handler mq.Handler) error {
	if handler == nil {
		return errors.New("nil RabbitMQ handler")
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
		return fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}
	defer ch.Close()
	if err := ch.Qos(a.options.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set RabbitMQ prefetch: %w", err)
	}
	deliveries, err := ch.Consume(a.queueName(sub), "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume RabbitMQ quorum queue: %w", err)
	}
	deliveryCtx, finish := a.deliveryContext(ctx)
	defer finish()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closedCh:
			return mq.ErrClosed
		case delivery, open := <-deliveries:
			if !open {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return ErrConsumerClosed
			}
			if err := a.handleDelivery(ctx, deliveryCtx, sub, delivery, handler); err != nil {
				return err
			}
		}
	}
}

func (a *Adapter) handleDelivery(runCtx, deliveryCtx context.Context, sub mq.Subscription, delivery amqp.Delivery, handler mq.Handler) error {
	message, err := decode(delivery)
	if err != nil {
		if dlqErr := a.deadLetter(deliveryCtx, sub, delivery, "invalid-envelope", err); dlqErr != nil {
			return dlqErr
		}
		return delivery.Ack(false)
	}
	if message.Topic != sub.Topic {
		if dlqErr := a.deadLetter(deliveryCtx, sub, delivery, "wrong-topic", errors.New("message topic differs from subscription")); dlqErr != nil {
			return dlqErr
		}
		return delivery.Ack(false)
	}
	for attempt := 1; ; attempt++ {
		if err := runCtx.Err(); err != nil {
			return err
		}
		select {
		case <-a.closedCh:
			return mq.ErrClosed
		default:
		}
		err := handler(deliveryCtx, message)
		if stopErr := a.deliveryExpired(runCtx, deliveryCtx); stopErr != nil {
			return stopErr
		}
		switch {
		case err == nil:
			return delivery.Ack(false)
		case mq.IsPermanent(err):
			if dlqErr := a.deadLetter(deliveryCtx, sub, delivery, "permanent", err); dlqErr != nil {
				return dlqErr
			}
			return delivery.Ack(false)
		default:
			if waitErr := a.waitRetry(runCtx, a.retryDelay(attempt)); waitErr != nil {
				return waitErr
			}
		}
	}
}

func (a *Adapter) deadLetter(ctx context.Context, sub mq.Subscription, delivery amqp.Delivery, category string, cause error) error {
	headers := make(amqp.Table, len(delivery.Headers)+3)
	for key, value := range delivery.Headers {
		headers[key] = value
	}
	headers["mq.dead.category"] = category
	headers["mq.dead.reason"] = cause.Error()
	headers["mq.dead.source"] = a.queueName(sub)
	publishing := amqp.Publishing{Headers: headers, Body: append([]byte(nil), delivery.Body...), MessageId: delivery.MessageId, DeliveryMode: amqp.Persistent, Timestamp: delivery.Timestamp, ContentType: delivery.ContentType}
	if err := a.publishRaw(ctx, "", a.deadLetterName(sub), publishing); err != nil {
		return fmt.Errorf("publish RabbitMQ dead letter before ACK: %w", err)
	}
	return nil
}

func (a *Adapter) publishRaw(ctx context.Context, exchange, key string, publishing amqp.Publishing) error {
	if err := publishing.Headers.Validate(); err != nil {
		return err
	}
	pc, err := a.borrow(ctx)
	if err != nil {
		return mq.OutcomeUnknown(err)
	}
	healthy := true
	defer func() { a.release(pc, healthy) }()
	pc.serial++
	publishing.CorrelationId = strconv.FormatUint(pc.serial, 10)
	confirmation, err := pc.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, publishing)
	if err != nil || confirmation == nil {
		healthy = false
		return mq.OutcomeUnknown(fmt.Errorf("send RabbitMQ message: %w", err))
	}
	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		healthy = false
		return mq.OutcomeUnknown(err)
	}
	if !ack {
		if pc.ch.IsClosed() {
			healthy = false
			return mq.OutcomeUnknown(ErrConsumerClosed)
		}
		return ErrPublishNack
	}
	select {
	case returned, open := <-pc.returns:
		if !open {
			healthy = false
			return mq.OutcomeUnknown(ErrConsumerClosed)
		}
		return fmt.Errorf("%w: RabbitMQ routing key %q (code %d)", mq.ErrNoRoute, key, returned.ReplyCode)
	default:
		return nil
	}
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

func (a *Adapter) waitRetry(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
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

func (a *Adapter) deliveryExpired(runCtx, deliveryCtx context.Context) error {
	if deliveryCtx.Err() == nil {
		return nil
	}
	if err := runCtx.Err(); err != nil {
		return err
	}
	select {
	case <-a.closedCh:
		return mq.ErrClosed
	default:
		return deliveryCtx.Err()
	}
}

func (a *Adapter) deliveryContext(parent context.Context) (context.Context, func()) {
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

var _ mq.Subscriber = (*Adapter)(nil)
