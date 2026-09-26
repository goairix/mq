package rabbitdelay

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Run releases due messages. Multiple processes may run workers for one topology.
// A failed target is confirmed into a retry bucket before the release ACK.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.begin(); err != nil {
		return err
	}
	defer s.end()
	if err := s.prepare(ctx); err != nil {
		return err
	}
	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	workCtx, finish := s.workContext(ctx)
	defer finish()
	var attempt int
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-s.closedCh:
			return mq.ErrClosed
		default:
		}
		delivery, available, err := ch.Get(s.releaseQueue(), false)
		if err != nil {
			return fmt.Errorf("get RabbitMQ delay release: %w", err)
		}
		if !available {
			if err := s.wait(ctx, s.options.PollInterval); err != nil {
				return err
			}
			continue
		}
		for {
			if err := s.process(workCtx, delivery); err == nil {
				if workCtx.Err() != nil {
					return s.stopped(ctx, workCtx)
				}
				if err := delivery.Ack(false); err != nil {
					return err
				}
				attempt = 0
				break
			}
			attempt++
			if err := s.wait(ctx, s.retryDelay(attempt)); err != nil {
				return err
			}
		}
	}
}

func (s *Scheduler) process(ctx context.Context, delivery amqp.Delivery) error {
	r, err := decode(delivery.Body)
	if err != nil {
		return s.quarantine(ctx, delivery, fmt.Errorf("decode delay record: %w", err))
	}
	if r.Message.SizeBytes() > s.options.MaxMessageBytes {
		return s.quarantine(ctx, delivery, fmt.Errorf("delay record exceeds MaxMessageBytes %d", s.options.MaxMessageBytes))
	}
	if !r.Due.After(time.Now()) {
		if err := s.target.Publish(ctx, r.Message); err == nil {
			return nil
		}
		r.Attempts++
		body, err := json.Marshal(r)
		if err != nil {
			return err
		}
		return s.publish(ctx, s.bucketQueue(chooseRetryBucket(s.retryDelay(r.Attempts))), r.Message.ID, body)
	}
	return s.publish(ctx, s.bucketQueue(chooseBucket(time.Until(r.Due))), r.Message.ID, delivery.Body)
}

func (s *Scheduler) quarantine(ctx context.Context, delivery amqp.Delivery, cause error) error {
	return s.publishWithHeaders(ctx, s.failedQueue(), delivery.MessageId, delivery.Body, amqp.Table{"mq.delay.failure": cause.Error()})
}

func chooseRetryBucket(wait time.Duration) time.Duration {
	for _, bucket := range buckets {
		if bucket >= wait {
			return bucket
		}
	}
	return buckets[len(buckets)-1]
}

func (s *Scheduler) retryDelay(attempt int) time.Duration {
	delay := s.options.RetryMin
	for i := 1; i < attempt && delay < s.options.RetryMax; i++ {
		if delay >= s.options.RetryMax/2 {
			return s.options.RetryMax
		}
		delay *= 2
	}
	if delay > s.options.RetryMax {
		return s.options.RetryMax
	}
	return delay
}

func (s *Scheduler) wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closedCh:
		return mq.ErrClosed
	case <-timer.C:
		return nil
	}
}

func (s *Scheduler) workContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
		case <-s.closedCh:
		case <-stop:
			return
		}
		timer := time.NewTimer(s.options.DrainTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}

func (s *Scheduler) stopped(parent, workCtx context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	select {
	case <-s.closedCh:
		return mq.ErrClosed
	default:
		return workCtx.Err()
	}
}
