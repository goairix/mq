package mq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Subscription struct {
	Topic string
	Name  string
}

func (s Subscription) Validate() error {
	if strings.TrimSpace(s.Topic) == "" {
		return errors.New("subscription topic is required")
	}
	if strings.TrimSpace(s.Name) == "" {
		return errors.New("subscription name is required")
	}
	return nil
}

// BatchOptions bounds each delivery batch and concurrent in-flight work.
// MaxBytes uses Message.SizeBytes. MaxWait == 0 delivers available messages
// immediately. A single message larger than MaxBytes is delivered alone if
// it fits MaxInFlightBytes; one larger than MaxInFlightBytes makes RunBatch
// return an error before invoking the handler. MaxInFlightBatches and
// MaxInFlightBytes are hard upper bounds, not target concurrency levels.
type BatchOptions struct {
	MaxMessages        int
	MaxBytes           int
	MaxWait            time.Duration
	MaxInFlightBatches int
	MaxInFlightBytes   int
}

func DefaultBatchOptions() BatchOptions {
	return BatchOptions{MaxMessages: 256, MaxBytes: 1 << 20, MaxWait: 50 * time.Millisecond, MaxInFlightBatches: 8, MaxInFlightBytes: 16 << 20}
}

func (o BatchOptions) Validate() error {
	if o.MaxMessages <= 0 || o.MaxBytes <= 0 || o.MaxInFlightBatches <= 0 || o.MaxInFlightBytes < o.MaxBytes || o.MaxWait < 0 {
		return errors.New("invalid batch limits")
	}
	return nil
}

type PublishState uint8

const (
	PublishUnknown PublishState = iota
	PublishAccepted
	PublishRejected
)

type PublishResult struct {
	State PublishState
	Err   error
}

func ValidatePublishResults(expected int, results []PublishResult) error {
	if len(results) != expected {
		return fmt.Errorf("got %d publish results, want %d", len(results), expected)
	}
	for i, result := range results {
		if result.State > PublishRejected {
			return fmt.Errorf("result %d has invalid state", i)
		}
		if result.State == PublishAccepted && result.Err != nil {
			return fmt.Errorf("accepted result %d has error", i)
		}
		if result.State != PublishAccepted && result.Err == nil {
			return fmt.Errorf("failed or unknown result %d has no error", i)
		}
	}
	return nil
}

type Handler func(context.Context, Message) error

// BatchHandler returns (nil, nil) for all-success, a non-nil error for
// all-retry, or one error per message (nil=success, Permanent(err)=dead letter).
type BatchHandler func(context.Context, []Message) ([]error, error)

type Publisher interface {
	Publish(context.Context, Message) error
	Close(context.Context) error
}

type BatchPublisher interface {
	PublishBatch(context.Context, []Message) []PublishResult
}
type Subscriber interface {
	Run(context.Context, Subscription, Handler) error
}
type BatchSubscriber interface {
	RunBatch(context.Context, Subscription, BatchOptions, BatchHandler) error
}
type ScheduledPublisher interface {
	PublishAt(context.Context, Message, time.Time) error
}
