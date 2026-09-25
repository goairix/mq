package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
)

func (b *Broker) PublishBatch(ctx context.Context, messages []mq.Message) []mq.PublishResult {
	results := make([]mq.PublishResult, len(messages))
	for i, message := range messages {
		err := b.Publish(ctx, message)
		switch {
		case err == nil:
			results[i].State = mq.PublishAccepted
		case mq.IsOutcomeUnknown(err):
			results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: err}
		default:
			results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
		}
	}
	return results
}

func (b *Broker) RunBatch(ctx context.Context, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler) error {
	if err := sub.Validate(); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil batch handler")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return mq.ErrClosed
		}
		topic, group := b.groupLocked(sub)
		var batch []mq.Message
		var indices []uint64
		var size int
		if !group.busy && !time.Now().Before(group.nextRetry) {
			for index := group.next; index-topic.base < uint64(len(topic.messages)) && len(batch) < options.MaxMessages; index++ {
				if group.completed[index] {
					continue
				}
				message := topic.messages[index-topic.base]
				messageSize := message.SizeBytes()
				if messageSize > options.MaxInFlightBytes {
					b.mu.Unlock()
					return fmt.Errorf("message %s size %d exceeds MaxInFlightBytes %d", message.ID, messageSize, options.MaxInFlightBytes)
				}
				if len(batch) > 0 && (size+messageSize > options.MaxBytes || size+messageSize > options.MaxInFlightBytes) {
					break
				}
				batch = append(batch, cloneMessage(message))
				indices = append(indices, index)
				size += messageSize
				if size >= options.MaxBytes {
					break
				}
			}
			if len(batch) > 0 {
				group.busy = true
			}
		}
		wake, retryAt := b.wake, group.nextRetry
		if group.busy && len(batch) == 0 {
			retryAt = time.Time{}
		}
		b.mu.Unlock()
		if len(batch) == 0 {
			if err := waitFor(ctx, wake, retryAt); err != nil {
				return err
			}
			continue
		}
		results, handlerErr := handler(ctx, batch)
		if handlerErr == nil && results != nil && len(results) != len(batch) {
			b.mu.Lock()
			_, group = b.groupLocked(sub)
			group.busy = false
			b.signalLocked()
			b.mu.Unlock()
			return fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(batch))
		}
		b.mu.Lock()
		_, group = b.groupLocked(sub)
		group.busy = false
		failed := handlerErr != nil
		for i, index := range indices {
			if handlerErr != nil {
				continue
			}
			var result error
			if results != nil {
				result = results[i]
			}
			switch {
			case result == nil:
				group.completed[index] = true
			case mq.IsPermanent(result):
				group.deadLetter = append(group.deadLetter, DeadLetter{Message: cloneMessage(batch[i]), Reason: result.Error()})
				group.completed[index] = true
			default:
				failed = true
			}
		}
		for group.completed[group.next] {
			delete(group.completed, group.next)
			group.next++
		}
		if failed {
			group.attempts++
			group.nextRetry = time.Now().Add(retryDelay(group.attempts))
		} else {
			group.attempts = 0
			group.nextRetry = time.Time{}
		}
		b.signalLocked()
		b.mu.Unlock()
	}
}

var _ mq.BatchPublisher = (*Broker)(nil)
var _ mq.BatchSubscriber = (*Broker)(nil)
