// Package memory provides a bounded, nonpersistent MQ adapter for tests.
package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	mq "github.com/goairix/mq/v2"
)

// DeadLetter is a test-visible record of a permanently failed delivery.
type DeadLetter struct {
	Message mq.Message
	Reason  string
}

type groupState struct {
	next       uint64
	busy       bool
	completed  map[uint64]bool
	attempts   int
	nextRetry  time.Time
	deadLetter []DeadLetter
}

type topicState struct {
	base     uint64
	messages []mq.Message
	groups   map[string]*groupState
}

// Broker is a bounded, process-local transport for deterministic tests.
type Broker struct {
	mu       sync.Mutex
	topics   map[string]*topicState
	capacity int
	retained int
	wake     chan struct{}
	closed   bool
}

func New(capacity int) (*Broker, error) {
	if capacity <= 0 {
		return nil, errors.New("memory capacity must be positive")
	}
	return &Broker{topics: make(map[string]*topicState), capacity: capacity, wake: make(chan struct{})}, nil
}

func cloneMessage(m mq.Message) mq.Message {
	m.Key = append([]byte(nil), m.Key...)
	m.Payload = append([]byte(nil), m.Payload...)
	if m.Headers != nil {
		headers := make(map[string]string, len(m.Headers))
		for key, value := range m.Headers {
			headers[key] = value
		}
		m.Headers = headers
	}
	return m
}

func (b *Broker) signalLocked() {
	close(b.wake)
	b.wake = make(chan struct{})
}

func (b *Broker) topicLocked(name string) *topicState {
	topic := b.topics[name]
	if topic == nil {
		topic = &topicState{groups: make(map[string]*groupState)}
		b.topics[name] = topic
	}
	return topic
}

func (b *Broker) groupLocked(sub mq.Subscription) (*topicState, *groupState) {
	topic := b.topicLocked(sub.Topic)
	group := topic.groups[sub.Name]
	if group == nil {
		group = &groupState{next: topic.base, completed: make(map[uint64]bool)}
		topic.groups[sub.Name] = group
	}
	return topic, group
}

func (b *Broker) reclaimLocked() {
	for _, topic := range b.topics {
		if len(topic.groups) == 0 {
			continue
		}
		limit := topic.base + uint64(len(topic.messages))
		for _, group := range topic.groups {
			if group.next < limit {
				limit = group.next
			}
		}
		count := int(limit - topic.base)
		if count > 0 {
			clear(topic.messages[:count])
			topic.messages = topic.messages[count:]
			topic.base = limit
			b.retained -= count
		}
	}
}

func (b *Broker) Publish(ctx context.Context, m mq.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.Validate(); err != nil {
		return err
	}
	copy := cloneMessage(m)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return mq.ErrClosed
	}
	if b.retained >= b.capacity {
		b.reclaimLocked()
	}
	if b.retained >= b.capacity {
		return mq.ErrBackpressure
	}
	topic := b.topicLocked(m.Topic)
	topic.messages = append(topic.messages, copy)
	b.retained++
	b.signalLocked()
	return nil
}

func (b *Broker) Run(ctx context.Context, sub mq.Subscription, handler mq.Handler) error {
	if err := sub.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("nil handler")
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
		var msg mq.Message
		ready := !group.busy && group.next >= topic.base && group.next-topic.base < uint64(len(topic.messages)) && !time.Now().Before(group.nextRetry)
		if ready {
			msg = cloneMessage(topic.messages[group.next-topic.base])
			group.busy = true
		}
		wake, retryAt := b.wake, group.nextRetry
		if group.busy {
			retryAt = time.Time{}
		}
		b.mu.Unlock()
		if !ready {
			if err := waitFor(ctx, wake, retryAt); err != nil {
				return err
			}
			continue
		}
		err := handler(ctx, msg)
		b.mu.Lock()
		_, group = b.groupLocked(sub)
		group.busy = false
		switch {
		case err == nil:
			group.completed[group.next] = true
			group.attempts = 0
			group.nextRetry = time.Time{}
		case mq.IsPermanent(err):
			group.deadLetter = append(group.deadLetter, DeadLetter{Message: cloneMessage(msg), Reason: err.Error()})
			group.completed[group.next] = true
			group.attempts = 0
			group.nextRetry = time.Time{}
		default:
			group.attempts++
			group.nextRetry = time.Now().Add(retryDelay(group.attempts))
		}
		for group.completed[group.next] {
			delete(group.completed, group.next)
			group.next++
		}
		b.signalLocked()
		b.mu.Unlock()
	}
}

func retryDelay(attempt int) time.Duration {
	if attempt > 7 {
		attempt = 7
	}
	return time.Duration(1<<uint(attempt-1)) * 10 * time.Millisecond
}

func waitFor(ctx context.Context, wake <-chan struct{}, retryAt time.Time) error {
	var timer *time.Timer
	var timerC <-chan time.Time
	if !retryAt.IsZero() {
		delay := time.Until(retryAt)
		if delay < 0 {
			delay = 0
		}
		timer = time.NewTimer(delay)
		timerC = timer.C
		defer timer.Stop()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-timerC:
		return nil
	}
}

func (b *Broker) DeadLetters(sub mq.Subscription) []DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	topic := b.topics[sub.Topic]
	if topic == nil || topic.groups[sub.Name] == nil {
		return nil
	}
	stored := topic.groups[sub.Name].deadLetter
	result := make([]DeadLetter, len(stored))
	for i, letter := range stored {
		result[i] = DeadLetter{Message: cloneMessage(letter.Message), Reason: letter.Reason}
	}
	return result
}

func (b *Broker) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.signalLocked()
	}
	return nil
}

var _ mq.Publisher = (*Broker)(nil)
var _ mq.Subscriber = (*Broker)(nil)
