package memory

import (
	"container/heap"
	"context"
	"time"

	mq "github.com/goairix/mq/v2"
)

type scheduledMessage struct {
	due     time.Time
	message mq.Message
}

type scheduledHeap []scheduledMessage

func (h scheduledHeap) Len() int           { return len(h) }
func (h scheduledHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h scheduledHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scheduledHeap) Push(value any)    { *h = append(*h, value.(scheduledMessage)) }
func (h *scheduledHeap) Pop() any {
	last := len(*h) - 1
	value := (*h)[last]
	(*h)[last] = scheduledMessage{}
	*h = (*h)[:last]
	return value
}

// PublishAt schedules a message in process memory. The schedule and queued
// message disappear when the broker closes or the process exits.
func (b *Broker) PublishAt(ctx context.Context, message mq.Message, due time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := message.Validate(); err != nil {
		return err
	}
	if !due.After(time.Now()) {
		return b.Publish(ctx, message)
	}
	copy := cloneMessage(message)
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
	heap.Push(&b.scheduled, scheduledMessage{due: due, message: copy})
	b.retained++
	if b.schedulerDone == nil {
		b.schedulerDone = make(chan struct{})
		go b.scheduleLoop()
	}
	b.signalLocked()
	return nil
}

func (b *Broker) scheduleLoop() {
	defer close(b.schedulerDone)
	for {
		b.mu.Lock()
		if b.closed {
			b.retained -= len(b.scheduled)
			clear(b.scheduled)
			b.scheduled = nil
			b.mu.Unlock()
			return
		}
		if len(b.scheduled) > 0 && !b.scheduled[0].due.After(time.Now()) {
			next := heap.Pop(&b.scheduled).(scheduledMessage)
			topic := b.topicLocked(next.message.Topic)
			topic.messages = append(topic.messages, next.message)
			b.signalLocked()
			b.mu.Unlock()
			continue
		}
		wake := b.wake
		var delay time.Duration
		hasDue := len(b.scheduled) > 0
		if hasDue {
			delay = time.Until(b.scheduled[0].due)
		}
		b.mu.Unlock()
		if !hasDue {
			<-wake
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

var _ mq.ScheduledPublisher = (*Broker)(nil)
