package rabbitadapter

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrPublishNack = errors.New("RabbitMQ publisher NACK")

type publishChannel struct {
	ch      *amqp.Channel
	returns chan amqp.Return
	serial  uint64
}

func (a *Adapter) initPool() {
	a.poolOnce.Do(func() {
		a.pool = make(chan *publishChannel, a.options.PublishChannels)
		for i := 0; i < a.options.PublishChannels; i++ {
			a.pool <- nil
		}
	})
}

func (a *Adapter) newPublishChannel() (*publishChannel, error) {
	ch, err := a.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, err
	}
	pc := &publishChannel{ch: ch, returns: make(chan amqp.Return, a.options.PublishBatchSize)}
	ch.NotifyReturn(pc.returns)
	return pc, nil
}

func (a *Adapter) borrow(ctx context.Context) (*publishChannel, error) {
	a.initPool()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.closedCh:
		return nil, mq.ErrClosed
	case pc := <-a.pool:
		if pc != nil {
			return pc, nil
		}
		created, err := a.newPublishChannel()
		if err != nil {
			a.pool <- nil
			return nil, err
		}
		return created, nil
	}
}

func (a *Adapter) release(pc *publishChannel, healthy bool) {
	if !healthy {
		if pc != nil {
			_ = pc.ch.Close()
		}
		pc = nil
	}
	a.pool <- pc
}

func (a *Adapter) Publish(ctx context.Context, message mq.Message) error {
	results := a.PublishBatch(ctx, []mq.Message{message})
	return results[0].Err
}

type outstanding struct {
	index        int
	correlation  string
	confirmation *amqp.DeferredConfirmation
}

func (a *Adapter) PublishBatch(ctx context.Context, messages []mq.Message) []mq.PublishResult {
	results := make([]mq.PublishResult, len(messages))
	if len(messages) == 0 {
		return results
	}
	if err := a.begin(); err != nil {
		for i := range results {
			results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
		}
		return results
	}
	defer a.end()
	for start := 0; start < len(messages); {
		end := start
		chunkBytes := 0
		for end < len(messages) && end-start < a.options.PublishBatchSize {
			size := messages[end].SizeBytes()
			if size > a.options.MaxPublishBytes {
				if end == start {
					results[end] = mq.PublishResult{State: mq.PublishRejected, Err: fmt.Errorf("message size %d exceeds MaxPublishBytes %d", size, a.options.MaxPublishBytes)}
					end++
				}
				break
			}
			if end > start && size > a.options.MaxPublishBytes-chunkBytes {
				break
			}
			chunkBytes += size
			end++
		}
		if results[start].Err != nil && end == start+1 {
			start = end
			continue
		}
		if err := ctx.Err(); err != nil {
			for i := start; i < len(messages); i++ {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			}
			break
		}
		pc, err := a.borrow(ctx)
		if err != nil {
			state := mq.PublishUnknown
			wrapped := mq.OutcomeUnknown(err)
			if errors.Is(err, ctx.Err()) || errors.Is(err, mq.ErrClosed) {
				state = mq.PublishRejected
				wrapped = err
			}
			for i := start; i < end; i++ {
				if validErr := messages[i].Validate(); validErr != nil {
					results[i] = mq.PublishResult{State: mq.PublishRejected, Err: validErr}
				} else {
					results[i] = mq.PublishResult{State: state, Err: wrapped}
				}
			}
			start = end
			continue
		}
		healthy := true
		pending := make([]outstanding, 0, end-start)
		for i := start; i < end; i++ {
			if err := messages[i].Validate(); err != nil {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
				continue
			}
			publishing := encode(messages[i])
			if err := publishing.Headers.Validate(); err != nil {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
				continue
			}
			if !healthy {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: errors.New("publishing channel failed before dispatch")}
				continue
			}
			pc.serial++
			publishing.CorrelationId = strconv.FormatUint(pc.serial, 10)
			confirmation, err := pc.ch.PublishWithDeferredConfirmWithContext(ctx, a.options.Exchange, a.routeKey(messages[i].Topic), true, false, publishing)
			if err != nil || confirmation == nil {
				if err == nil {
					err = errors.New("RabbitMQ confirm mode unavailable")
				}
				results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(fmt.Errorf("publish RabbitMQ message: %w", err))}
				healthy = false
				continue
			}
			pending = append(pending, outstanding{index: i, correlation: publishing.CorrelationId, confirmation: confirmation})
		}
		if !healthy {
			for _, item := range pending {
				results[item.index] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(errors.New("publishing channel failed before confirmation"))}
			}
			a.release(pc, false)
			start = end
			continue
		}
		for position, item := range pending {
			ack, waitErr := item.confirmation.WaitContext(ctx)
			if waitErr != nil {
				for _, remaining := range pending[position:] {
					results[remaining.index] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(waitErr)}
				}
				healthy = false
				break
			}
			if !ack && pc.ch.IsClosed() {
				results[item.index] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(errors.New("RabbitMQ channel closed before confirmation"))}
				healthy = false
				continue
			}
			if ack {
				results[item.index] = mq.PublishResult{State: mq.PublishAccepted}
			} else {
				results[item.index] = mq.PublishResult{State: mq.PublishRejected, Err: ErrPublishNack}
			}
		}
		returned := make(map[string]bool)
		for {
			select {
			case result, open := <-pc.returns:
				if !open {
					healthy = false
					goto drained
				}
				returned[result.CorrelationId] = true
			default:
				goto drained
			}
		}
	drained:
		for _, item := range pending {
			if returned[item.correlation] && results[item.index].State == mq.PublishAccepted {
				results[item.index] = mq.PublishResult{State: mq.PublishRejected, Err: fmt.Errorf("%w: topic %q", mq.ErrNoRoute, messages[item.index].Topic)}
			}
		}
		a.release(pc, healthy)
		start = end
	}
	return results
}

var _ mq.Publisher = (*Adapter)(nil)
var _ mq.BatchPublisher = (*Adapter)(nil)
