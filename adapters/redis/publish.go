package redisadapter

import (
	"context"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

func (a *Adapter) Publish(ctx context.Context, message mq.Message) error {
	if err := a.begin(); err != nil {
		return err
	}
	defer a.end()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := message.Validate(); err != nil {
		return err
	}
	if err := a.client.XAdd(ctx, &redis.XAddArgs{Stream: a.streamKey(message.Topic), Values: encode(message)}).Err(); err != nil {
		return mq.OutcomeUnknown(err)
	}
	return nil
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
	for start := 0; start < len(messages); start += a.options.PublishBatchSize {
		end := start + a.options.PublishBatchSize
		if end > len(messages) {
			end = len(messages)
		}
		if err := ctx.Err(); err != nil {
			for i := start; i < len(messages); i++ {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			}
			break
		}
		pipeline := a.client.Pipeline()
		commands := make([]*redis.StringCmd, end-start)
		for i := start; i < end; i++ {
			if err := messages[i].Validate(); err != nil {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
				continue
			}
			commands[i-start] = pipeline.XAdd(ctx, &redis.XAddArgs{Stream: a.streamKey(messages[i].Topic), Values: encode(messages[i])})
		}
		if pipeline.Len() > 0 {
			_, _ = pipeline.Exec(ctx)
		}
		for offset, command := range commands {
			if command == nil {
				continue
			}
			i := start + offset
			if err := command.Err(); err != nil {
				results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(err)}
			} else if command.Val() == "" {
				results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(nil)}
			} else {
				results[i] = mq.PublishResult{State: mq.PublishAccepted}
			}
		}
	}
	return results
}

var _ mq.Publisher = (*Adapter)(nil)
var _ mq.BatchPublisher = (*Adapter)(nil)
