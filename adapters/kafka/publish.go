package kafkaadapter

import (
	"context"
	"errors"
	"fmt"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (a *Adapter) Publish(ctx context.Context, message mq.Message) error {
	return a.PublishBatch(ctx, []mq.Message{message})[0].Err
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
	records := make([]*kgo.Record, 0, a.options.BatchMaxMessages)
	indices := make([]int, 0, a.options.BatchMaxMessages)
	batchBytes := 0
	flush := func() {
		if len(records) == 0 {
			return
		}
		if err := ctx.Err(); err != nil {
			for _, i := range indices {
				results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			}
		} else {
			positions := make(map[*kgo.Record]int, len(records))
			for j, r := range records {
				positions[r] = indices[j]
			}
			for _, result := range a.producer.ProduceSync(ctx, records...) {
				i, ok := positions[result.Record]
				if !ok {
					continue
				}
				delete(positions, result.Record)
				if result.Err == nil {
					results[i] = mq.PublishResult{State: mq.PublishAccepted}
				} else if errors.Is(result.Err, kerr.MessageTooLarge) {
					results[i] = mq.PublishResult{State: mq.PublishRejected, Err: result.Err}
				} else {
					results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(result.Err)}
				}
			}
			for _, i := range positions {
				results[i] = mq.PublishResult{State: mq.PublishUnknown, Err: mq.OutcomeUnknown(errors.New("Kafka producer result missing"))}
			}
		}
		records = records[:0]
		indices = indices[:0]
		batchBytes = 0
	}
	for i, message := range messages {
		if err := ctx.Err(); err != nil {
			flush()
			for j := i; j < len(messages); j++ {
				results[j] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			}
			break
		}
		if err := message.Validate(); err != nil {
			results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			continue
		}
		size := message.SizeBytes()
		if size > a.options.MaxMessageBytes {
			results[i] = mq.PublishResult{State: mq.PublishRejected, Err: fmt.Errorf("message size %d exceeds MaxMessageBytes %d", size, a.options.MaxMessageBytes)}
			continue
		}
		if len(records) >= a.options.BatchMaxMessages || (len(records) > 0 && size > a.options.BatchMaxBytes-batchBytes) {
			flush()
		}
		if err := ctx.Err(); err != nil {
			results[i] = mq.PublishResult{State: mq.PublishRejected, Err: err}
			continue
		}
		records = append(records, encode(message))
		indices = append(indices, i)
		batchBytes += size
	}
	flush()
	return results
}

var _ mq.Publisher = (*Adapter)(nil)
var _ mq.BatchPublisher = (*Adapter)(nil)
