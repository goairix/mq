package otelmq

import (
	"context"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type observedPublisher struct {
	instrumentation *Instrumentation
	target          mq.Publisher
}

func (w observedPublisher) Publish(ctx context.Context, message mq.Message) error {
	ctx, span := w.instrumentation.tracer.Start(ctx, "mq.publish", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("topic", message.Topic)))
	defer span.End()
	start := time.Now()
	err := w.target.Publish(ctx, cloneWithTrace(ctx, w.instrumentation.propagator, message))
	w.instrumentation.recordPublish(ctx, "publish", message.Topic, publishResult(err), 1, len(message.Payload), time.Since(start))
	recordSpanError(span, err)
	return err
}

func (w observedPublisher) Close(ctx context.Context) error { return w.target.Close(ctx) }

type observedBatchPublisher struct {
	instrumentation *Instrumentation
	target          mq.BatchPublisher
}

func (w observedBatchPublisher) PublishBatch(ctx context.Context, messages []mq.Message) []mq.PublishResult {
	ctx, span := w.instrumentation.tracer.Start(ctx, "mq.publish.batch", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.Int("message.count", len(messages))))
	defer span.End()
	copied := make([]mq.Message, len(messages))
	for index, message := range messages {
		copied[index] = cloneWithTrace(ctx, w.instrumentation.propagator, message)
	}
	start := time.Now()
	results := w.target.PublishBatch(ctx, copied)
	duration := time.Since(start)
	type group struct{ topic, outcome string }
	type amount struct{ messages, bytes int }
	counts := make(map[group]amount)
	batchOutcome := "empty"
	for index, result := range results {
		if index >= len(messages) {
			break
		}
		outcome := "unknown"
		switch result.State {
		case mq.PublishAccepted:
			outcome = "accepted"
		case mq.PublishRejected:
			outcome = "rejected"
		}
		if index == 0 {
			batchOutcome = outcome
		} else if batchOutcome != outcome {
			batchOutcome = "mixed"
		}
		key := group{topic: messages[index].Topic, outcome: outcome}
		value := counts[key]
		value.messages++
		value.bytes += len(messages[index].Payload)
		counts[key] = value
		if result.Err != nil {
			recordSpanError(span, result.Err)
		}
	}
	for group, amount := range counts {
		w.instrumentation.recordPublishCount(ctx, "publish_batch", group.topic, group.outcome, amount.messages, amount.bytes)
	}
	topic, outcome := "__empty__", batchOutcome
	if len(messages) > 0 {
		topic = messages[0].Topic
		for _, message := range messages {
			if message.Topic != topic {
				topic = "__mixed__"
				break
			}
		}
	}
	if len(results) != len(messages) {
		outcome = "invalid"
		recordSpanError(span, fmt.Errorf("batch publisher returned %d results for %d messages", len(results), len(messages)))
	}
	w.instrumentation.recordPublishDuration(ctx, "publish_batch", topic, outcome, duration)
	return results
}

type observedSubscriber struct {
	instrumentation *Instrumentation
	target          mq.Subscriber
}

func (w observedSubscriber) Run(ctx context.Context, sub mq.Subscription, handler mq.Handler) error {
	if handler == nil {
		return w.target.Run(ctx, sub, nil)
	}
	return w.target.Run(ctx, sub, func(handlerCtx context.Context, message mq.Message) error {
		parent := w.instrumentation.extract(handlerCtx, message)
		spanCtx, span := w.instrumentation.tracer.Start(parent, "mq.consume", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name)))
		defer span.End()
		w.instrumentation.recordConsumeStart(spanCtx, sub, 1, message.CreatedAt, time.Now())
		defer w.instrumentation.recordConsumeFinish(spanCtx, sub, 1)
		start := time.Now()
		err := handler(spanCtx, message)
		w.instrumentation.recordConsume(spanCtx, sub, consumeResult(err), 1, len(message.Payload), time.Since(start))
		recordSpanError(span, err)
		return err
	})
}

type observedBatchSubscriber struct {
	instrumentation *Instrumentation
	target          mq.BatchSubscriber
}

func (w observedBatchSubscriber) RunBatch(ctx context.Context, sub mq.Subscription, options mq.BatchOptions, handler mq.BatchHandler) error {
	if handler == nil {
		return w.target.RunBatch(ctx, sub, options, nil)
	}
	return w.target.RunBatch(ctx, sub, options, func(handlerCtx context.Context, messages []mq.Message) ([]error, error) {
		links := make([]trace.Link, 0, len(messages))
		var oldest time.Time
		payloadBytes := 0
		for _, message := range messages {
			payloadBytes += len(message.Payload)
			if !message.CreatedAt.IsZero() && (oldest.IsZero() || message.CreatedAt.Before(oldest)) {
				oldest = message.CreatedAt
			}
			spanContext := trace.SpanContextFromContext(w.instrumentation.extract(handlerCtx, message))
			if spanContext.IsValid() {
				links = append(links, trace.Link{SpanContext: spanContext})
			}
		}
		// Keep each span below the Go SDK's default link limit (128). Extra
		// links live on child spans, preserving every incoming trace relation.
		const linksPerSpan = 64
		primary := links[:min(len(links), linksPerSpan)]
		spanCtx, span := w.instrumentation.tracer.Start(handlerCtx, "mq.consume.batch", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithLinks(primary...), trace.WithAttributes(attribute.String("topic", sub.Topic), attribute.String("subscription", sub.Name), attribute.Int("message.count", len(messages))))
		defer span.End()
		for start := linksPerSpan; start < len(links); start += linksPerSpan {
			_, linked := w.instrumentation.tracer.Start(spanCtx, "mq.consume.batch.links", trace.WithLinks(links[start:min(start+linksPerSpan, len(links))]...), trace.WithAttributes(attribute.Int("link.group", start/linksPerSpan)))
			linked.End()
		}
		w.instrumentation.recordConsumeStart(spanCtx, sub, len(messages), oldest, time.Now())
		defer w.instrumentation.recordConsumeFinish(spanCtx, sub, len(messages))
		start := time.Now()
		results, err := handler(spanCtx, messages)
		duration := time.Since(start)
		if err != nil {
			w.instrumentation.recordConsume(spanCtx, sub, "retry", len(messages), payloadBytes, duration)
			recordSpanError(span, err)
			return results, err
		}
		if results != nil && len(results) != len(messages) {
			w.instrumentation.recordConsume(spanCtx, sub, "invalid", len(messages), payloadBytes, duration)
			recordSpanError(span, fmt.Errorf("batch handler returned %d results for %d messages", len(results), len(messages)))
			return results, nil
		}
		if results == nil {
			w.instrumentation.recordConsume(spanCtx, sub, "success", len(messages), payloadBytes, duration)
			return nil, nil
		}
		type amount struct{ messages, bytes int }
		counts := make(map[string]amount)
		for index, result := range results {
			outcome := consumeResult(result)
			value := counts[outcome]
			value.messages++
			value.bytes += len(messages[index].Payload)
			counts[outcome] = value
			if result != nil {
				recordSpanError(span, result)
			}
		}
		for result, amount := range counts {
			w.instrumentation.recordConsumeCount(spanCtx, sub, result, amount.messages, amount.bytes)
		}
		outcome := "mixed"
		if len(counts) == 1 {
			for result := range counts {
				outcome = result
			}
		}
		w.instrumentation.recordConsumeDuration(spanCtx, sub, outcome, duration)
		return results, nil
	})
}

type observedScheduledPublisher struct {
	instrumentation *Instrumentation
	target          mq.ScheduledPublisher
}

func (w observedScheduledPublisher) PublishAt(ctx context.Context, message mq.Message, due time.Time) error {
	ctx, span := w.instrumentation.tracer.Start(ctx, "mq.schedule", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("topic", message.Topic)))
	defer span.End()
	start := time.Now()
	err := w.target.PublishAt(ctx, cloneWithTrace(ctx, w.instrumentation.propagator, message), due)
	w.instrumentation.recordPublish(ctx, "schedule", message.Topic, publishResult(err), 1, len(message.Payload), time.Since(start))
	recordSpanError(span, err)
	return err
}

var _ mq.Publisher = observedPublisher{}
var _ mq.BatchPublisher = observedBatchPublisher{}
var _ mq.Subscriber = observedSubscriber{}
var _ mq.BatchSubscriber = observedBatchSubscriber{}
var _ mq.ScheduledPublisher = observedScheduledPublisher{}
