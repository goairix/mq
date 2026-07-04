package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Producer Kafka生产者
type Producer struct {
	writer    *kafka.Writer
	meter     metric.Meter
	logger    *zap.Logger
	keyPrefix string
}

// NewKafkaProducer 创建Kafka生产者
func NewKafkaProducer(brokers []string, observer observability.Observer, keyPrefix string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &Producer{
		writer:    writer,
		meter:     observer.GetMeter(),
		logger:    observer.GetLogger(),
		keyPrefix: keyPrefix,
	}
}

// Send 发送消息
func (p *Producer) Send(ctx context.Context, msg *message.Message) error {
	start := time.Now()
	defer func() {
		if duration, err := p.meter.Float64Histogram("producer_send_duration"); err == nil {
			duration.Record(ctx, time.Since(start).Seconds())
		}
	}()

	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)

	body, err := json.Marshal(msg)
	if err != nil {
		if counter, cerr := p.meter.Int64Counter("producer_send_errors"); cerr == nil {
			counter.Add(ctx, 1)
		}
		return fmt.Errorf("marshal message failed: %w", err)
	}

	// 构建Kafka消息头
	headers := make([]kafka.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	topicName := fmt.Sprintf("%s:%s", p.keyPrefix, msg.Topic)

	kafkaMsg := kafka.Message{
		Topic:   topicName,
		Key:     []byte(msg.ID),
		Value:   body,
		Headers: headers,
		Time:    msg.CreateAt,
	}

	err = p.writer.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		if counter, cerr := p.meter.Int64Counter("producer_send_errors"); cerr == nil {
			counter.Add(ctx, 1)
		}
		p.logger.Error("send message failed", zap.Error(err), zap.String("topic", msg.Topic))
		return fmt.Errorf("send message failed: %w", err)
	}

	if counter, cerr := p.meter.Int64Counter("producer_send_total"); cerr == nil {
		counter.Add(ctx, 1)
	}
	p.logger.Info("message sent", zap.String("id", msg.ID), zap.String("topic", msg.Topic))
	return nil
}

// SendDelay 发送延时消息
func (p *Producer) SendDelay(ctx context.Context, msg *message.Message, delay time.Duration) error {
	// Kafka本身不支持延时消息，需要通过延时队列实现
	msg.SetDelay(delay)
	return p.Send(ctx, msg)
}

// SendBatch 批量发送消息
func (p *Producer) SendBatch(ctx context.Context, msgs []*message.Message) error {
	kafkaMsgs := make([]kafka.Message, 0, len(msgs))

	for _, msg := range msgs {
		if msg.ID == "" {
			msg.SetID(uuid.NewString())
		}
		msg.SetCreateAt(time.Now())
		msg.SetDelay(0)

		body, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal message failed: %w", err)
		}

		headers := make([]kafka.Header, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			headers = append(headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}

		topicName := fmt.Sprintf("%s:%s", p.keyPrefix, msg.Topic)

		kafkaMsgs = append(kafkaMsgs, kafka.Message{
			Topic:   topicName,
			Key:     []byte(msg.ID),
			Value:   body,
			Headers: headers,
			Time:    msg.CreateAt,
		})
	}

	err := p.writer.WriteMessages(ctx, kafkaMsgs...)
	if err != nil {
		if counter, cerr := p.meter.Int64Counter("producer_batch_send_errors"); cerr == nil {
			counter.Add(ctx, 1)
		}
		return fmt.Errorf("batch send failed: %w", err)
	}

	if counter, cerr := p.meter.Int64Counter("producer_send_total"); cerr == nil {
		counter.Add(ctx, int64(len(msgs)))
	}
	return nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.writer.Close()
}
