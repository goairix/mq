package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/segmentio/kafka-go"
)

// Consumer Kafka消费者
type Consumer struct {
	brokers     []string
	meter       metric.Meter
	logger      *zap.Logger
	readers     map[string]*kafka.Reader
	subscribers map[string]context.CancelFunc
	mu          sync.RWMutex
	keyPrefix   string // 添加keyPrefix字段
}

// NewKafkaConsumer 创建Kafka消费者
func NewKafkaConsumer(brokers []string, observer observability.Observer, keyPrefix string) *Consumer {
	return &Consumer{
		brokers:     brokers,
		meter:       observer.GetMeter(),
		logger:      observer.GetLogger(),
		readers:     make(map[string]*kafka.Reader),
		subscribers: make(map[string]context.CancelFunc),
		keyPrefix:   keyPrefix,
	}
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler message.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	topicName := fmt.Sprintf("%s:%s", c.keyPrefix, topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.brokers,
		Topic:          topicName,
		GroupID:        fmt.Sprintf("%s-group", topicName),
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	c.readers[topic] = reader

	// 创建取消上下文
	subCtx, cancel := context.WithCancel(ctx)
	c.subscribers[topic] = cancel

	// 启动消费协程
	go c.consumeLoop(subCtx, topic, reader, handler)

	c.logger.Info("consumer started", zap.String("topic", topic))
	return nil
}

// consumeLoop 消费循环
func (c *Consumer) consumeLoop(ctx context.Context, topic string, reader *kafka.Reader, handler message.Handler) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("consumer stopped", zap.String("topic", topic))
			return
		default:
			// 读取消息
			kafkaMsg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // 上下文已取消
				}
				c.logger.Error("read message failed", zap.Error(err), zap.String("topic", topic))
				continue
			}

			// 解析消息
			var msg message.Message
			if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
				c.logger.Error("unmarshal message failed", zap.Error(err), zap.String("topic", topic))
				if counter, cerr := c.meter.Int64Counter("consumer_unmarshal_errors"); cerr == nil {
					counter.Add(ctx, 1)
				}
				continue
			}

			// 转换头部信息
			if msg.Headers == nil {
				msg.Headers = make(map[string]string)
			}
			for _, header := range kafkaMsg.Headers {
				msg.Headers[header.Key] = string(header.Value)
			}

			// 处理消息
			if err := handler(ctx, &msg); err != nil {
				c.logger.Error("handle message failed", zap.Error(err), zap.String("topic", topic), zap.String("msgId", msg.ID))
				if counter, cerr := c.meter.Int64Counter("consumer_handle_errors"); cerr == nil {
					counter.Add(ctx, 1)
				}
			} else {
				if counter, cerr := c.meter.Int64Counter("consumer_handle_success"); cerr == nil {
					counter.Add(ctx, 1)
				}
			}
		}
	}
}

// Unsubscribe 取消订阅
func (c *Consumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cancel, exists := c.subscribers[topic]; exists {
		cancel()
		delete(c.subscribers, topic)
	}

	if reader, exists := c.readers[topic]; exists {
		_ = reader.Close()
		delete(c.readers, topic)
	}

	c.logger.Info("unsubscribed", zap.String("topic", topic))
	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭所有订阅
	for topic, cancel := range c.subscribers {
		cancel()
		delete(c.subscribers, topic)
	}

	// 关闭所有Reader
	for topic, reader := range c.readers {
		_ = reader.Close()
		delete(c.readers, topic)
	}

	return nil
}
