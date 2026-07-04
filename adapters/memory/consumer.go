package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
)

// Consumer 内存消费者
type Consumer struct {
	subscriptions map[string]*subscription // topic -> subscription
	producer      *Producer                // 引用生产者以访问队列
	recorder      *observability.MetricsRecorder
	keyPrefix     string
	config        config.MemoryConfig
	mu            sync.RWMutex
	closed        bool
}

// subscription 订阅信息
type subscription struct {
	topic   string
	handler message.Handler
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewMemoryConsumer 创建内存消费者
func NewMemoryConsumer(cfg config.MemoryConfig, recorder *observability.MetricsRecorder, keyPrefix string) *Consumer {
	return &Consumer{
		subscriptions: make(map[string]*subscription),
		recorder:      recorder,
		keyPrefix:     keyPrefix,
		config:        cfg,
	}
}

// SetProducer 设置生产者引用（用于访问队列）
func (c *Consumer) SetProducer(producer *Producer) {
	c.producer = producer
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler message.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	// 检查是否已经订阅
	if _, exists := c.subscriptions[topic]; exists {
		return fmt.Errorf("topic %s already subscribed", topic)
	}

	// 创建订阅上下文
	subCtx, subCancel := context.WithCancel(ctx)
	sub := &subscription{
		topic:   topic,
		handler: handler,
		ctx:     subCtx,
		cancel:  subCancel,
	}

	c.subscriptions[topic] = sub

	// 启动消费协程
	sub.wg.Add(1)
	go c.consumeMessages(sub)

	return nil
}

// Unsubscribe 取消订阅
func (c *Consumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, exists := c.subscriptions[topic]
	if !exists {
		return fmt.Errorf("topic %s not subscribed", topic)
	}

	// 取消订阅
	sub.cancel()
	sub.wg.Wait()
	delete(c.subscriptions, topic)

	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// 关闭所有订阅
	for topic := range c.subscriptions {
		sub := c.subscriptions[topic]
		sub.cancel()
		sub.wg.Wait()
	}

	c.subscriptions = make(map[string]*subscription)
	return nil
}

// consumeMessages 消费消息
func (c *Consumer) consumeMessages(sub *subscription) {
	defer sub.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond) // 10ms轮询一次
	defer ticker.Stop()

	for {
		select {
		case <-sub.ctx.Done():
			return
		case <-ticker.C:
			c.processMessages(sub)
		}
	}
}

// processMessages 处理消息
func (c *Consumer) processMessages(sub *subscription) {
	if c.producer == nil {
		return
	}

	queue := c.producer.GetQueue(sub.topic)
	if queue == nil {
		return
	}

	// 批量处理消息
	for i := 0; i < 10; i++ { // 每次最多处理10条消息
		msg := queue.Pop()
		if msg == nil {
			break
		}

		start := time.Now()
		err := sub.handler(sub.ctx, msg)
		processTime := time.Since(start)

		if c.recorder != nil {
			if err != nil {
				c.recorder.RecordMessageFailed(sub.ctx, sub.topic, err)
				c.recorder.LogError("message processing failed",
					err,
					zap.String("message_id", msg.ID),
					zap.Error(err),
				)
			} else {
				c.recorder.RecordProcessingTime(sub.ctx, sub.topic, processTime)
				c.recorder.RecordThroughput(sub.ctx, sub.topic, 1)
			}
		}

		// 如果处理失败且有重试次数，可以重新入队
		if err != nil && msg.Retry > 0 {
			msg.Retry--
			_ = queue.Push(msg) // 重新入队
		}
	}
}
