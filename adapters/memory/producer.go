package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/google/uuid"
)

// Producer 内存生产者
type Producer struct {
	queues     map[string]*Queue // topic -> queue
	delayQueue *DelayQueue       // 延时队列引用
	recorder   *observability.MetricsRecorder
	keyPrefix  string
	config     config.MemoryConfig
	mu         sync.RWMutex
	closed     bool
}

// NewMemoryProducer 创建内存生产者
func NewMemoryProducer(cfg config.MemoryConfig, recorder *observability.MetricsRecorder, keyPrefix string) *Producer {
	return &Producer{
		queues:    make(map[string]*Queue),
		recorder:  recorder,
		keyPrefix: keyPrefix,
		config:    cfg,
	}
}

// SetDelayQueue 设置延时队列引用
func (p *Producer) SetDelayQueue(delayQueue *DelayQueue) {
	p.delayQueue = delayQueue
}

// Send 发送消息
func (p *Producer) Send(ctx context.Context, msg *message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("producer is closed")
	}

	start := time.Now()
	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(0)

	// 获取或创建队列
	queue := p.getOrCreateQueue(msg.Topic)

	// 发送消息到队列
	if err := queue.Push(msg); err != nil {
		if p.recorder != nil {
			p.recorder.RecordProcessingError(ctx, msg.Topic, "send_error")
		}
		return fmt.Errorf("failed to send message: %w", err)
	}

	// 记录指标
	if p.recorder != nil {
		p.recorder.RecordMessageLatency(ctx, msg.Topic, time.Since(start))
		p.recorder.RecordThroughput(ctx, msg.Topic, 1)
		p.recorder.RecordMessageSent(ctx, msg.Topic)
	}

	return nil
}

// SendDelay 发送延时消息
func (p *Producer) SendDelay(ctx context.Context, msg *message.Message, delay time.Duration) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("producer is closed")
	}

	if p.delayQueue == nil {
		return fmt.Errorf("delay queue not initialized")
	}

	start := time.Now()
	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(delay)

	// 发送到延时队列
	if err := p.delayQueue.Push(ctx, msg, delay); err != nil {
		if p.recorder != nil {
			p.recorder.RecordProcessingError(ctx, msg.Topic, "delay_send_error")
		}
		return fmt.Errorf("failed to send delay message: %w", err)
	}

	// 记录指标
	if p.recorder != nil {
		p.recorder.RecordMessageLatency(ctx, msg.Topic, time.Since(start))
		p.recorder.RecordMessageSent(ctx, msg.Topic) // 延时消息也算作已发送
	}

	return nil
}

// SendBatch 批量发送消息
func (p *Producer) SendBatch(ctx context.Context, msgs []*message.Message) error {
	for _, msg := range msgs {
		if err := p.Send(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	return nil
}

// getOrCreateQueue 获取或创建队列
func (p *Producer) getOrCreateQueue(topic string) *Queue {
	queue, exists := p.queues[topic]
	if !exists {
		queue = NewQueue(p.config.MaxQueueSize)
		p.queues[topic] = queue
	}
	return queue
}

// GetQueue 获取指定topic的队列（用于消费者）
func (p *Producer) GetQueue(topic string) *Queue {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.queues[topic]
}
