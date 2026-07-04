package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/contract"
	"github.com/goairix/mq/observability"
)

// Memory 内存消息队列实现
type Memory struct {
	producer   *Producer
	consumer   *Consumer
	delayQueue *DelayQueue
	recorder   *observability.MetricsRecorder
	keyPrefix  string
	config     config.MemoryConfig

	// 延时消息处理
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
	mu     sync.RWMutex
}

// NewMemoryMQ 创建内存消息队列
func NewMemoryMQ(cfg config.MemoryConfig, observer observability.Observer, keyPrefix string) (*Memory, error) {
	if keyPrefix == "" {
		keyPrefix = config.DefaultKeyPrefix
	}

	// 设置默认值
	cfg.SetDefaults()

	// 创建上下文
	mainCtx, mainCancel := context.WithCancel(context.Background())
	var success bool
	defer func() {
		if !success {
			mainCancel()
		}
	}()

	// 创建指标记录器
	recorder, err := observability.NewMetricsRecorder(observer, config.AdapterMemory.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics recorder: %w", err)
	}

	// 创建组件
	producer := NewMemoryProducer(cfg, recorder, keyPrefix)
	consumer := NewMemoryConsumer(cfg, recorder, keyPrefix)
	delayQueue := NewMemoryDelayQueue(cfg, recorder, keyPrefix)

	// 设置组件之间的引用关系
	consumer.SetProducer(producer)
	producer.SetDelayQueue(delayQueue)

	mq := &Memory{
		producer:   producer,
		consumer:   consumer,
		delayQueue: delayQueue,
		recorder:   recorder,
		keyPrefix:  keyPrefix,
		config:     cfg,
		ctx:        mainCtx,
		cancel:     mainCancel,
	}

	// 启动延时消息处理器
	mq.startDelayProcessor()

	success = true
	return mq, nil
}

// Producer 返回生产者
func (m *Memory) Producer() contract.Producer {
	return m.producer
}

// Consumer 返回消费者
func (m *Memory) Consumer() contract.Consumer {
	return m.consumer
}

// DelayQueue 返回延时队列
func (m *Memory) DelayQueue() contract.DelayQueue {
	return m.delayQueue
}

// HealthCheck 健康检查
func (m *Memory) HealthCheck() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return fmt.Errorf("memory mq is closed")
	}
	return nil
}

// Close 关闭消息队列
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	m.cancel()
	m.wg.Wait()

	// 关闭各个组件
	if err := m.producer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	if err := m.consumer.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}

	return nil
}

// startDelayProcessor 启动延时消息处理器
func (m *Memory) startDelayProcessor() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.config.DelayCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				m.processDelayedMessages()
			}
		}
	}()
}

// processDelayedMessages 处理延时消息
func (m *Memory) processDelayedMessages() {
	for {
		msg, err := m.delayQueue.Pop(m.ctx)
		if err != nil || msg == nil {
			break
		}

		// 将延时消息发送到普通队列
		if err := m.producer.Send(m.ctx, msg); err != nil {
			// 记录错误但继续处理其他消息
			continue
		}
	}
}
