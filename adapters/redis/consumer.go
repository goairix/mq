package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/goairix/mq/pool"
	"github.com/goairix/mq/serializer"
	"go.uber.org/zap"
)

// Consumer Redis消费者
type Consumer struct {
	client         redis.Cmdable
	metrics        *observability.MetricsRecorder
	logger         *zap.Logger
	subscribers    map[string]*subscription
	mu             sync.RWMutex
	closed         bool
	workerPool     *WorkerPool
	keys           *KeyGenerator
	consumerConfig config.ConsumerPerformanceConfig
	config         config.RedisConfig
	serializer     serializer.Serializer // 序列化器
	messagePool    *pool.MessagePool     // 消息对象池
	bufferPool     *pool.ByteBufferPool  // 缓冲区对象池
}

// subscription 订阅信息
type subscription struct {
	topic   string
	handler message.Handler
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewRedisConsumer 创建Redis消费者
func NewRedisConsumer(
	client redis.Cmdable,
	observer observability.Observer,
	config config.RedisConfig,
	recorder *observability.MetricsRecorder,
	ser serializer.Serializer,
	keys *KeyGenerator,
) *Consumer {
	consumer := &Consumer{
		client:         client,
		metrics:        recorder,
		logger:         observer.GetLogger(),
		subscribers:    make(map[string]*subscription),
		config:         config,
		consumerConfig: config.GetConsumerConfig(),
		keys:           keys,
		serializer:     ser,
	}

	// 创建对象池（如果启用）
	var messagePool *pool.MessagePool
	var bufferPool *pool.ByteBufferPool
	if config.GetObjectPoolConfig().Enabled {
		messagePool = pool.NewMessagePool(recorder)
		bufferPool = pool.NewByteBufferPool(recorder)
	}

	consumer.serializer = ser
	consumer.messagePool = messagePool
	consumer.bufferPool = bufferPool

	// 创建工作池
	consumer.workerPool = NewWorkerPool(consumer.consumerConfig.WorkerCount, consumer.consumerConfig.BufferSize, consumer.logger, consumer.metrics)
	consumer.workerPool.Start()

	return consumer
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler message.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.New("consumer is closed")
	}

	if _, exists := c.subscribers[topic]; exists {
		return fmt.Errorf("already subscribed to topic: %s", topic)
	}

	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		topic:   topic,
		handler: handler,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	c.subscribers[topic] = sub

	// 启动消费循环
	go c.consumeLoop(ctx, sub)

	c.logger.Info("consumer started",
		zap.String("topic", topic),
		zap.String("queue_key", c.keys.QueueKey(topic)),
	)

	return nil
}

// consumeLoop 消费循环（支持批处理和工作池）
func (c *Consumer) consumeLoop(ctx context.Context, sub *subscription) {
	defer close(sub.done)
	defer c.logger.Info("consumer stopped", zap.String("topic", sub.topic))

	retryInterval := c.consumerConfig.RetryInterval

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 批量获取消息
			messages, err := c.batchPopMessages(ctx, sub.topic, c.consumerConfig.BatchSize)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					c.logger.Debug("consume loop stopped due to context cancellation",
						zap.String("topic", sub.topic), zap.Error(err))
					return
				}
				c.logger.Error("consume loop failed", zap.String("topic", sub.topic), zap.Error(err))
				c.logger.Error("failed to pop messages", zap.Error(err), zap.String("topic", sub.topic))
				time.Sleep(retryInterval)
				continue
			}

			// 如果没有消息，短暂休眠
			if len(messages) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// 将消息提交到工作池处理
			for _, msg := range messages {
				msg := msg // 避免闭包问题
				task := &Task{
					Message: msg,
					Handler: sub.handler,
					Topic:   sub.topic,
				}
				c.workerPool.Submit(task)
			}
		}
	}
}

// batchPopMessages 批量弹出消息
func (c *Consumer) batchPopMessages(ctx context.Context, topic string, batchSize int) ([]*message.Message, error) {
	queueKey := c.keys.QueueKey(topic)
	var messages []*message.Message

	// 使用阻塞弹出获取第一条消息
	result, err := c.client.BRPop(ctx, 0, queueKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return messages, nil
		}
		return nil, fmt.Errorf("failed to pop message from queue %s: %w", queueKey, err)
	}

	// 处理第一条消息
	if len(result) == 2 {
		rawMessage := result[1]

		var msg *message.Message
		if c.messagePool != nil {
			msg = c.messagePool.Get()
		} else {
			msg = &message.Message{Headers: make(map[string]string)}
		}

		if err = c.serializer.Deserialize([]byte(rawMessage), msg); err != nil {
			c.logger.Error("failed to deserialize message", zap.Error(err))
			// 归还对象到池
			if c.messagePool != nil {
				c.messagePool.Put(msg)
			}
			return nil, fmt.Errorf("deserialize message failed: %w", err)
		}

		messages = append(messages, msg)
	}

	// 继续获取剩余消息直到达到批处理大小
	for len(messages) < batchSize {
		res, err := c.client.RPop(ctx, queueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// 没有更多消息，退出循环
				break
			}
			c.logger.Error("failed to pop additional message", zap.Error(err))
			break
		}

		var msg *message.Message
		if c.messagePool != nil {
			msg = c.messagePool.Get()
		} else {
			msg = &message.Message{Headers: make(map[string]string)}
		}
		if err = c.serializer.Deserialize([]byte(res), msg); err != nil {
			c.logger.Error("failed to deserialize message", zap.Error(err))
			// 归还对象到池
			if c.messagePool != nil {
				c.messagePool.Put(msg)
			}
			return nil, fmt.Errorf("deserialize message failed: %w", err)
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// Unsubscribe 取消订阅
func (c *Consumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sub, exists := c.subscribers[topic]; exists {
		sub.cancel()
		<-sub.done // 等待消费协程结束
		delete(c.subscribers, topic)
		c.logger.Info("unsubscribed", zap.String("topic", topic))
	}
	return nil
}

// unsubscribe 内部取消订阅方法（不加锁）
func (c *Consumer) unsubscribe(topic string) {
	if sub, exists := c.subscribers[topic]; exists {
		sub.cancel()
		<-sub.done // 等待消费协程结束
		delete(c.subscribers, topic)
		c.logger.Info("unsubscribed", zap.String("topic", topic))
	}
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// 停止所有订阅
	for topic := range c.subscribers {
		c.unsubscribe(topic)
	}

	// 停止工作池
	if c.workerPool != nil {
		c.workerPool.Stop()
	}

	c.logger.Info("redis consumer closed")
	return nil
}

// GetWorkerPoolStats 获取工作池统计信息
func (c *Consumer) GetWorkerPoolStats() (queueSize, queueCapacity int) {
	if c.workerPool == nil {
		return 0, 0
	}
	return c.workerPool.Stats()
}
