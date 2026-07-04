package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/goairix/mq/pool"
	"github.com/goairix/mq/serializer"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Consumer RabbitMQ消费者
type Consumer struct {
	pool        *ConnectionPool
	meter       metric.Meter
	logger      *zap.Logger
	subscribers map[string]*subscription
	mu          sync.RWMutex
	config      config.RabbitMQConfig
	serializer  serializer.Serializer
	recorder    *observability.MetricsRecorder
	keyGen      *KeyGenerator

	// 对象池
	messagePool    *pool.MessagePool
	byteBufferPool *pool.ByteBufferPool
	poolEnabled    bool
}

// subscription 订阅信息
type subscription struct {
	cancel  context.CancelFunc
	ch      *amqp.Channel
	topic   string
	handler message.Handler
	ctx     context.Context
}

// NewRabbitConsumer 创建RabbitMQ消费者
func NewRabbitConsumer(
	pool *ConnectionPool,
	observer observability.Observer,
	recorder *observability.MetricsRecorder,
	config config.RabbitMQConfig,
	ser serializer.Serializer,
	keyGen *KeyGenerator,
	messagePool *pool.MessagePool,
	byteBufferPool *pool.ByteBufferPool,
) *Consumer {
	return &Consumer{
		pool:           pool,
		meter:          observer.GetMeter(),
		logger:         observer.GetLogger(),
		subscribers:    make(map[string]*subscription),
		config:         config,
		serializer:     ser,
		recorder:       recorder,
		keyGen:         keyGen,
		messagePool:    messagePool,
		byteBufferPool: byteBufferPool,
		poolEnabled:    config.GetObjectPoolConfig().Enabled,
	}
}

// Subscribe 订阅消息
func (c *Consumer) Subscribe(ctx context.Context, topic string, handler message.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否已订阅
	if _, exists := c.subscribers[topic]; exists {
		return fmt.Errorf("topic %s already subscribed", topic)
	}

	// 获取通道
	ch, err := c.pool.GetChannel(ctx)
	if err != nil {
		return fmt.Errorf("failed to get channel: %w", err)
	}

	// 设置通道配置
	if err := c.setupChannel(ch, topic); err != nil {
		c.pool.ReturnChannel(ch)
		return fmt.Errorf("failed to setup channel: %w", err)
	}

	// 创建订阅上下文
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		cancel:  cancel,
		ch:      ch,
		topic:   topic,
		handler: handler,
		ctx:     subCtx,
	}

	c.subscribers[topic] = sub

	// 启动消费协程
	go c.consume(sub)

	// 启动队列积压监控
	go c.monitorQueueBacklog(subCtx, topic, c.keyGen.QueueName(topic))

	c.logger.Info("consumer subscribed", zap.String("topic", topic))
	return nil
}

// setupChannel 设置通道配置
func (c *Consumer) setupChannel(ch *amqp.Channel, topic string) error {
	queueName := c.keyGen.QueueName(topic)
	exchangeName := c.keyGen.ExchangeName()

	// 声明交换机
	err := ch.ExchangeDeclare(
		exchangeName,
		c.config.ExchangeType,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// 声明队列
	_, err = ch.QueueDeclare(
		queueName,
		c.config.QueueDurable,
		c.config.QueueAutoDelete,
		c.config.QueueExclusive,
		c.config.QueueNoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 绑定队列到交换机
	err = ch.QueueBind(
		queueName,
		topic,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// 设置QoS
	err = ch.Qos(
		c.config.QoS,
		0,
		false,
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	return nil
}

// consume 消费消息
func (c *Consumer) consume(sub *subscription) {
	defer func() {
		c.pool.ReturnChannel(sub.ch)
		c.logger.Info("consumer stopped", zap.String("topic", sub.topic))
	}()

	queueName := c.keyGen.QueueName(sub.topic)

	// 开始消费
	msgs, err := sub.ch.Consume(
		queueName,
		"",
		false, // auto-ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		c.logger.Error("failed to start consuming", zap.Error(err), zap.String("topic", sub.topic))
		return
	}

	// 处理消息
	for {
		select {
		case <-sub.ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				c.logger.Warn("message channel closed", zap.String("topic", sub.topic))
				return
			}
			c.handleMessage(sub.ctx, d, sub.handler, sub.topic)
		}
	}
}

// 队列积压监控方法
func (c *Consumer) monitorQueueBacklog(ctx context.Context, topic, queueName string) {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ch, err := c.pool.GetChannel(ctx)
			if err != nil {
				c.recorder.LogWarn("failed to get channel for queue monitoring",
					zap.Error(err), zap.String("topic", topic))
				continue
			}

			queue, err := ch.QueueDeclare(
				queueName,
				c.config.QueueDurable,    // durable
				c.config.QueueAutoDelete, // auto-delete
				c.config.QueueExclusive,  // exclusive
				c.config.QueueNoWait,     // no-wait
				nil,                      // arguments
			)
			if err != nil {
				c.recorder.LogWarn("failed to declare queue for monitoring",
					zap.Error(err),
					zap.String("topic", topic),
					zap.String("queue", queueName))
				c.pool.ReturnChannel(ch)
				continue
			}

			// 记录队列积压
			c.recorder.RecordQueueBacklog(ctx, topic, int64(queue.Messages))
			c.recorder.RecordQueueSize(ctx, topic, int64(queue.Messages))

			// 如果队列积压过多，记录警告
			if queue.Messages > 1000 {
				c.recorder.LogWarn("high queue backlog detected",
					zap.String("topic", topic),
					zap.String("queue", queueName),
					zap.Int("messages", queue.Messages))
			}

			c.pool.ReturnChannel(ch)
		}
	}
}

// handleMessage 处理消息
func (c *Consumer) handleMessage(ctx context.Context, delivery amqp.Delivery, handler message.Handler, topic string) {
	start := time.Now()
	defer func() {
		c.recorder.RecordProcessingTime(ctx, topic, time.Since(start))
		c.recorder.RecordThroughput(ctx, topic, 1)
	}()

	// 记录消息接收
	c.recorder.RecordMessageReceived(ctx, topic)

	// 从对象池获取消息对象
	var msg *message.Message
	if c.poolEnabled && c.messagePool != nil {
		msg = c.messagePool.Get()
		defer c.messagePool.Put(msg)
	} else {
		msg = &message.Message{}
	}

	// 使用字节缓冲池进行反序列化
	var deserializeErr error
	if c.poolEnabled && c.byteBufferPool != nil {
		buf := c.byteBufferPool.Get()
		defer c.byteBufferPool.Put(buf)

		// 复制数据到缓冲区
		buf = append(buf, delivery.Body...)

		// 反序列化
		deserializeErr = c.serializer.Deserialize(buf, msg)
	} else {
		// 直接反序列化
		deserializeErr = c.serializer.Deserialize(delivery.Body, msg)
	}

	if deserializeErr != nil {
		c.logger.Error("failed to deserialize message", zap.Error(deserializeErr), zap.String("topic", topic))
		c.recorder.RecordProcessingError(ctx, topic, "deserialize_error")
		_ = delivery.Nack(false, false) // 拒绝消息，不重新入队
		return
	}

	// 计算消息延迟（从创建到处理的时间）
	if !msg.CreateAt.IsZero() {
		latency := time.Since(msg.CreateAt)
		c.recorder.RecordMessageLatency(ctx, topic, latency)
	}

	// 转换头部信息
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	for k, v := range delivery.Headers {
		if str, ok := v.(string); ok {
			msg.Headers[k] = str
		}
	}

	err := handler(ctx, msg)
	if err != nil {
		c.recorder.RecordMessageFailed(ctx, topic, err)
		c.logger.Error("message processing failed",
			zap.String("topic", topic),
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)
	}

	// 确认消息
	err = delivery.Ack(false)
	if err != nil {
		c.logger.Error("failed to ack message", zap.Error(err), zap.String("topic", topic))
		return
	}
}

// Unsubscribe 取消订阅
func (c *Consumer) Unsubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub, exists := c.subscribers[topic]
	if !exists {
		return fmt.Errorf("topic %s not subscribed", topic)
	}

	// 取消订阅
	sub.cancel()
	delete(c.subscribers, topic)

	c.logger.Info("unsubscribed", zap.String("topic", topic))
	return nil
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭所有订阅
	for topic, sub := range c.subscribers {
		sub.cancel()
		delete(c.subscribers, topic)
		c.logger.Info("closed subscription", zap.String("topic", topic))
	}

	return nil
}
