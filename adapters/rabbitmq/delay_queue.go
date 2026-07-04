package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/goairix/mq/serializer"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// DelayQueue RabbitMQ延时队列实现 - 使用x-delayed-message插件
type DelayQueue struct {
	pool       *ConnectionPool
	meter      metric.Meter
	logger     *zap.Logger
	config     config.RabbitMQConfig
	serializer serializer.Serializer
	recorder   *observability.MetricsRecorder
	keyGen     *KeyGenerator
}

// NewRabbitDelayQueue 创建RabbitMQ延时队列
func NewRabbitDelayQueue(
	pool *ConnectionPool,
	observer observability.Observer,
	recorder *observability.MetricsRecorder,
	config config.RabbitMQConfig,
	ser serializer.Serializer,
	keyGen *KeyGenerator,
) *DelayQueue {
	return &DelayQueue{
		pool:       pool,
		meter:      observer.GetMeter(),
		logger:     observer.GetLogger(),
		config:     config,
		serializer: ser,
		recorder:   recorder,
		keyGen:     keyGen,
	}
}

// Push 推送延时消息
func (dq *DelayQueue) Push(ctx context.Context, msg *message.Message, delay time.Duration) error {
	start := time.Now()

	// 获取通道
	ch, err := dq.pool.GetChannel(ctx)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "get_channel_error")
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer dq.pool.ReturnChannel(ch)

	// 设置延时交换机
	delayExchange := dq.keyGen.DelayExchangeName()
	err = ch.ExchangeDeclare(
		delayExchange,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		map[string]interface{}{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "declare_exchange_error")
		return fmt.Errorf("failed to declare delay exchange: %w", err)
	}

	// 声明延时队列
	delayQueueName := dq.keyGen.DelayQueueName(msg.Topic)
	_, err = ch.QueueDeclare(
		delayQueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "declare_queue_error")
		return fmt.Errorf("failed to declare delay queue: %w", err)
	}

	// 绑定队列到延时交换机
	err = ch.QueueBind(
		delayQueueName,
		msg.Topic, // routing key
		delayExchange,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "bind_queue_error")
		return fmt.Errorf("failed to bind delay queue: %w", err)
	}

	// 序列化消息
	body, err := dq.serializer.Serialize(msg)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "serialize_error")
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// 构建AMQP头部
	headers := make(map[string]interface{})
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers["x-delay"] = int64(delay / time.Millisecond)

	// 发布延时消息
	err = ch.PublishWithContext(
		ctx,
		delayExchange,
		msg.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType:  dq.serializer.ContentType(),
			Body:         body,
			Headers:      headers,
			DeliveryMode: amqp.Persistent,
			MessageId:    msg.ID,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "publish_error")
		return fmt.Errorf("failed to publish delay message: %w", err)
	}

	// 记录成功指标
	dq.recorder.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
	dq.recorder.RecordMessageSent(ctx, msg.Topic)
	dq.logger.Debug("delay message pushed",
		zap.String("topic", msg.Topic),
		zap.String("message_id", msg.ID),
		zap.Duration("delay", delay))

	return nil
}

// Pop 弹出到期消息 (RabbitMQ通过x-delayed-message插件自动处理)
func (dq *DelayQueue) Pop(ctx context.Context) (*message.Message, error) {
	return nil, fmt.Errorf("rabbitmq delay queue uses automatic routing, use consumer to receive messages")
}

// Remove 移除消息
func (dq *DelayQueue) Remove(ctx context.Context, msgID string) error {
	return fmt.Errorf("rabbitmq delay queue does not support message removal after publishing")
}

// Size 获取队列大小
func (dq *DelayQueue) Size(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("rabbitmq delay queue size check requires management API")
}

// Close 关闭延时队列
func (dq *DelayQueue) Close() error {
	return nil // 连接池会统一管理连接关闭
}
