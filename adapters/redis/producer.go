package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/goairix/mq/serializer"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Producer Redis生产者
type Producer struct {
	client         redis.Cmdable
	metrics        *observability.MetricsRecorder
	logger         *zap.Logger
	producerConfig config.ProducerPerformanceConfig
	config         config.RedisConfig
	serializer     serializer.Serializer // 序列化器
	keys           *KeyGenerator
}

func NewRedisProducer(
	client redis.Cmdable,
	observer observability.Observer,
	config config.RedisConfig,
	recorder *observability.MetricsRecorder,
	ser serializer.Serializer,
	keys *KeyGenerator,
) *Producer {
	return &Producer{
		client:         client,
		metrics:        recorder,
		logger:         observer.GetLogger(),
		producerConfig: config.GetProducerConfig(),
		config:         config,
		serializer:     ser,
		keys:           keys,
	}
}

func (p *Producer) Send(ctx context.Context, msg *message.Message) error {
	start := time.Now()

	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(0)

	// 使用配置的序列化器
	data, err := p.serializer.Serialize(msg)
	if err != nil {
		return fmt.Errorf("serialize message failed: %w", err)
	}

	queueKey := p.keys.QueueKey(msg.Topic)
	err = p.client.LPush(ctx, queueKey, data).Err()
	if err != nil {
		return fmt.Errorf("send message failed: %w", err)
	}

	// 记录指标
	if p.metrics != nil {
		p.metrics.RecordMessageLatency(ctx, msg.Topic, time.Since(start))
		p.metrics.RecordThroughput(ctx, msg.Topic, 1)
		p.metrics.RecordMessageSent(ctx, msg.Topic)
	}

	return nil
}

// SendDelay 发送延时消息
func (p *Producer) SendDelay(ctx context.Context, msg *message.Message, delay time.Duration) error {
	start := time.Now()

	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(delay)

	// 直接使用延时队列发送，而不是普通队列
	executeTime := start.Add(delay).Unix()

	data, err := p.serializer.Serialize(msg)
	if err != nil {
		p.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		return fmt.Errorf("serialize message failed: %w", err)
	}

	delayKey := p.keys.DelayQueueKey()
	msgKey := p.keys.DelayMessageKey(msg.ID)

	// 使用事务确保原子性
	pipe := p.client.TxPipeline()
	pipe.ZAdd(ctx, delayKey, &redis.Z{
		Score:  float64(executeTime),
		Member: msg.ID,
	})
	pipe.Set(ctx, msgKey, data, delay+time.Hour) // 设置过期时间

	_, err = pipe.Exec(ctx)
	if err != nil {
		p.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		p.logger.Error("send delay message failed", zap.Error(err), zap.String("topic", msg.Topic))
		return fmt.Errorf("send delay message failed: %w", err)
	}

	// 记录性能指标
	p.metrics.RecordMessageSent(ctx, msg.Topic)
	p.metrics.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
	return nil
}

// SendBatch 批量发送消息
func (p *Producer) SendBatch(ctx context.Context, messages []*message.Message) error {
	start := time.Now()
	pipe := p.client.Pipeline()

	for _, msg := range messages {
		if msg.ID == "" {
			msg.SetID(uuid.NewString())
		}
		msg.SetCreateAt(start)
		msg.SetDelay(0)

		data, err := p.serializer.Serialize(msg)
		if err != nil {
			p.metrics.RecordMessageFailed(ctx, msg.Topic, err)
			return fmt.Errorf("serialize message failed: %w", err)
		}

		queueKey := p.keys.QueueKey(msg.Topic)
		pipe.LPush(ctx, queueKey, data)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		// 记录批量发送失败
		for _, msg := range messages {
			p.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		}
		return fmt.Errorf("batch send failed: %w", err)
	}

	// 记录成功发送的消息指标
	for _, msg := range messages {
		p.metrics.RecordMessageSent(ctx, msg.Topic)
	}
	p.metrics.RecordThroughput(ctx, "batch", float64(len(messages)))
	p.metrics.RecordProcessingTime(ctx, "batch", time.Since(start))

	return nil
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return nil
}
