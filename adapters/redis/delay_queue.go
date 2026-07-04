package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"github.com/goairix/mq/serializer"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// DelayQueue Redis延时队列实现
type DelayQueue struct {
	client     redis.Cmdable
	metrics    *observability.MetricsRecorder
	logger     *zap.Logger
	keys       *KeyGenerator
	serializer serializer.Serializer
}

// NewRedisDelayQueue 创建Redis延时队列
func NewRedisDelayQueue(
	client redis.Cmdable,
	observer observability.Observer,
	recorder *observability.MetricsRecorder,
	serializer serializer.Serializer,
	keys *KeyGenerator,
) *DelayQueue {
	return &DelayQueue{
		client:     client,
		metrics:    recorder,
		logger:     observer.GetLogger(),
		keys:       keys,
		serializer: serializer,
	}
}

// Push 推送延时消息
func (dq *DelayQueue) Push(ctx context.Context, msg *message.Message, delay time.Duration) error {
	start := time.Now()
	executeTime := time.Now().Add(delay).Unix()
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	msg.CreateAt = time.Now()
	msg.Delay = delay

	data, err := dq.serializer.Serialize(msg)
	if err != nil {
		dq.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		return fmt.Errorf("serialize message failed: %w", err)
	}

	delayKey := dq.keys.DelayQueueKey()
	msgKey := dq.keys.DelayMessageKey(msg.ID)

	// 使用事务确保原子性
	pipe := dq.client.TxPipeline()
	pipe.ZAdd(ctx, delayKey, &redis.Z{
		Score:  float64(executeTime),
		Member: msg.ID,
	})
	pipe.Set(ctx, msgKey, data, delay+time.Hour) // 设置过期时间

	_, err = pipe.Exec(ctx)
	if err != nil {
		dq.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		return fmt.Errorf("push delay message failed: %w", err)
	}

	dq.metrics.RecordMessageSent(ctx, msg.Topic)
	dq.metrics.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
	dq.logger.Info("delay message pushed", zap.String("id", msg.ID), zap.Duration("delay", delay))
	return nil
}

// Pop 弹出到期消息
func (dq *DelayQueue) Pop(ctx context.Context) (*message.Message, error) {
	start := time.Now()
	now := start.Unix()
	delayKey := dq.keys.DelayQueueKey()

	// 获取到期的消息ID
	result := dq.client.ZRangeByScore(ctx, delayKey, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(now, 10),
	})

	msgIDs, err := result.Result()
	if err != nil {
		return nil, fmt.Errorf("get expired messages failed: %w", err)
	}

	if len(msgIDs) == 0 {
		return nil, nil // 没有到期消息
	}

	// 处理第一条消息（保持当前接口不变）
	msgID := msgIDs[0]
	msgKey := dq.keys.DelayMessageKey(msgID)

	// 原子性地移除消息并获取内容
	luaScript := `
		local msgKey = KEYS[1]
		local delayKey = KEYS[2]
		local msgID = ARGV[1]
		
		local data = redis.call('GET', msgKey)
		if data then
			redis.call('DEL', msgKey)
			redis.call('ZREM', delayKey, msgID)
			return data
		end
		return nil
	`

	data, err := dq.client.Eval(ctx, luaScript, []string{msgKey, delayKey}, msgID).Result()
	if err != nil {
		return nil, fmt.Errorf("pop delay message failed: %w", err)
	}

	if data == nil {
		return nil, nil
	}

	var msg message.Message
	err = dq.serializer.Deserialize([]byte(data.(string)), &msg)
	if err != nil {
		return nil, fmt.Errorf("deserialize message failed: %w", err)
	}

	dq.metrics.RecordMessageReceived(ctx, msg.Topic)
	dq.metrics.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
	return &msg, nil
}

// Remove 移除消息
func (dq *DelayQueue) Remove(ctx context.Context, msgID string) error {
	delayKey := dq.keys.DelayQueueKey()
	msgKey := dq.keys.DelayMessageKey(msgID)

	pipe := dq.client.TxPipeline()
	pipe.ZRem(ctx, delayKey, msgID)
	pipe.Del(ctx, msgKey)

	_, err := pipe.Exec(ctx)
	return err
}

// Size 获取队列大小
func (dq *DelayQueue) Size(ctx context.Context) (int64, error) {
	delayKey := dq.keys.DelayQueueKey()
	return dq.client.ZCard(ctx, delayKey).Result()
}
