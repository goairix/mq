package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/goairix/mq/serializer"

	"github.com/go-redis/redis/v8"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"go.uber.org/zap"
)

// DelayProcessor 延时消息处理器
type DelayProcessor struct {
	client     Client
	metrics    *observability.MetricsRecorder
	logger     *zap.Logger
	keys       *KeyGenerator
	serializer serializer.Serializer

	// 性能优化配置
	batchSize    int           // 批处理大小
	pollInterval time.Duration // 轮询间隔
	maxBackoff   time.Duration // 最大退避时间
}

// NewDelayProcessor 创建延时消息处理器
func NewDelayProcessor(
	client Client,
	observer observability.Observer,
	recorder *observability.MetricsRecorder,
	serializer serializer.Serializer,
	keys *KeyGenerator,
) *DelayProcessor {
	return &DelayProcessor{
		client:       client,
		metrics:      recorder,
		logger:       observer.GetLogger(),
		keys:         keys,
		serializer:   serializer,
		batchSize:    100,
		pollInterval: time.Second,
		maxBackoff:   30 * time.Second,
	}
}

// Start 启动延时消息处理
func (dp *DelayProcessor) Start(ctx context.Context) {
	backoff := dp.pollInterval

	for {
		select {
		case <-ctx.Done():
			dp.logger.Info("delay processor stopped")
			return
		case <-time.After(backoff):
			processed, err := dp.processExpiredMessages(ctx)
			if err != nil {
				dp.logger.Error("process expired messages failed", zap.Error(err))
				backoff = dp.calculateBackoff(backoff, true)
			} else if processed == 0 {
				// 没有消息时增加退避时间
				backoff = dp.calculateBackoff(backoff, false)
			} else {
				// 有消息时重置退避时间
				backoff = dp.pollInterval
				dp.logger.Debug("processed expired messages", zap.Int("count", processed))
			}
		}
	}
}

// processExpiredMessages 处理到期消息
func (dp *DelayProcessor) processExpiredMessages(ctx context.Context) (int, error) {
	now := time.Now().Unix()
	delayKey := dp.keys.DelayQueueKey()

	// 批量获取到期消息
	result := dp.client.ZRangeByScore(ctx, delayKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   strconv.FormatInt(now, 10),
		Count: int64(dp.batchSize),
	})

	msgIDs, err := result.Result()
	if err != nil {
		return 0, fmt.Errorf("get expired messages failed: %w", err)
	}

	if len(msgIDs) == 0 {
		return 0, nil
	}

	// 批量处理消息
	processed := 0
	for _, msgID := range msgIDs {
		if err := dp.processMessage(ctx, msgID); err != nil {
			dp.logger.Error("process message failed",
				zap.String("msg_id", msgID),
				zap.Error(err))
			continue
		}
		processed++
	}

	return processed, nil
}

// processMessage 处理单个消息
func (dp *DelayProcessor) processMessage(ctx context.Context, msgID string) error {
	start := time.Now()
	delayKey := dp.keys.DelayQueueKey()
	msgKey := dp.keys.DelayMessageKey(msgID)

	// 使用Lua脚本确保原子性
	luaScript := `
		local msgKey = KEYS[1]
		local delayKey = KEYS[2]
		local msgID = ARGV[1]
		
		-- 获取消息内容
		local msgData = redis.call('GET', msgKey)
		if not msgData then
			return nil
		end
		
		-- 从延时队列中移除
		redis.call('ZREM', delayKey, msgID)
		-- 删除消息数据
		redis.call('DEL', msgKey)
		
		return msgData
	`

	result := dp.client.Eval(ctx, luaScript, []string{msgKey, delayKey}, msgID)
	msgData, err := result.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil // 消息已被处理
		}
		return fmt.Errorf("eval lua script failed: %w", err)
	}

	if msgData == nil {
		return nil // 消息已被处理
	}

	// 解析消息
	var msg message.Message
	if err = dp.serializer.Deserialize([]byte(msgData.(string)), &msg); err != nil {
		return fmt.Errorf("deserialize message failed: %w", err)
	}

	// 将消息发送到普通队列
	queueKey := dp.keys.QueueKey(msg.Topic)
	data, err := dp.serializer.Serialize(&msg)
	if err != nil {
		dp.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		return fmt.Errorf("serialize message failed: %w", err)
	}

	if err = dp.client.LPush(ctx, queueKey, data).Err(); err != nil {
		dp.metrics.RecordMessageFailed(ctx, msg.Topic, err)
		return fmt.Errorf("push message failed: %w", err)
	}

	dp.metrics.RecordMessageSent(ctx, msg.Topic)
	dp.metrics.RecordProcessingTime(ctx, msg.Topic, time.Since(start))

	return nil
}

// calculateBackoff 计算退避时间
func (dp *DelayProcessor) calculateBackoff(current time.Duration, hasError bool) time.Duration {
	if hasError {
		// 有错误时使用指数退避
		next := current * 2
		if next > dp.maxBackoff {
			return dp.maxBackoff
		}
		return next
	}

	// 无消息时线性增加
	next := current + dp.pollInterval
	if next > dp.maxBackoff {
		return dp.maxBackoff
	}
	return next
}
