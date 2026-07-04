package contract

import (
	"context"
	"time"

	"github.com/goairix/mq/message"
)

// Producer 生产者接口
type Producer interface {
	// Send 发送消息
	Send(ctx context.Context, msg *message.Message) error
	// SendDelay 发送延时消息
	SendDelay(ctx context.Context, msg *message.Message, delay time.Duration) error
	// SendBatch 批量发送消息
	SendBatch(ctx context.Context, msgs []*message.Message) error
	// Close 关闭生产者
	Close() error
}

// Consumer 消费者接口
type Consumer interface {
	// Subscribe 订阅消息
	Subscribe(ctx context.Context, topic string, handler message.Handler) error
	// Unsubscribe 取消订阅
	Unsubscribe(topic string) error
	// Close 关闭消费者
	Close() error
}

// DelayQueue 延时队列接口
type DelayQueue interface {
	// Push 推送延时消息
	Push(ctx context.Context, msg *message.Message, delay time.Duration) error
	// Pop 弹出到期消息
	Pop(ctx context.Context) (*message.Message, error)
	// Remove 移除消息
	Remove(ctx context.Context, msgID string) error
	// Size 获取队列大小
	Size(ctx context.Context) (int64, error)
}

// MQ 消息队列统一接口
type MQ interface {
	Producer() Producer
	Consumer() Consumer
	DelayQueue() DelayQueue
	HealthCheck() error
	Close() error
}
