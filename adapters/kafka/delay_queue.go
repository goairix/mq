package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// DelayQueue Kafka延时队列实现
// 基于时间轮算法和专用延时Topic实现
type DelayQueue struct {
	producer   *Producer
	consumer   *Consumer
	meter      metric.Meter
	logger     *zap.Logger
	delayTopic string
	timeWheel  *TimeWheel
	keyPrefix  string
}

// TimeWheel 时间轮结构
type TimeWheel struct {
	slots    []map[string]*message.Message
	size     int
	tickTime time.Duration
	current  int
	ticker   *time.Ticker
	handler  func(*message.Message)
	stopCh   chan bool
}

// NewKafkaDelayQueue 创建Kafka延时队列
func NewKafkaDelayQueue(brokers []string, observer observability.Observer, keyPrefix string) *DelayQueue {
	producer := NewKafkaProducer(brokers, observer, keyPrefix)
	consumer := NewKafkaConsumer(brokers, observer, keyPrefix)

	dq := &DelayQueue{
		producer:   producer,
		consumer:   consumer,
		meter:      observer.GetMeter(),
		logger:     observer.GetLogger(),
		delayTopic: fmt.Sprintf("%s:__delay_queue__", keyPrefix), // 为延时topic添加前缀
		keyPrefix:  keyPrefix,
	}

	// 初始化时间轮
	dq.timeWheel = NewTimeWheel(3600, time.Second, dq.handleExpiredMessage)
	dq.timeWheel.Start()

	return dq
}

// NewTimeWheel 创建时间轮
func NewTimeWheel(size int, tick time.Duration, handler func(*message.Message)) *TimeWheel {
	slots := make([]map[string]*message.Message, size)
	for i := range slots {
		slots[i] = make(map[string]*message.Message)
	}

	return &TimeWheel{
		slots:    slots,
		size:     size,
		tickTime: tick,
		current:  0,
		handler:  handler,
		stopCh:   make(chan bool),
	}
}

// Start 启动时间轮
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.tickTime)
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.tick()
			case <-tw.stopCh:
				tw.ticker.Stop()
				return
			}
		}
	}()
}

// tick 时间轮转动
func (tw *TimeWheel) tick() {
	slot := tw.slots[tw.current]
	for id, msg := range slot {
		tw.handler(msg)
		delete(slot, id)
	}
	tw.current = (tw.current + 1) % tw.size
}

// AddMessage 添加延时消息
func (tw *TimeWheel) AddMessage(msg *message.Message, delay time.Duration) {
	slots := int(delay/tw.tickTime) % tw.size
	index := (tw.current + slots) % tw.size
	tw.slots[index][msg.ID] = msg
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	close(tw.stopCh)
}

// Push 推送延时消息
func (dq *DelayQueue) Push(ctx context.Context, msg *message.Message, delay time.Duration) error {
	// 将消息添加到时间轮
	dq.timeWheel.AddMessage(msg, delay)

	if counter, err := dq.meter.Int64Counter("delay_queue_push_total"); err == nil {
		counter.Add(ctx, 1)
	}
	dq.logger.Info("delay message pushed", zap.String("id", msg.ID), zap.Duration("delay", delay))
	return nil
}

// handleExpiredMessage 处理到期消息
func (dq *DelayQueue) handleExpiredMessage(msg *message.Message) {
	// 将到期消息发送到原始Topic
	ctx := context.Background()
	err := dq.producer.Send(ctx, msg)
	if err != nil {
		dq.logger.Error("send expired message failed", zap.Error(err), zap.String("id", msg.ID))
		if counter, cerr := dq.meter.Int64Counter("delay_queue_send_errors"); cerr == nil {
			counter.Add(ctx, 1)
		}
	} else {
		if counter, cerr := dq.meter.Int64Counter("delay_queue_pop_total"); cerr == nil {
			counter.Add(ctx, 1)
		}
		dq.logger.Info("expired message sent", zap.String("id", msg.ID), zap.String("topic", msg.Topic))
	}
}

// Pop 弹出到期消息 (由时间轮自动处理)
func (dq *DelayQueue) Pop(ctx context.Context) (*message.Message, error) {
	// Kafka延时队列通过时间轮自动处理到期消息
	return nil, fmt.Errorf("kafka delay queue uses automatic time wheel processing")
}

// Remove 移除消息
func (dq *DelayQueue) Remove(ctx context.Context, msgID string) error {
	// 从时间轮中移除消息
	for _, slot := range dq.timeWheel.slots {
		if _, exists := slot[msgID]; exists {
			delete(slot, msgID)
			return nil
		}
	}
	return fmt.Errorf("message not found: %s", msgID)
}

// Size 获取队列大小
func (dq *DelayQueue) Size(ctx context.Context) (int64, error) {
	var count int64
	for _, slot := range dq.timeWheel.slots {
		count += int64(len(slot))
	}
	return count, nil
}
