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
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Producer RabbitMQ生产者
type Producer struct {
	connectionPool *ConnectionPool
	meter          metric.Meter
	logger         *zap.Logger
	producerConfig config.ProducerPerformanceConfig
	config         config.RabbitMQConfig
	serializer     serializer.Serializer
	recorder       *observability.MetricsRecorder
	keyGen         *KeyGenerator

	// 对象池
	messagePool    *pool.MessagePool
	byteBufferPool *pool.ByteBufferPool

	// 批量发送
	batchCh    chan *message.Message
	batchMu    sync.Mutex
	batch      []*message.Message
	flushTimer *time.Timer
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewRabbitProducer 创建RabbitMQ生产者
func NewRabbitProducer(
	pool *ConnectionPool,
	observer observability.Observer,
	recorder *observability.MetricsRecorder,
	config config.RabbitMQConfig,
	ser serializer.Serializer,
	keyGen *KeyGenerator,
	messagePool *pool.MessagePool,
	byteBufferPool *pool.ByteBufferPool,
) *Producer {
	ctx, cancel := context.WithCancel(context.Background())

	producerConfig := config.GetProducerConfig()

	p := &Producer{
		connectionPool: pool,
		meter:          observer.GetMeter(),
		logger:         observer.GetLogger(),
		recorder:       recorder,
		keyGen:         keyGen,
		producerConfig: producerConfig,
		config:         config,
		serializer:     ser,
		messagePool:    messagePool,
		byteBufferPool: byteBufferPool,
		batchCh:        make(chan *message.Message, producerConfig.BatchSize*2),
		batch:          make([]*message.Message, 0, producerConfig.BatchSize),
		ctx:            ctx,
		cancel:         cancel,
	}

	// 启动批量处理
	if producerConfig.BatchSize > 1 {
		p.startBatchProcessor()
	}

	return p
}

// Send 发送消息
func (p *Producer) Send(ctx context.Context, msg *message.Message) error {
	start := time.Now()
	defer func() {
		p.recorder.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
		// 吞吐量指标
		p.recorder.RecordThroughput(ctx, msg.Topic, 1)
	}()

	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(0)

	// 记录消息延迟（从创建到发送的时间）
	if !msg.CreateAt.IsZero() {
		latency := time.Since(msg.CreateAt)
		p.recorder.RecordMessageLatency(ctx, msg.Topic, latency)
	}

	// 如果启用批量发送
	if p.producerConfig.BatchSize > 1 {
		select {
		case p.batchCh <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// 直接发送
	err := p.sendSingle(ctx, msg)
	if err != nil {
		p.recorder.RecordMessageFailed(ctx, msg.Topic, err)
		return err
	}

	// 记录发送成功
	p.recorder.RecordMessageSent(ctx, msg.Topic)
	return nil
}

// SendDelay 发送延时消息 - 使用x-delayed-message插件
func (p *Producer) SendDelay(ctx context.Context, msg *message.Message, delay time.Duration) error {
	start := time.Now()
	defer func() {
		p.recorder.RecordProcessingTime(ctx, msg.Topic, time.Since(start))
	}()

	if msg.ID == "" {
		msg.SetID(uuid.NewString())
	}
	msg.SetCreateAt(start)
	msg.SetDelay(delay)

	// 获取通道
	ch, err := p.connectionPool.GetChannel(ctx)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "get_channel_error")
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer p.connectionPool.ReturnChannel(ch)

	// 声明延时交换机（需要x-delayed-message插件）
	exchangeName := p.keyGen.DelayExchangeName()
	err = ch.ExchangeDeclare(
		exchangeName,
		"x-delayed-message",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		amqp.Table{
			"x-delayed-type": "direct",
		},
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "declare_exchange_error")
		return fmt.Errorf("failed to declare delay exchange: %w", err)
	}

	// 声明目标队列
	queueName := p.keyGen.DelayQueueName(msg.Topic)
	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "declare_queue_error")
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 绑定队列到延时交换机
	err = ch.QueueBind(
		queueName,
		queueName, // routing key
		exchangeName,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "bind_queue_error")
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// 序列化消息
	body, err := p.serializer.Serialize(msg)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "serialize_error")
		return fmt.Errorf("serialize message failed: %w", err)
	}

	// 构建AMQP消息头
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	// 设置延时时间（毫秒）
	headers["x-delay"] = int64(delay / time.Millisecond)

	// 发布延时消息
	err = ch.Publish(
		exchangeName, // exchange
		queueName,    // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: p.serializer.ContentType(),
			Body:        body,
			Headers:     headers,
		},
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "publish_error")
		return fmt.Errorf("failed to publish delay message: %w", err)
	}

	// 记录发送成功
	p.recorder.RecordMessageSent(ctx, msg.Topic)
	p.recorder.LogInfo("delay message sent",
		zap.String("topic", msg.Topic),
		zap.String("message_id", msg.ID),
		zap.Duration("delay", delay),
	)

	return nil
}

// sendSingle 发送单条消息
func (p *Producer) sendSingle(ctx context.Context, msg *message.Message) error {
	// 获取通道
	ch, err := p.connectionPool.GetChannel(ctx)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "get_channel_error")
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer p.connectionPool.ReturnChannel(ch)

	// 使用字节缓冲池进行序列化（如果启用）
	var body []byte
	if p.byteBufferPool != nil {
		buf := p.byteBufferPool.Get()
		defer p.byteBufferPool.Put(buf)

		// 序列化到缓冲区
		body, err = p.serializer.Serialize(msg)
		if err != nil {
			p.recorder.RecordProcessingError(ctx, msg.Topic, "serialize_error")
			return fmt.Errorf("serialize message failed: %w", err)
		}
	} else {
		body, err = p.serializer.Serialize(msg)
		if err != nil {
			p.recorder.RecordProcessingError(ctx, msg.Topic, "serialize_error")
			return fmt.Errorf("serialize message failed: %w", err)
		}
	}

	exchangeName := p.keyGen.ExchangeName()
	queueName := p.keyGen.QueueName(msg.Topic)

	// 获取RabbitMQ配置
	rabbitmqConfig := p.connectionPool.config

	// 声明交换机
	err = ch.ExchangeDeclare(
		exchangeName,
		rabbitmqConfig.ExchangeType,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "declare_exchange_error")
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// 声明队列
	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "declare_queue_error")
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 记录队列大小
	p.recorder.RecordQueueSize(ctx, msg.Topic, int64(queue.Messages))

	// 绑定队列到交换机
	err = ch.QueueBind(
		queueName,
		msg.Topic, // routing key
		exchangeName,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "bind_queue_error")
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	// 构建AMQP消息头
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	// 通过交换机发布消息
	err = ch.Publish(
		exchangeName,
		msg.Topic, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  p.serializer.ContentType(),
			Body:         body,
			MessageId:    msg.ID,
			Timestamp:    msg.CreateAt,
			Headers:      headers,
			DeliveryMode: amqp.Persistent, // 持久化消息
		},
	)

	if err != nil {
		p.recorder.RecordProcessingError(ctx, msg.Topic, "publish_error")
		return fmt.Errorf("publish message failed: %w", err)
	}

	// 记录成功发送的消息
	if counter, err := p.meter.Int64Counter("producer_messages_sent"); err == nil {
		counter.Add(ctx, 1)
	}

	p.recorder.RecordMessageSent(ctx, msg.Topic)
	p.logger.Info("message sent",
		zap.String("topic", msg.Topic),
		zap.String("message_id", msg.ID),
		zap.String("exchange", exchangeName),
	)

	return nil
}

// startBatchProcessor 启动批量处理器
func (p *Producer) startBatchProcessor() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.flushTimer = time.NewTimer(p.producerConfig.FlushInterval)
		defer p.flushTimer.Stop()

		for {
			select {
			case <-p.ctx.Done():
				p.flushBatch()
				return
			case msg := <-p.batchCh:
				p.addToBatch(msg)
			case <-p.flushTimer.C:
				p.flushBatch()
				p.flushTimer.Reset(p.producerConfig.FlushInterval)
			}
		}
	}()
}

// addToBatch 添加到批次
func (p *Producer) addToBatch(msg *message.Message) {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	p.batch = append(p.batch, msg)
	if len(p.batch) >= p.producerConfig.BatchSize {
		p.flushBatch()
		p.flushTimer.Reset(p.producerConfig.FlushInterval)
	}
}

// flushBatch 刷新批次
func (p *Producer) flushBatch() {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	if len(p.batch) == 0 {
		return
	}

	// 批量发送
	for _, msg := range p.batch {
		if err := p.sendSingle(context.Background(), msg); err != nil {
			p.logger.Error("failed to send batch message", zap.Error(err), zap.String("message_id", msg.ID))
		}
	}

	// 清空批次
	p.batch = p.batch[:0]
}

// Close 关闭生产者
func (p *Producer) Close() error {
	p.cancel()
	p.wg.Wait()
	return nil
}

// SendBatch 批量发送消息
func (p *Producer) SendBatch(ctx context.Context, messages []*message.Message) error {
	start := time.Now()
	defer func() {
		p.recorder.RecordProcessingTime(ctx, "batch", time.Since(start))
		// 记录批量吞吐量
		p.recorder.RecordThroughput(ctx, "batch", float64(len(messages)))
	}()

	if len(messages) == 0 {
		return nil
	}

	// 按topic分组
	topicGroups := make(map[string][]*message.Message)
	for _, msg := range messages {
		if msg.ID == "" {
			msg.SetID(uuid.NewString())
		}
		msg.SetCreateAt(start)
		msg.SetDelay(0)
		topicGroups[msg.Topic] = append(topicGroups[msg.Topic], msg)
	}

	// 获取通道
	ch, err := p.connectionPool.GetChannel(ctx)
	if err != nil {
		p.recorder.RecordProcessingError(ctx, "batch", "get_channel_error")
		return fmt.Errorf("failed to get channel: %w", err)
	}
	defer p.connectionPool.ReturnChannel(ch)

	// 按topic批量发送
	var lastErr error
	successCount := 0
	failedCount := 0

	for topic, msgs := range topicGroups {
		for _, msg := range msgs {
			err := p.sendSingle(ctx, msg)
			if err != nil {
				p.recorder.RecordMessageFailed(ctx, topic, err)
				lastErr = err
				failedCount++
				continue
			}
			successCount++
		}

		// 计算并记录错误率
		totalCount := len(msgs)
		if totalCount > 0 {
			errorRate := float64(failedCount) / float64(totalCount)
			p.recorder.RecordErrorRate(ctx, topic, errorRate)
		}
	}

	p.recorder.LogInfo("batch send completed",
		zap.Int("total", len(messages)),
		zap.Int("success", successCount),
		zap.Int("failed", failedCount),
	)

	return lastErr
}
