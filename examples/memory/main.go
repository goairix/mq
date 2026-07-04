package main

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/goairix/mq"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/contract"
	"github.com/goairix/mq/message"
)

// MyObserver 实现Observer接口
type MyObserver struct {
	meter  metric.Meter
	logger *zap.Logger
}

func (o *MyObserver) GetMeter() metric.Meter {
	return o.meter
}

func (o *MyObserver) GetLogger() *zap.Logger {
	return o.logger
}

func main() {
	// 创建内存消息队列配置
	cfg := config.Config{
		Adapter: config.AdapterMemory,
		Memory: config.MemoryConfig{
			MaxQueueSize:       1000,
			MaxDelayQueueSize:  100,
			DelayCheckInterval: 100 * time.Millisecond,
			EnableMetrics:      true,
		},
	}

	ctx := context.Background()

	// 2. 初始化zap日志
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal("Failed to initialize logger:", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	// 3. 获取Meter
	meter := otel.Meter("mq-example")

	// 4. 创建Observer实例
	observer := &MyObserver{
		meter:  meter,
		logger: logger,
	}

	// 创建MQ实例
	factory := mq.NewFactory(cfg, mq.WithObserver(observer))
	mqInstance, err := factory.CreateMQ()
	if err != nil {
		log.Fatal("创建MQ实例失败:", err)
	}
	defer func() {
		_ = mqInstance.Close()
	}()

	// 生产者示例
	producer := mqInstance.Producer()
	msg := message.New("test-topic", []byte("Hello Memory MQ!"))
	msg.SetHeaders(map[string]string{"type": "test"})

	err = producer.Send(ctx, msg)
	if err != nil {
		log.Fatal("发送消息失败:", err)
	}

	middlewareChain := contract.NewMiddlewareChain(
		contract.LoggingMiddleware(logger),
		contract.TimeoutMiddleware(30*time.Second),
		contract.RetryMiddleware(3, time.Second),
	)

	handler := middlewareChain.Apply(func(ctx context.Context, msg *message.Message) error {
		log.Printf("收到消息: %s", string(msg.Payload))
		log.Printf("%+v", msg)
		return nil
	})

	// 消费者示例
	consumer := mqInstance.Consumer()
	err = consumer.Subscribe(ctx, "test-topic", handler)
	if err != nil {
		log.Fatal("订阅失败:", err)
	}

	// 延时消息示例
	delayMsg := message.New("test-topic", []byte("Delayed message!"))
	err = producer.SendDelay(ctx, delayMsg, 5*time.Second)
	if err != nil {
		log.Fatal("发送延时消息失败:", err)
	}

	// 等待消息处理
	time.Sleep(10 * time.Second)
}
