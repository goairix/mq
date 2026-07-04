package main

import (
	"context"
	"log"
	"time"

	"github.com/goairix/mq"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
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

	// 5. 创建配置
	cfg := config.Config{
		Adapter:   config.AdapterRedis,
		KeyPrefix: "app:mq",
		Redis:     config.DefaultRedisConfig(),
	}

	// 6. 创建MQ工厂
	factory := mq.NewFactory(cfg, mq.WithObserver(observer))

	// 7. 创建Redis MQ实例
	mqInstance, err := factory.CreateMQ()
	if err != nil {
		log.Fatal("Failed to create MQ instance:", err)
	}
	defer func() {
		_ = mqInstance.Close()
	}()

	// 8. 发送消息
	producer := mqInstance.Producer()
	go func() {
		err = producer.Send(ctx, message.New("test-topic", []byte("Hello, World---1!")))
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		}

		err = producer.Send(ctx, message.New("test-topic", []byte("Hello, World---2!")))

		go func() {
			time.Sleep(5 * time.Second)
			err = producer.Send(ctx, message.New("test-topic", []byte("Hello, World---3!")))
		}()

		err = producer.SendDelay(ctx, message.New("delay-test-topic", []byte("Hello, Delay Message!")), 10*time.Second)

		_ = producer.SendDelay(ctx, message.New("delay2-test-topic", []byte("Hello, Delay2 Message---1!")), 20*time.Second)

		time.Sleep(5 * time.Second)
		_ = producer.SendDelay(ctx, &message.Message{
			Topic:   "delay2-test-topic",
			Payload: []byte("Hello, Delay2 Message---2!"),
		}, 20*time.Second)
	}()

	// 9. 消费消息
	consumer := mqInstance.Consumer()
	err = consumer.Subscribe(context.Background(), "test-topic", func(ctx context.Context, msg *message.Message) error {
		log.Printf("Received message: %s", string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	err = consumer.Subscribe(context.Background(), "delay-test-topic", func(ctx context.Context, msg *message.Message) error {
		log.Printf("Received delay message: %s", string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal("Failed to delay subscribe:", err)
	}

	err = consumer.Subscribe(context.Background(), "delay2-test-topic", func(ctx context.Context, msg *message.Message) error {
		log.Printf("Received delay message: %s", string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal("Failed to delay subscribe:", err)
	}

	// 等待一段时间让消息处理完成
	time.Sleep(1 * time.Minute)

	log.Println("Example completed successfully")
}
