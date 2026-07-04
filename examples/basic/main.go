package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/goairix/mq"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
)

func main() {
	// 创建配置
	cfg := config.Config{
		Adapter:   config.AdapterRedis,
		KeyPrefix: "app:mq",
		Redis:     config.DefaultRedisConfig(),
	}

	// 创建消息队列实例
	factory := mq.NewFactory(cfg)
	mqInstance, err := factory.CreateMQ()
	if err != nil {
		log.Fatal("Failed to create MQ:", err)
	}
	defer func() {
		_ = mqInstance.Close()
	}()

	// 生产者示例
	producer := mqInstance.Producer()
	msg := message.New("test-topic", []byte("hello world"))
	msg.SetHeaders(map[string]string{"source": "example"})

	ctx := context.Background()
	err = producer.Send(ctx, msg)
	if err != nil {
		log.Fatal("Failed to send message:", err)
	}
	fmt.Println("Message sent successfully")

	// 延时消息示例
	delayMsg := message.New("delay-topic", []byte("Delayed message"))
	err = mqInstance.DelayQueue().Push(ctx, delayMsg, 10*time.Second)
	if err != nil {
		log.Fatal("Failed to send delay message:", err)
	}
	fmt.Println("Delay message sent successfully")

	// 消费者示例
	consumer := mqInstance.Consumer()
	err = consumer.Subscribe(ctx, "test-topic", func(ctx context.Context, msg *message.Message) error {
		fmt.Printf("[%s] Received message: %s\n", time.Now().Format(time.DateTime), string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}
	_ = consumer.Subscribe(ctx, "delay-topic", func(ctx context.Context, msg *message.Message) error {
		fmt.Printf("[%s] Received message: %s\n", time.Now().Format(time.DateTime), string(msg.Payload))
		return nil
	})

	// 等待消息处理
	time.Sleep(30 * time.Second)
}
