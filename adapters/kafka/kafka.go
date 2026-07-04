package kafka

import (
	"fmt"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/contract"
	"github.com/goairix/mq/observability"
	"github.com/segmentio/kafka-go"
)

// Kafka Kafka消息队列实现
type Kafka struct {
	brokers    []string
	producer   *Producer
	consumer   *Consumer
	delayQueue *DelayQueue
	recorder   *observability.MetricsRecorder
	config     config.KafkaConfig
	keyPrefix  string
}

// NewKafkaMQ 创建Kafka消息队列
func NewKafkaMQ(config config.KafkaConfig, observer observability.Observer, keyPrefix string) (*Kafka, error) {
	if keyPrefix == "" {
		keyPrefix = "mq"
	}

	// 创建指标记录器
	recorder, err := observability.NewMetricsRecorder(observer, "kafka")
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics recorder: %w", err)
	}

	mq := &Kafka{
		brokers:   config.Brokers,
		recorder:  recorder,
		config:    config,
		keyPrefix: keyPrefix,
	}

	mq.producer = NewKafkaProducer(config.Brokers, observer, keyPrefix)
	mq.consumer = NewKafkaConsumer(config.Brokers, observer, keyPrefix)
	mq.delayQueue = NewKafkaDelayQueue(config.Brokers, observer, keyPrefix)

	return mq, nil
}

// Producer 获取生产者
func (k *Kafka) Producer() contract.Producer {
	return k.producer
}

// Consumer 获取消费者
func (k *Kafka) Consumer() contract.Consumer {
	return k.consumer
}

// DelayQueue 获取延时队列
func (k *Kafka) DelayQueue() contract.DelayQueue {
	return k.delayQueue
}

// HealthCheck 健康检查
func (k *Kafka) HealthCheck() error {
	// 创建临时连接测试
	conn, err := kafka.Dial("tcp", k.brokers[0])
	if err != nil {
		return fmt.Errorf("kafka connection failed: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	// 获取broker信息
	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("kafka broker check failed: %w", err)
	}

	return nil
}

// Close 关闭连接
func (k *Kafka) Close() error {
	var errs []error

	if err := k.producer.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := k.consumer.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}
