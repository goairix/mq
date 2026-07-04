package mq

import (
	"fmt"

	"github.com/goairix/mq/adapters/kafka"
	"github.com/goairix/mq/adapters/memory"
	"github.com/goairix/mq/adapters/rabbitmq"
	"github.com/goairix/mq/adapters/redis"
	"github.com/goairix/mq/config"
	"github.com/goairix/mq/contract"
	"github.com/goairix/mq/observability"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// Factory MQ工厂
type Factory struct {
	config   config.Config
	observer observability.Observer
}

// FactoryOption 工厂选项函数类型
type FactoryOption func(*Factory)

// WithObserver 设置Observer选项
func WithObserver(observer observability.Observer) FactoryOption {
	return func(f *Factory) {
		f.observer = observer
	}
}

// defaultObserver 默认的Observer实现
type defaultObserver struct {
	meter  metric.Meter
	logger *zap.Logger
}

// GetMeter 获取默认的Meter（noop实现）
func (d *defaultObserver) GetMeter() metric.Meter {
	return d.meter
}

// GetLogger 获取默认的Logger
func (d *defaultObserver) GetLogger() *zap.Logger {
	return d.logger
}

// newDefaultObserver 创建默认Observer
func newDefaultObserver() observability.Observer {
	// 创建一个基本的logger，如果失败则使用nop logger
	logger, err := zap.NewProduction()
	if err != nil {
		logger = zap.NewNop()
	}

	return &defaultObserver{
		meter:  noop.NewMeterProvider().Meter("default"), // 使用noop meter
		logger: logger,
	}
}

// NewFactory 创建MQ工厂
func NewFactory(cfg config.Config, options ...FactoryOption) *Factory {
	factory := &Factory{
		config:   cfg,
		observer: newDefaultObserver(),
	}

	for _, option := range options {
		option(factory)
	}

	return factory
}

// CreateMQ 创建MQ实例
func (factory *Factory) CreateMQ() (contract.MQ, error) {
	if !factory.config.Adapter.IsValid() {
		return nil, fmt.Errorf("unsupported adapter: %s", factory.config.Adapter)
	}

	switch factory.config.Adapter {
	case config.AdapterRedis:
		return redis.NewRedisMQ(factory.config.Redis, factory.observer, factory.config.KeyPrefix)
	case config.AdapterRabbitMQ:
		return rabbitmq.NewRabbitMQ(factory.config.RabbitMQ, factory.observer, factory.config.KeyPrefix)
	case config.AdapterKafka:
		return kafka.NewKafkaMQ(factory.config.Kafka, factory.observer, factory.config.KeyPrefix)
	case config.AdapterMemory:
		return memory.NewMemoryMQ(factory.config.Memory, factory.observer, factory.config.KeyPrefix)
	default:
		return nil, fmt.Errorf("unsupported adapter: %s", factory.config.Adapter)
	}
}
