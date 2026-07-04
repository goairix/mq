# Message Queue (MQ)

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/goairix/mq)](https://goreportcard.com/report/github.com/goairix/mq)

A high-performance, scalable Go message queue package that supports multiple underlying implementations and enterprise-grade features.

## ✨ Features

- 🚀 **High Performance**: Optimized with connection pooling and batch processing
- 🔧 **Multi-Adapter Support**: Redis, RabbitMQ, Kafka and more mainstream message queues
- ⏰ **Delay Queue**: Efficient delayed message processing based on time wheel algorithm
- 📊 **Observability**: Integrated Prometheus metrics, OpenTelemetry tracing and structured logging
- 🏗️ **Elegant Architecture**: Unified interface design, easy to extend and maintain
- 🛡️ **Enterprise-Grade**: Comprehensive error handling, retry mechanisms and health checks
- 🔑 **Key Prefix Support**: Global key prefix for multi-tenant isolation
- 🎯 **Type Safety**: Strong typing with adapter validation
- 📦 **Zero Dependencies**: Optional observability components

## 🚀 Quick Start

### Installation

```bash
go get github.com/goairix/mq
```

### Basic Usage
```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/goairix/mq"
    "github.com/goairix/mq/config"
    "github.com/goairix/mq/message"
)

func main() {
    // Create configuration
    cfg := config.Config{
        Adapter:   config.AdapterRedis,
        KeyPrefix: "app:mq",
        Redis:     config.DefaultRedisConfig(),
    }

    // Create MQ factory
    factory := mq.NewFactory(cfg)
    mqInstance, err := factory.CreateMQ()
    if err != nil {
        log.Fatal("Failed to create MQ:", err)
    }
    defer mqInstance.Close()

    ctx := context.Background()

    // Producer example
    producer := mqInstance.Producer()
    msg := &message.Message{
        Topic:   "test-topic",
        Payload: []byte("Hello, World!"),
        Headers: map[string]string{
            "source": "example",
        },
    }

    err = producer.Send(ctx, msg)
    if err != nil {
        log.Fatal("Failed to send message:", err)
    }

    // Consumer example
    consumer := mqInstance.Consumer()
    err = consumer.Subscribe(ctx, "test-topic", func(ctx context.Context, msg *message.Message) error {
        log.Printf("Received: %s", string(msg.Payload))
        return nil
    })
    if err != nil {
        log.Fatal("Failed to subscribe:", err)
    }

    // Delay queue example
    delayMsg := &message.Message{
        Topic:   "delay-topic",
        Payload: []byte("Delayed message"),
    }
    err = mqInstance.DelayQueue().Push(ctx, delayMsg, 10*time.Second)
    if err != nil {
        log.Fatal("Failed to send delay message:", err)
    }

    time.Sleep(30 * time.Second)
}
```

### 📋 Supported Adapters

|Adapter | Status | Features|
|--------|--------|---------|
| Memory |✅ |Pure in-memory queues, High performance, **Single machine only**|
| Redis| ✅ |List-based queues, Sorted sets for delays|
| RabbitMQ| ✅ |AMQP protocol, Exchange routing|
| Kafka |✅ |Distributed streaming, Partitioning|

## ⚙️ Configuration

### Memory Configuration
```go
cfg := config.Config{
    Adapter:   config.AdapterMemory,
    KeyPrefix: "myapp",
    Memory: config.MemoryConfig{
        // Queue configuration
        MaxQueueSize:       10000,              // Max queue size per topic
        MaxDelayQueueSize:  1000,               // Max delay queue size
        DelayCheckInterval: 100 * time.Millisecond, // Delay message check interval
        
        // Monitoring configuration
        EnableMetrics:      true,               // Enable metrics collection
    },
}
```
**Note**: The memory adapter is a pure in-memory implementation with no data persistence. It's suitable for single-machine environments only. All messages will be lost after application restart.

### Redis Configuration
```go
cfg := config.Config{
    Adapter:   config.AdapterRedis,
    KeyPrefix: "myapp",
    Redis: config.RedisConfig{
        Addr:         "localhost:6379",
        Password:     "",
        DB:           0,
        PoolSize:     10,
        MinIdleConns: 5,
        MaxConnAge:   time.Hour,
        PoolTimeout:  30 * time.Second,
        IdleTimeout:  5 * time.Minute,
    },
}
```
### RabbitMQ Configuration
```go
cfg := config.Config{
    Adapter:   config.AdapterRabbitMQ,
    KeyPrefix: "myapp",
    RabbitMQ: config.RabbitMQConfig{
        URL:              "amqp://guest:guest@localhost:5672/",
        Exchange:         "mq.direct",
        ExchangeType:     "direct",
        QueueDurable:     true,
        QueueAutoDelete:  false,
        QueueExclusive:   false,
        QoS:              10,
        Heartbeat:        60 * time.Second,
        ConnectionTimeout: 30 * time.Second,
    },
}
```
### Kafka Configuration
```go
cfg := config.Config{
    Adapter:   config.AdapterKafka,
    KeyPrefix: "myapp",
    Kafka: config.KafkaConfig{
        Brokers:  []string{"localhost:9092"},
        GroupID:  "mq-consumer-group",
        ClientID: "mq-client",
        Version:  "2.8.0",
        Producer: config.KafkaProducerConfig{
            MaxMessageBytes: 1000000,
            RequiredAcks:    1,
            Timeout:         30 * time.Second,
            Compression:     "snappy",
            Idempotent:      true,
        },
        Consumer: config.KafkaConsumerConfig{
            MinBytes:          1,
            MaxBytes:          1048576,
            MaxWait:           500 * time.Millisecond,
            CommitInterval:    1 * time.Second,
            StartOffset:       -1,
            HeartbeatInterval: 3 * time.Second,
            SessionTimeout:    30 * time.Second,
            RebalanceTimeout:  30 * time.Second,
        },
    },
}
```

## 📊 Observability
The package supports comprehensive observability through OpenTelemetry and structured logging.

### With Custom Observer

```go
package main

import (
    "github.com/goairix/mq"
    "github.com/goairix/mq/config"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/metric"
    "go.uber.org/zap"
)

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
    // Initialize OpenTelemetry and zap logger
    meter := otel.Meter("mq-service")
    logger, _ := zap.NewProduction()
    
    observer := &MyObserver{
        meter:  meter,
        logger: logger,
    }

    cfg := config.Config{
        Adapter:   config.AdapterRedis,
        KeyPrefix: "app:mq",
        Redis:     config.DefaultRedisConfig(),
    }

    // Create factory with observer
    factory := mq.NewFactory(cfg, mq.WithObserver(observer))
    mqInstance, err := factory.CreateMQ()
    if err != nil {
        panic(err)
    }
    defer mqInstance.Close()

    // Your application logic here...
}
```

### Available Metrics
- mq_messages_sent_total - Total number of messages sent
- mq_messages_received_total - Total number of messages received
- mq_messages_failed_total - Total number of failed messages
- mq_message_processing_duration_seconds - Message processing duration
- mq_queue_size - Current queue size

## 🏗️ Architecture
```
┌─────────────────┐
│   Application   │
└─────────┬───────┘
          │
┌─────────▼───────┐
│     Factory     │
└─────────┬───────┘
          │
┌─────────▼───────┐
│   MQ Interface  │
├─────────────────┤
│   • Producer    │
│   • Consumer    │
│   • DelayQueue  │
│   • HealthCheck │
└─────────┬───────┘
          │
┌─────────▼───────┐
│    Adapters     │
├─────────────────┤
│ Redis │RabbitMQ │
│ Kafka │  ...    │
└─────────────────┘
```

## 🔧 Advanced Features

### Delay Queue
The delay queue uses a time wheel algorithm for efficient delayed message processing:

```go
// Send a delayed message
msg := &message.Message{
    Topic:   "notification",
    Payload: []byte("Reminder: Meeting in 1 hour"),
}

// Will be delivered after 1 hour
err := mqInstance.DelayQueue().Push(ctx, msg, time.Hour)
```

### Batch Operations
```go
// Batch send messages
messages := []*message.Message{
    {Topic: "topic1", Payload: []byte("msg1")},
    {Topic: "topic2", Payload: []byte("msg2")},
}

err := producer.SendBatch(ctx, messages)
```

### Health Checks
```go
// Check MQ health
if err := mqInstance.HealthCheck(); err != nil {
    log.Printf("MQ health check failed: %v", err)
}
```

## 🧪 Testing
```bash
# Run tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. ./...
```

## 📝 Examples
Check out the examples directory for more comprehensive usage examples:

- Basic Usage - Simple producer/consumer example
- With Observability - Full observability setup

## 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments
- [go-redis](https://github.com/go-redis/redis) for Redis client
- [amqp091-go](https://github.com/rabbitmq/amqp091-go) for RabbitMQ client
- [kafka-go](https://github.com/segmentio/kafka-go) for Kafka client
- [OpenTelemetry](https://opentelemetry.io/docs/) for observability
- [Zap](https://github.com/uber-go/zap) for structured logging