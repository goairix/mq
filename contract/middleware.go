package contract

import (
	"context"
	"time"

	"github.com/goairix/mq/message"
	"go.uber.org/zap"
)

// Middleware 中间件函数类型
type Middleware func(next message.Handler) message.Handler

// MiddlewareChain 中间件链
type MiddlewareChain struct {
	middlewares []Middleware
}

// NewMiddlewareChain 创建中间件链
func NewMiddlewareChain(middlewares ...Middleware) *MiddlewareChain {
	return &MiddlewareChain{middlewares: middlewares}
}

// Apply 应用中间件链
func (mc *MiddlewareChain) Apply(handler message.Handler) message.Handler {
	for i := len(mc.middlewares) - 1; i >= 0; i-- {
		handler = mc.middlewares[i](handler)
	}
	return handler
}

// 预定义中间件

// LoggingMiddleware 日志中间件 - 简化版本
func LoggingMiddleware(logger *zap.Logger) Middleware {
	return func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.Message) error {
			start := time.Now()
			err := next(ctx, msg)
			duration := time.Since(start)

			if err != nil {
				logger.Error("message processing failed",
					zap.String("topic", msg.Topic),
					zap.String("message_id", msg.ID),
					zap.Duration("duration", duration),
					zap.Int("payload_size", len(msg.Payload)),
					zap.Error(err),
				)
			} else {
				logger.Info("message processed successfully",
					zap.String("topic", msg.Topic),
					zap.String("message_id", msg.ID),
					zap.Duration("duration", duration),
					zap.Int("payload_size", len(msg.Payload)),
				)
			}

			return err
		}
	}
}

// RetryMiddleware 重试中间件
func RetryMiddleware(maxRetries int, backoff time.Duration) Middleware {
	return func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.Message) error {
			var err error
			for i := 0; i <= maxRetries; i++ {
				err = next(ctx, msg)
				if err == nil {
					return nil
				}

				if i < maxRetries {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(backoff * time.Duration(i+1)):
						// 继续重试
					}
				}
			}
			return err
		}
	}
}

// TimeoutMiddleware 超时中间件
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.Message) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return next(ctx, msg)
		}
	}
}
