package redis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/goairix/mq/config"
)

// Client Redis客户端接口
type Client interface {
	redis.Cmdable
	Close() error
	Ping(ctx context.Context) *redis.StatusCmd
}

// ClientFactory Redis客户端工厂
type ClientFactory struct {
	config config.RedisConfig
}

// NewClientFactory 创建客户端工厂
func NewClientFactory(config config.RedisConfig) *ClientFactory {
	return &ClientFactory{config: config}
}

// CreateClient 创建Redis客户端
func (f *ClientFactory) CreateClient() (Client, error) {
	switch f.config.Mode {
	case config.RedisModeStandalone:
		return f.createStandaloneClient(), nil
	case config.RedisModeCluster:
		return f.createClusterClient(), nil
	case config.RedisModeSentinel:
		return f.createSentinelClient(), nil
	default:
		return nil, fmt.Errorf("unsupported redis mode: %s", f.config.Mode)
	}
}

// createStandaloneClient 创建单机客户端
func (f *ClientFactory) createStandaloneClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:               f.config.Addr,
		Password:           f.config.Password,
		DB:                 f.config.DB,
		PoolSize:           f.config.PoolSize,
		MinIdleConns:       f.config.MinIdleConns,
		MaxConnAge:         f.config.MaxConnAge,
		PoolTimeout:        f.config.PoolTimeout,
		IdleTimeout:        f.config.IdleTimeout,
		IdleCheckFrequency: f.config.IdleCheckFrequency,
		MaxRetries:         f.config.MaxRetries,
		MinRetryBackoff:    f.config.MinRetryBackoff,
		MaxRetryBackoff:    f.config.MaxRetryBackoff,
		DialTimeout:        f.config.DialTimeout,
		ReadTimeout:        f.config.ReadTimeout,
		WriteTimeout:       f.config.WriteTimeout,
	})
}

// createClusterClient 创建集群客户端
func (f *ClientFactory) createClusterClient() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              f.config.Addrs,
		Password:           f.config.Password,
		PoolSize:           f.config.PoolSize,
		MinIdleConns:       f.config.MinIdleConns,
		MaxConnAge:         f.config.MaxConnAge,
		PoolTimeout:        f.config.PoolTimeout,
		IdleTimeout:        f.config.IdleTimeout,
		IdleCheckFrequency: f.config.IdleCheckFrequency,
		MaxRetries:         f.config.MaxRetries,
		MinRetryBackoff:    f.config.MinRetryBackoff,
		MaxRetryBackoff:    f.config.MaxRetryBackoff,
		DialTimeout:        f.config.DialTimeout,
		ReadTimeout:        f.config.ReadTimeout,
		WriteTimeout:       f.config.WriteTimeout,
	})
}

// createSentinelClient 创建哨兵客户端
func (f *ClientFactory) createSentinelClient() *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:         f.config.MasterName,
		SentinelAddrs:      f.config.SentinelAddrs,
		SentinelPassword:   f.config.SentinelPassword,
		Password:           f.config.Password,
		DB:                 f.config.DB,
		PoolSize:           f.config.PoolSize,
		MinIdleConns:       f.config.MinIdleConns,
		MaxConnAge:         f.config.MaxConnAge,
		PoolTimeout:        f.config.PoolTimeout,
		IdleTimeout:        f.config.IdleTimeout,
		IdleCheckFrequency: f.config.IdleCheckFrequency,
		MaxRetries:         f.config.MaxRetries,
		MinRetryBackoff:    f.config.MinRetryBackoff,
		MaxRetryBackoff:    f.config.MaxRetryBackoff,
		DialTimeout:        f.config.DialTimeout,
		ReadTimeout:        f.config.ReadTimeout,
		WriteTimeout:       f.config.WriteTimeout,
	})
}
