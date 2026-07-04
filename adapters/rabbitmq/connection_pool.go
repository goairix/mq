package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/observability"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// ConnectionPool RabbitMQ连接池
type ConnectionPool struct {
	connections  []*amqp.Connection
	channels     chan *amqp.Channel
	config       config.RabbitMQConfig
	logger       *zap.Logger
	recorder     *observability.MetricsRecorder
	mu           sync.RWMutex
	closed       bool
	connURL      string
	reconnectCh  chan struct{}
	healthTicker *time.Ticker
}

// ConnectionFactory RabbitMQ连接工厂
type ConnectionFactory struct {
	config config.RabbitMQConfig
}

// NewConnectionFactory 创建连接工厂
func NewConnectionFactory(config config.RabbitMQConfig) *ConnectionFactory {
	return &ConnectionFactory{
		config: config,
	}
}

// CreateConnectionPool 创建连接池
func (f *ConnectionFactory) CreateConnectionPool(observer observability.Observer, recorder *observability.MetricsRecorder) (*ConnectionPool, error) {
	var connURL string
	if f.config.URL != "" {
		connURL = f.config.URL
	} else {
		connURL = fmt.Sprintf("amqp://%s:%s@%s:%d%s",
			f.config.Username, f.config.Password, f.config.Host, f.config.Port, f.config.VHost)
	}

	pool := &ConnectionPool{
		connections: make([]*amqp.Connection, 0, f.config.PoolSize),
		channels:    make(chan *amqp.Channel, f.config.ChannelPoolSize),
		config:      f.config,
		logger:      observer.GetLogger(),
		recorder:    recorder,
		connURL:     connURL,
	}

	// 初始化连接池
	if err := pool.initialize(); err != nil {
		return nil, err
	}

	return pool, nil
}

// initialize 初始化连接池
func (p *ConnectionPool) initialize() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.logger.Info("开始初始化连接池", zap.Int("min_connections", p.config.MinConnections), zap.Int("channel_pool_size", p.config.ChannelPoolSize))

	// 创建初始连接
	for i := 0; i < p.config.MinConnections; i++ {
		conn, err := p.createConnection()
		if err != nil {
			return fmt.Errorf("failed to create initial connection %d: %w", i, err)
		}
		p.connections = append(p.connections, conn)
	}

	// 预创建通道池
	p.logger.Info("开始创建通道池")
	for i := 0; i < p.config.ChannelPoolSize; i++ {
		if i%10 == 0 {
			p.logger.Info("创建通道进度", zap.Int("created", i), zap.Int("total", p.config.ChannelPoolSize))
		}

		// 直接使用第一个连接创建通道，避免调用 getHealthyConnection() 导致死锁
		if len(p.connections) == 0 {
			p.logger.Warn("no connections available for channel creation")
			continue
		}

		conn := p.connections[0] // 使用第一个连接
		if conn.IsClosed() {
			p.logger.Warn("connection is closed, skipping channel creation", zap.Int("index", i))
			continue
		}

		// 直接创建通道，不使用 createChannel() 方法
		ch, err := conn.Channel()
		if err != nil {
			p.logger.Warn("failed to create initial channel", zap.Error(err), zap.Int("index", i))
			continue
		}
		p.channels <- ch
	}
	p.logger.Info("通道池创建完成")

	// 记录连接池大小
	p.recordPoolSize()

	// 启动健康检查
	p.startHealthCheck()

	// 启动重连监听
	p.startReconnectMonitor()

	return nil
}

// startHealthCheck 启动健康检查
func (p *ConnectionPool) startHealthCheck() {
	p.healthTicker = time.NewTicker(30 * time.Second)
	go func() {
		for {
			select {
			case <-p.healthTicker.C:
				p.performHealthCheck()
			case <-p.reconnectCh:
				return
			}
		}
	}()
}

// performHealthCheck 执行健康检查
func (p *ConnectionPool) performHealthCheck() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	healthyCount := 0
	for i, conn := range p.connections {
		if conn.IsClosed() {
			p.logger.Warn("connection is closed, attempting to reconnect", zap.Int("index", i))
			// 尝试重新连接
			newConn, err := p.createConnection()
			if err != nil {
				p.logger.Error("failed to reconnect", zap.Error(err), zap.Int("index", i))
				continue
			}
			p.connections[i] = newConn
			p.logger.Info("connection reconnected successfully", zap.Int("index", i))
		}
		healthyCount++
	}

	// 记录健康连接数 - 使用已定义的方法
	p.recorder.RecordConnectionPoolSize(context.Background(), int64(healthyCount))
}

// startReconnectMonitor 启动重连监听
func (p *ConnectionPool) startReconnectMonitor() {
	p.reconnectCh = make(chan struct{})
	go func() {
		for _, conn := range p.connections {
			go p.monitorConnection(conn)
		}
	}()
}

// monitorConnection 监控单个连接
func (p *ConnectionPool) monitorConnection(conn *amqp.Connection) {
	notifyClose := make(chan *amqp.Error)
	conn.NotifyClose(notifyClose)

	select {
	case err := <-notifyClose:
		if err != nil {
			p.logger.Error("connection closed unexpectedly", zap.Error(err))
			// 触发健康检查
			go p.performHealthCheck()
		}
	case <-p.reconnectCh:
		return
	}
}

// createConnection 创建新连接
func (p *ConnectionPool) createConnection() (*amqp.Connection, error) {
	cfg := amqp.Config{
		Heartbeat: p.config.Heartbeat,
		Dial:      amqp.DefaultDial(p.config.ConnectionTimeout),
	}

	if p.config.ChannelMax > 0 {
		cfg.ChannelMax = uint16(p.config.ChannelMax)
	}
	if p.config.FrameSize > 0 {
		cfg.FrameSize = p.config.FrameSize
	}

	return amqp.DialConfig(p.connURL, cfg)
}

// createChannel 创建新通道
func (p *ConnectionPool) createChannel() (*amqp.Channel, error) {
	conn := p.getHealthyConnection()
	if conn == nil {
		return nil, fmt.Errorf("no healthy connection available")
	}

	// 添加超时机制
	type result struct {
		ch  *amqp.Channel
		err error
	}

	resultCh := make(chan result, 1)
	go func() {
		ch, err := conn.Channel()
		resultCh <- result{ch: ch, err: err}
	}()

	select {
	case res := <-resultCh:
		return res.ch, res.err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("channel creation timeout after 10 seconds")
	}
}

// getHealthyConnection 获取健康的连接
func (p *ConnectionPool) getHealthyConnection() *amqp.Connection {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, conn := range p.connections {
		if !conn.IsClosed() {
			return conn
		}
	}
	return nil
}

// GetChannel 获取通道
func (p *ConnectionPool) GetChannel(ctx context.Context) (*amqp.Channel, error) {
	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}

	select {
	case ch := <-p.channels:
		if !ch.IsClosed() {
			return ch, nil
		}
		// 通道已关闭，创建新的
		return p.createChannel()
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// 池中没有可用通道，创建新的
		return p.createChannel()
	}
}

// ReturnChannel 归还通道
func (p *ConnectionPool) ReturnChannel(ch *amqp.Channel) {
	if p.closed || ch.IsClosed() {
		return
	}

	select {
	case p.channels <- ch:
		// 成功归还
	default:
		// 池已满，关闭通道
		err := ch.Close()
		if err != nil {
			p.logger.Warn("failed to close channel", zap.Error(err))
		}
	}
}

// recordPoolSize 记录连接池大小
func (p *ConnectionPool) recordPoolSize() {
	ctx := context.Background()
	p.recorder.RecordConnectionPoolSize(ctx, int64(len(p.connections)))
}

// HealthCheck 健康检查
func (p *ConnectionPool) HealthCheck(ctx context.Context) error {
	start := time.Now()
	defer func() {
		p.recorder.RecordProcessingTime(ctx, "health_check", time.Since(start))
	}()

	healthyCount := 0
	for _, conn := range p.connections {
		if !conn.IsClosed() {
			healthyCount++
		}
	}

	// 记录健康连接数
	p.recorder.RecordConnectionPoolSize(ctx, int64(healthyCount))

	if healthyCount == 0 {
		err := fmt.Errorf("no healthy connections available")
		p.recorder.RecordProcessingError(ctx, "connection_pool", "no_healthy_connections")
		return err
	}

	return nil
}

// Close 关闭连接池
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	// 停止健康检查
	if p.healthTicker != nil {
		p.healthTicker.Stop()
	}

	// 停止重连监听
	if p.reconnectCh != nil {
		close(p.reconnectCh)
	}

	// 关闭所有通道
	close(p.channels)
	for ch := range p.channels {
		if err := ch.Close(); err != nil {
			p.logger.Warn("failed to close channel", zap.Error(err))
		}
	}

	// 关闭所有连接
	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			p.logger.Warn("failed to close connection", zap.Error(err))
		}
	}

	return nil
}
