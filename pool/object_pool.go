package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
)

// MessagePool 消息对象池
type MessagePool struct {
	pool    sync.Pool
	metrics *observability.MetricsRecorder
	// 统计字段
	getCount    int64
	putCount    int64
	createCount int64
}

// NewMessagePool 创建消息池
func NewMessagePool(metrics *observability.MetricsRecorder) *MessagePool {
	mp := &MessagePool{
		metrics: metrics,
	}
	mp.pool = sync.Pool{
		New: func() interface{} {
			atomic.AddInt64(&mp.createCount, 1)
			if mp.metrics != nil {
				mp.metrics.RecordObjectCreate(context.Background(), "message")
			}
			return &message.Message{
				Headers: make(map[string]string),
			}
		},
	}
	return mp
}

// Get 获取消息对象
func (p *MessagePool) Get() *message.Message {
	atomic.AddInt64(&p.getCount, 1)

	msg := p.pool.Get().(*message.Message)

	// 记录获取操作
	p.metrics.RecordObjectGet(context.Background(), "message")

	// 重置消息
	msg.ID = ""
	msg.Topic = ""
	msg.Payload = nil
	msg.Delay = 0
	msg.Retry = 0
	msg.CreateAt = time.Time{}
	// 清空headers但保留map
	for k := range msg.Headers {
		delete(msg.Headers, k)
	}

	return msg
}

// Put 归还消息对象
func (p *MessagePool) Put(msg *message.Message) {
	if msg != nil {
		atomic.AddInt64(&p.putCount, 1)
		p.metrics.RecordObjectPut(context.Background(), "message")
		p.pool.Put(msg)
	}
}

// ReportMetrics 定期计算和上报池统计信息
func (p *MessagePool) ReportMetrics(ctx context.Context) {
	if p.metrics == nil {
		return
	}

	getCount := atomic.LoadInt64(&p.getCount)
	putCount := atomic.LoadInt64(&p.putCount)
	createCount := atomic.LoadInt64(&p.createCount)

	// 计算活跃对象数量 = 获取次数 - 归还次数
	activeObjects := getCount - putCount
	if activeObjects < 0 {
		activeObjects = 0
	}

	// 记录对象池中的对象数量
	p.metrics.RecordPoolObjectCount(ctx, "message", activeObjects)

	// 计算命中率
	hitRate := float64(getCount-createCount) / float64(getCount)
	if getCount == 0 {
		hitRate = 0
	}

	p.metrics.RecordPoolHitRate(ctx, "message", hitRate)
}

// ByteBufferPool 字节缓冲池
type ByteBufferPool struct {
	pool    sync.Pool
	metrics *observability.MetricsRecorder
	// 统计字段
	getCount     int64
	putCount     int64
	discardCount int64
}

// NewByteBufferPool 创建字节缓冲池
func NewByteBufferPool(metrics *observability.MetricsRecorder) *ByteBufferPool {
	return &ByteBufferPool{
		metrics: metrics,
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, 1024) // 初始容量1KB
				return &buf
			},
		},
	}
}

// Get 获取字节缓冲
func (p *ByteBufferPool) Get() []byte {
	atomic.AddInt64(&p.getCount, 1)

	bufPtr := p.pool.Get().(*[]byte)
	*bufPtr = (*bufPtr)[:0] // 重置长度但保留容量

	// 记录获取操作
	if p.metrics != nil {
		p.metrics.RecordObjectGet(context.Background(), "buffer")
	}

	return *bufPtr
}

// Put 归还字节缓冲
func (p *ByteBufferPool) Put(buf []byte) {
	if cap(buf) <= 64*1024 { // 限制最大容量64KB
		atomic.AddInt64(&p.putCount, 1)
		// 记录归还操作
		if p.metrics != nil {
			p.metrics.RecordObjectPut(context.Background(), "buffer")
		}
		p.pool.Put(&buf) // 传递指针
	} else {
		// 缓冲区太大，丢弃并记录
		atomic.AddInt64(&p.discardCount, 1)
		if p.metrics != nil {
			p.metrics.RecordBufferDiscard(context.Background(), cap(buf))
		}
	}
}

// ReportMetrics 报告缓冲池指标
func (p *ByteBufferPool) ReportMetrics(ctx context.Context) {
	if p.metrics == nil {
		return
	}

	// 估算当前池中的缓冲区数量（这是一个近似值，因为sync.Pool不提供精确计数）
	getCount := atomic.LoadInt64(&p.getCount)
	putCount := atomic.LoadInt64(&p.putCount)
	discardCount := atomic.LoadInt64(&p.discardCount)

	// 当前活跃的缓冲区数量 = 获取次数 - 归还次数 - 丢弃次数
	activeBuffers := getCount - putCount - discardCount
	if activeBuffers < 0 {
		activeBuffers = 0
	}

	// 记录池大小（活跃缓冲区数量）
	p.metrics.RecordBufferPoolSize(ctx, activeBuffers)

	// 计算命中率
	hitRate := float64(getCount-discardCount) / float64(getCount)
	if getCount == 0 {
		hitRate = 0
	}
	p.metrics.RecordPoolHitRate(ctx, "buffer", hitRate)

	// 记录实际的平均缓冲区大小（基于使用情况估算）
	// 由于无法精确跟踪每个缓冲区的大小，使用合理的估算
	if activeBuffers > 0 {
		// 估算平均大小：考虑初始1KB + 可能的增长
		estimatedAvgSize := 1024.0 + float64(getCount)*0.1 // 简单的增长估算
		if estimatedAvgSize > 65536 {                      // 不超过64KB
			estimatedAvgSize = 65536
		}
		p.metrics.RecordBufferAverageSize(ctx, estimatedAvgSize)
	} else {
		p.metrics.RecordBufferAverageSize(ctx, 1024.0) // 默认1KB
	}
}
