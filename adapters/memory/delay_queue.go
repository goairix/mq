package memory

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goairix/mq/config"
	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
)

// DelayQueue 延时队列
type DelayQueue struct {
	heap      *DelayHeap
	recorder  *observability.MetricsRecorder
	keyPrefix string
	config    config.MemoryConfig
	mu        sync.RWMutex
}

// DelayMessage 延时消息
type DelayMessage struct {
	Message   *message.Message
	ExecuteAt time.Time
	Index     int // heap中的索引
}

// DelayHeap 延时消息堆
type DelayHeap []*DelayMessage

func (h DelayHeap) Len() int           { return len(h) }
func (h DelayHeap) Less(i, j int) bool { return h[i].ExecuteAt.Before(h[j].ExecuteAt) }
func (h DelayHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *DelayHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*DelayMessage)
	item.Index = n
	*h = append(*h, item)
}

func (h *DelayHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*h = old[0 : n-1]
	return item
}

// NewMemoryDelayQueue 创建内存延时队列
func NewMemoryDelayQueue(cfg config.MemoryConfig, recorder *observability.MetricsRecorder, keyPrefix string) *DelayQueue {
	h := &DelayHeap{}
	heap.Init(h)

	return &DelayQueue{
		heap:      h,
		recorder:  recorder,
		keyPrefix: keyPrefix,
		config:    cfg,
	}
}

// Push 推送延时消息
func (dq *DelayQueue) Push(ctx context.Context, msg *message.Message, delay time.Duration) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	// 检查队列大小限制
	if dq.config.MaxDelayQueueSize > 0 && dq.heap.Len() >= dq.config.MaxDelayQueueSize {
		return fmt.Errorf("delay queue is full, max size: %d", dq.config.MaxDelayQueueSize)
	}

	delayMsg := &DelayMessage{
		Message:   msg,
		ExecuteAt: time.Now().Add(delay),
	}

	heap.Push(dq.heap, delayMsg)

	if dq.recorder != nil {
		dq.recorder.RecordProcessingError(ctx, msg.Topic, "push_error")
	}

	return nil
}

// Pop 弹出到期消息
func (dq *DelayQueue) Pop(ctx context.Context) (*message.Message, error) {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	if dq.heap.Len() == 0 {
		return nil, nil
	}

	// 检查堆顶消息是否到期
	top := (*dq.heap)[0]
	if time.Now().Before(top.ExecuteAt) {
		return nil, nil // 还没到期
	}

	// 弹出到期消息
	delayMsg := heap.Pop(dq.heap).(*DelayMessage)

	return delayMsg.Message, nil
}

// Remove 移除消息
func (dq *DelayQueue) Remove(ctx context.Context, msgID string) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	// 查找并移除消息
	for i, delayMsg := range *dq.heap {
		if delayMsg.Message.ID == msgID {
			heap.Remove(dq.heap, i)
			return nil
		}
	}

	return fmt.Errorf("message not found: %s", msgID)
}

// Size 获取队列大小
func (dq *DelayQueue) Size(ctx context.Context) (int64, error) {
	dq.mu.RLock()
	defer dq.mu.RUnlock()
	return int64(dq.heap.Len()), nil
}
