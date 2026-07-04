package memory

import (
	"fmt"
	"sync"

	"github.com/goairix/mq/message"
)

// Queue 内存队列
type Queue struct {
	messages []*message.Message
	maxSize  int
	mu       sync.RWMutex
}

// NewQueue 创建新队列
func NewQueue(maxSize int) *Queue {
	return &Queue{
		messages: make([]*message.Message, 0),
		maxSize:  maxSize,
	}
}

// Push 推送消息到队列
func (q *Queue) Push(msg *message.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// 检查队列大小限制
	if q.maxSize > 0 && len(q.messages) >= q.maxSize {
		return fmt.Errorf("queue is full, max size: %d", q.maxSize)
	}

	q.messages = append(q.messages, msg)
	return nil
}

// Pop 从队列弹出消息
func (q *Queue) Pop() *message.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg
}

// Size 获取队列大小
func (q *Queue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.messages)
}

// IsEmpty 检查队列是否为空
func (q *Queue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.messages) == 0
}
