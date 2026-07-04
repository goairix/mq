package redis

import (
	"context"
	"sync"
	"time"

	"github.com/goairix/mq/message"
	"github.com/goairix/mq/observability"
	"go.uber.org/zap"
)

// WorkerPool 工作池
type WorkerPool struct {
	workerCount int
	bufferSize  int
	tasks       chan *Task
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zap.Logger
	metrics     *observability.MetricsRecorder
}

// Task 任务
type Task struct {
	Message *message.Message
	Handler message.Handler
	Topic   string
}

// NewWorkerPool 创建工作池
func NewWorkerPool(workerCount, bufferSize int, logger *zap.Logger, metrics *observability.MetricsRecorder) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		workerCount: workerCount,
		bufferSize:  bufferSize,
		tasks:       make(chan *Task, bufferSize),
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger,
		metrics:     metrics,
	}
}

// Start 启动工作池
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker 工作协程
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	wp.logger.Info("worker started", zap.Int("worker_id", id))

	for {
		select {
		case <-wp.ctx.Done():
			wp.logger.Info("worker stopped", zap.Int("worker_id", id))
			return
		case task := <-wp.tasks:
			start := time.Now()
			err := task.Handler(wp.ctx, task.Message)
			duration := time.Since(start)

			// 记录性能指标
			if err != nil {
				wp.metrics.RecordMessageFailed(wp.ctx, task.Topic, err)
				wp.logger.Error("task processing failed",
					zap.Int("worker_id", id),
					zap.String("topic", task.Topic),
					zap.String("message_id", task.Message.ID),
					zap.Duration("duration", duration),
					zap.Error(err),
				)
			} else {
				wp.metrics.RecordMessageReceived(wp.ctx, task.Topic)
				wp.metrics.RecordProcessingTime(wp.ctx, task.Topic, duration)
				wp.logger.Debug("task processed successfully",
					zap.Int("worker_id", id),
					zap.String("topic", task.Topic),
					zap.String("message_id", task.Message.ID),
					zap.Duration("duration", duration),
				)
			}
		}
	}
}

// Submit 提交任务
func (wp *WorkerPool) Submit(task *Task) bool {
	select {
	case wp.tasks <- task:
		return true
	default:
		wp.logger.Warn("worker pool buffer full, dropping task",
			zap.String("topic", task.Topic),
			zap.String("message_id", task.Message.ID),
		)
		return false
	}
}

// Stop 停止工作池
func (wp *WorkerPool) Stop() {
	wp.cancel()
	close(wp.tasks)
	wp.wg.Wait()
	wp.logger.Info("worker pool stopped")
}

// Stats 获取统计信息
func (wp *WorkerPool) Stats() (int, int) {
	return len(wp.tasks), cap(wp.tasks)
}
