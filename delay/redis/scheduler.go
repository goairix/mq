// Package redisdelay provides durable, optional delayed publication for MQ v2.
package redisdelay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

var ErrScheduleConflict = errors.New("mq delay: message ID already scheduled with different content or deadline")

// Options controls only the optional delay scheduler, never normal MQ traffic.
type Options struct {
	Prefix        string
	Shards        int
	PollInterval  time.Duration
	LeaseDuration time.Duration
	DrainTimeout  time.Duration
}

func (o Options) withDefaults() (Options, error) {
	if o.Shards < 0 || o.PollInterval < 0 || o.LeaseDuration < 0 || o.DrainTimeout < 0 {
		return o, errors.New("negative scheduler option")
	}
	if o.Prefix == "" {
		o.Prefix = "mq:v2:delay"
	}
	if strings.TrimSpace(o.Prefix) == "" || strings.ContainsAny(o.Prefix, "{}") {
		return o, errors.New("invalid scheduler prefix")
	}
	if o.Shards == 0 {
		o.Shards = 16
	}
	if o.Shards > 4096 {
		return o, errors.New("scheduler shards exceed 4096")
	}
	if o.PollInterval == 0 {
		o.PollInterval = 100 * time.Millisecond
	}
	if o.LeaseDuration == 0 {
		o.LeaseDuration = time.Minute
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.PollInterval < time.Millisecond || o.LeaseDuration < time.Millisecond || o.DrainTimeout < time.Millisecond {
		return o, errors.New("scheduler durations must be at least one millisecond")
	}
	return o, nil
}

type laneKeys struct{ due, leased, records, tokens string }

// Scheduler stores delayed tasks and publishes due work through target.
// The caller owns and closes both Redis client and target publisher.
type Scheduler struct {
	client  redis.UniversalClient
	target  mq.Publisher
	options Options
	mu      sync.Mutex
	closed  bool
	active  int
	drained chan struct{}
}

func New(client redis.UniversalClient, target mq.Publisher, options Options) (*Scheduler, error) {
	if isNil(client) || isNil(target) {
		return nil, errors.New("Redis client and target publisher are required")
	}
	configured, err := options.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Scheduler{client: client, target: target, options: configured, drained: make(chan struct{})}, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func (s *Scheduler) laneFor(message mq.Message) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(message.Topic))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(message.ID))
	return int(h.Sum64() % uint64(s.options.Shards))
}

func (s *Scheduler) keys(lane int) laneKeys {
	base := "{" + s.options.Prefix + ":" + strconv.Itoa(lane) + "}:" + s.options.Prefix + ":"
	return laneKeys{due: base + "due", leased: base + "leased", records: base + "records", tokens: base + "tokens"}
}

func taskID(message mq.Message) string {
	sum := sha256.Sum256([]byte(message.Topic + "\x00" + message.ID))
	return hex.EncodeToString(sum[:])
}

type taskRecord struct {
	Version int        `json:"v"`
	DueMS   int64      `json:"due_ms"`
	Message mq.Message `json:"message"`
}

func ceilMillis(when time.Time) int64 {
	ms := when.UnixMilli()
	if when.Nanosecond()%int(time.Millisecond) != 0 {
		ms++
	}
	return ms
}

func encodeTask(message mq.Message, due time.Time) (string, error) {
	message.CreatedAt = message.CreatedAt.UTC()
	bytes, err := json.Marshal(taskRecord{Version: 1, DueMS: ceilMillis(due), Message: message})
	if err != nil {
		return "", fmt.Errorf("encode scheduled message: %w", err)
	}
	return string(bytes), nil
}

func decodeTask(encoded string) (mq.Message, time.Time, error) {
	var record taskRecord
	if err := json.Unmarshal([]byte(encoded), &record); err != nil {
		return mq.Message{}, time.Time{}, fmt.Errorf("decode scheduled message: %w", err)
	}
	if record.Version != 1 {
		return mq.Message{}, time.Time{}, fmt.Errorf("unknown scheduled message version %d", record.Version)
	}
	if err := record.Message.Validate(); err != nil {
		return mq.Message{}, time.Time{}, err
	}
	return record.Message, time.UnixMilli(record.DueMS).UTC(), nil
}

var insertScript = redis.NewScript(`
local old = redis.call('HGET', KEYS[2], ARGV[1])
if old then
  if old == ARGV[2] then return 0 end
  return redis.error_reply('MQ_DELAY_CONFLICT')
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1`)

func (s *Scheduler) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return mq.ErrClosed
	}
	s.active++
	return nil
}

func (s *Scheduler) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.closed && s.active == 0 {
		close(s.drained)
	}
}

func (s *Scheduler) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.active == 0 {
			close(s.drained)
		}
	}
	drained := s.drained
	s.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scheduler) PublishAt(ctx context.Context, message mq.Message, due time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := message.Validate(); err != nil {
		return err
	}
	if due.IsZero() {
		return errors.New("due time is required")
	}
	encoded, err := encodeTask(message, due)
	if err != nil {
		return err
	}
	if err := s.begin(); err != nil {
		return err
	}
	defer s.end()
	k := s.keys(s.laneFor(message))
	_, err = insertScript.Run(ctx, s.client, []string{k.due, k.records}, taskID(message), encoded, ceilMillis(due)).Int()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "MQ_DELAY_CONFLICT") {
		return ErrScheduleConflict
	}
	return mq.OutcomeUnknown(fmt.Errorf("schedule message in Redis: %w", err))
}

var _ mq.ScheduledPublisher = (*Scheduler)(nil)
