package redisdelay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

type claimedTask struct {
	lane    int
	id      string
	token   string
	message mq.Message
}

var claimScript = redis.NewScript(`
local function valid(key, expected)
  local kind = redis.call('TYPE', key).ok
  return kind == 'none' or kind == expected
end
if not valid(KEYS[1], 'zset') or not valid(KEYS[2], 'zset') or not valid(KEYS[3], 'hash') or not valid(KEYS[4], 'hash') then
  return redis.error_reply('MQ_DELAY_BAD_KEY_TYPE')
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 1)
if #expired > 0 then
  local id = expired[1]
  local record = redis.call('HGET', KEYS[3], id)
  if not record then return redis.error_reply('MQ_DELAY_MISSING_RECORD') end
  redis.call('HSET', KEYS[4], id, ARGV[2])
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), id)
  return {id, record}
end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #due == 0 then return nil end
local id = due[1]
local record = redis.call('HGET', KEYS[3], id)
if not record then return redis.error_reply('MQ_DELAY_MISSING_RECORD') end
redis.call('HSET', KEYS[4], id, ARGV[2])
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[1]), id)
redis.call('ZREM', KEYS[1], id)
return {id, record}`)

var completeScript = redis.NewScript(`
local function valid(key, expected)
  local kind = redis.call('TYPE', key).ok
  return kind == 'none' or kind == expected
end
if not valid(KEYS[1], 'zset') or not valid(KEYS[2], 'hash') or not valid(KEYS[3], 'hash') then
  return redis.error_reply('MQ_DELAY_BAD_KEY_TYPE')
end
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1`)

func (s *Scheduler) claimOne(ctx context.Context, lane int) (*claimedTask, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, fmt.Errorf("generate claim token: %w", err)
	}
	token := hex.EncodeToString(random[:])
	k := s.keys(lane)
	result, err := claimScript.Run(ctx, s.client, []string{k.due, k.leased, k.records, k.tokens}, s.options.LeaseDuration.Milliseconds(), token).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim due Redis message: %w", err)
	}
	if len(result) != 2 {
		return nil, fmt.Errorf("claim script returned %d fields", len(result))
	}
	message, _, err := decodeTask(result[1])
	if err != nil {
		return nil, fmt.Errorf("decode claimed task %s: %w", result[0], err)
	}
	return &claimedTask{lane: lane, id: result[0], token: token, message: message}, nil
}

func (s *Scheduler) complete(ctx context.Context, task *claimedTask) (bool, error) {
	k := s.keys(task.lane)
	result, err := completeScript.Run(ctx, s.client, []string{k.leased, k.records, k.tokens}, task.id, task.token).Int()
	if err != nil {
		return false, fmt.Errorf("complete confirmed Redis task: %w", err)
	}
	return result == 1, nil
}

// Run publishes due tasks until ctx is canceled or an operational error occurs.
// Run may be called by multiple workers using the same prefix and shard count.
func (s *Scheduler) Run(ctx context.Context) error {
	deliveryCtx, finish := s.deliveryContext(ctx)
	defer finish()
	nextLane := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-s.closedCh:
			return mq.ErrClosed
		default:
		}
		found := false
		for offset := 0; offset < s.options.Shards; offset++ {
			lane := (nextLane + offset) % s.options.Shards
			if err := s.begin(); err != nil {
				return err
			}
			task, err := s.claimOne(ctx, lane)
			if err != nil {
				s.end()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			if task == nil {
				s.end()
				continue
			}
			nextLane = (lane + 1) % s.options.Shards
			err = s.target.Publish(deliveryCtx, task.message)
			if err == nil {
				_, err = s.complete(deliveryCtx, task)
			}
			s.end()
			if err != nil {
				return fmt.Errorf("publish scheduled message %s: %w", task.message.ID, err)
			}
			found = true
			break
		}
		if found {
			continue
		}
		timer := time.NewTimer(s.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.closedCh:
			timer.Stop()
			return mq.ErrClosed
		case <-timer.C:
		}
	}
}

func (s *Scheduler) deliveryContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stop := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
			timer := time.NewTimer(s.options.DrainTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancel()
			case <-stop:
			}
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }
}
