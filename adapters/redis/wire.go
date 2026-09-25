package redisadapter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/redis/go-redis/v9"
)

const wireVersion = "1"

func encode(message mq.Message) map[string]any {
	headers, _ := json.Marshal(message.Headers)
	if message.Headers == nil {
		headers = []byte("{}")
	}
	return map[string]any{
		"v":            wireVersion,
		"id":           message.ID,
		"topic":        message.Topic,
		"key":          string(message.Key),
		"payload":      string(message.Payload),
		"created_sec":  strconv.FormatInt(message.CreatedAt.Unix(), 10),
		"created_nano": strconv.Itoa(message.CreatedAt.Nanosecond()),
		"headers":      string(headers),
	}
}

func wireField(values map[string]any, key string) (string, error) {
	value, ok := values[key]
	if !ok || value == nil {
		return "", fmt.Errorf("missing wire field %s", key)
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case int:
		return strconv.Itoa(typed), nil
	default:
		return "", fmt.Errorf("invalid wire field %s type %T", key, value)
	}
}

func decode(entry redis.XMessage) (mq.Message, error) {
	version, err := wireField(entry.Values, "v")
	if err != nil {
		return mq.Message{}, err
	}
	if version != wireVersion {
		return mq.Message{}, fmt.Errorf("unsupported wire version %q", version)
	}
	id, err := wireField(entry.Values, "id")
	if err != nil {
		return mq.Message{}, err
	}
	topic, err := wireField(entry.Values, "topic")
	if err != nil {
		return mq.Message{}, err
	}
	key, err := wireField(entry.Values, "key")
	if err != nil {
		return mq.Message{}, err
	}
	payload, err := wireField(entry.Values, "payload")
	if err != nil {
		return mq.Message{}, err
	}
	created, err := wireField(entry.Values, "created_sec")
	if err != nil {
		return mq.Message{}, err
	}
	sec, err := strconv.ParseInt(created, 10, 64)
	if err != nil {
		return mq.Message{}, fmt.Errorf("invalid creation time: %w", err)
	}
	createdNanos, err := wireField(entry.Values, "created_nano")
	if err != nil {
		return mq.Message{}, err
	}
	ns, err := strconv.Atoi(createdNanos)
	if err != nil || ns < 0 || ns >= int(time.Second) {
		return mq.Message{}, fmt.Errorf("invalid creation nanoseconds %q", createdNanos)
	}
	rawHeaders, err := wireField(entry.Values, "headers")
	if err != nil {
		return mq.Message{}, err
	}
	var headers map[string]string
	if err := json.Unmarshal([]byte(rawHeaders), &headers); err != nil {
		return mq.Message{}, fmt.Errorf("invalid headers: %w", err)
	}
	message := mq.Message{ID: id, Topic: topic, Key: []byte(key), Payload: []byte(payload), Headers: headers, CreatedAt: time.Unix(sec, int64(ns)).UTC()}
	if err := message.Validate(); err != nil {
		return mq.Message{}, err
	}
	return message, nil
}
