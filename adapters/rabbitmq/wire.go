package rabbitadapter

import (
	"errors"
	"fmt"
	"time"

	mq "github.com/goairix/mq/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func encode(message mq.Message) amqp.Publishing {
	userHeaders := make(amqp.Table, len(message.Headers))
	for key, value := range message.Headers {
		userHeaders[key] = value
	}
	headers := amqp.Table{
		"mq.v":            int32(1),
		"mq.topic":        message.Topic,
		"mq.key":          append([]byte{}, message.Key...),
		"mq.created.sec":  message.CreatedAt.Unix(),
		"mq.created.nsec": int32(message.CreatedAt.Nanosecond()),
		"mq.headers":      userHeaders,
	}
	return amqp.Publishing{Headers: headers, ContentType: "application/octet-stream", DeliveryMode: amqp.Persistent, MessageId: message.ID, Timestamp: message.CreatedAt, Body: append([]byte(nil), message.Payload...)}
}

func decode(delivery amqp.Delivery) (mq.Message, error) {
	version, ok := delivery.Headers["mq.v"].(int32)
	if !ok || version != 1 {
		return mq.Message{}, errors.New("unknown RabbitMQ message envelope version")
	}
	topic, ok := delivery.Headers["mq.topic"].(string)
	if !ok {
		return mq.Message{}, errors.New("missing RabbitMQ message topic")
	}
	var key []byte
	switch raw := delivery.Headers["mq.key"].(type) {
	case []byte:
		key = append([]byte(nil), raw...)
	case string:
		key = []byte(raw)
	default:
		return mq.Message{}, errors.New("invalid RabbitMQ message key")
	}
	seconds, ok := delivery.Headers["mq.created.sec"].(int64)
	if !ok {
		return mq.Message{}, errors.New("invalid RabbitMQ creation seconds")
	}
	nanos, ok := delivery.Headers["mq.created.nsec"].(int32)
	if !ok || nanos < 0 || nanos >= 1e9 {
		return mq.Message{}, errors.New("invalid RabbitMQ creation nanoseconds")
	}
	rawHeaders, ok := delivery.Headers["mq.headers"].(amqp.Table)
	if !ok {
		return mq.Message{}, errors.New("invalid RabbitMQ user headers")
	}
	headers := make(map[string]string, len(rawHeaders))
	for name, raw := range rawHeaders {
		value, ok := raw.(string)
		if !ok {
			return mq.Message{}, fmt.Errorf("invalid RabbitMQ header %q", name)
		}
		headers[name] = value
	}
	message := mq.Message{ID: delivery.MessageId, Topic: topic, Key: key, Payload: append([]byte(nil), delivery.Body...), Headers: headers, CreatedAt: time.Unix(seconds, int64(nanos)).UTC()}
	if err := message.Validate(); err != nil {
		return mq.Message{}, err
	}
	return message, nil
}
