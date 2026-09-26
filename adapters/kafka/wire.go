package kafkaadapter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	mq "github.com/goairix/mq/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func encode(message mq.Message) *kgo.Record {
	var seconds [8]byte
	var nanos [4]byte
	binary.BigEndian.PutUint64(seconds[:], uint64(message.CreatedAt.Unix()))
	binary.BigEndian.PutUint32(nanos[:], uint32(message.CreatedAt.Nanosecond()))
	headers := make([]kgo.RecordHeader, 0, len(message.Headers)+4)
	headers = append(headers,
		kgo.RecordHeader{Key: "mq.v", Value: []byte{1}},
		kgo.RecordHeader{Key: "mq.id", Value: []byte(message.ID)},
		kgo.RecordHeader{Key: "mq.created.sec", Value: seconds[:]},
		kgo.RecordHeader{Key: "mq.created.nsec", Value: nanos[:]})
	for key, value := range message.Headers {
		headers = append(headers, kgo.RecordHeader{Key: "u." + key, Value: []byte(value)})
	}
	return &kgo.Record{Topic: message.Topic, Key: message.Key, Value: message.Payload, Headers: headers, Timestamp: message.CreatedAt}
}

func decode(record *kgo.Record) (mq.Message, error) {
	if record == nil {
		return mq.Message{}, errors.New("nil Kafka record")
	}
	var version, id []byte
	var seconds [8]byte
	var nanos [4]byte
	var haveSeconds, haveNanos bool
	headers := make(map[string]string)
	seen := make(map[string]bool)
	for _, header := range record.Headers {
		if seen[header.Key] {
			return mq.Message{}, fmt.Errorf("duplicate Kafka header %q", header.Key)
		}
		seen[header.Key] = true
		switch header.Key {
		case "mq.v":
			version = header.Value
		case "mq.id":
			id = header.Value
		case "mq.created.sec":
			if len(header.Value) != 8 {
				return mq.Message{}, errors.New("invalid Kafka created seconds")
			}
			copy(seconds[:], header.Value)
			haveSeconds = true
		case "mq.created.nsec":
			if len(header.Value) != 4 {
				return mq.Message{}, errors.New("invalid Kafka created nanoseconds")
			}
			copy(nanos[:], header.Value)
			haveNanos = true
		default:
			if strings.HasPrefix(header.Key, "u.") {
				headers[strings.TrimPrefix(header.Key, "u.")] = string(header.Value)
			}
		}
	}
	if len(version) != 1 || version[0] != 1 || len(id) == 0 || !haveSeconds || !haveNanos {
		return mq.Message{}, errors.New("invalid Kafka envelope")
	}
	nsec := binary.BigEndian.Uint32(nanos[:])
	if nsec >= 1e9 {
		return mq.Message{}, errors.New("invalid Kafka nanoseconds")
	}
	message := mq.Message{ID: string(id), Topic: record.Topic, Key: append([]byte(nil), record.Key...), Payload: append([]byte(nil), record.Value...), Headers: headers, CreatedAt: time.Unix(int64(binary.BigEndian.Uint64(seconds[:])), int64(nsec)).UTC()}
	if err := message.Validate(); err != nil {
		return mq.Message{}, err
	}
	return message, nil
}
