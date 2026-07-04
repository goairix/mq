package serializer

import (
	"encoding/json"
	"fmt"

	"github.com/goairix/mq/message"
	"github.com/vmihailenco/msgpack/v5"
)

// Serializer 序列化器接口
type Serializer interface {
	Serialize(msg *message.Message) ([]byte, error)
	Deserialize(data []byte, msg *message.Message) error
	ContentType() string
}

// JSONSerializer JSON序列化器
type JSONSerializer struct{}

func (s *JSONSerializer) Serialize(msg *message.Message) ([]byte, error) {
	return json.Marshal(msg)
}

func (s *JSONSerializer) Deserialize(data []byte, msg *message.Message) error {
	return json.Unmarshal(data, msg)
}

func (s *JSONSerializer) ContentType() string {
	return "application/json"
}

// MsgPackSerializer MessagePack序列化器（更高效）
type MsgPackSerializer struct{}

func (s *MsgPackSerializer) Serialize(msg *message.Message) ([]byte, error) {
	return msgpack.Marshal(msg)
}

func (s *MsgPackSerializer) Deserialize(data []byte, msg *message.Message) error {
	return msgpack.Unmarshal(data, msg)
}

func (s *MsgPackSerializer) ContentType() string {
	return "application/msgpack"
}

// Type 序列化器类型
type Type string

const (
	Json    Type = "json"
	MsgPack Type = "msgpack"
)

// NewSerializer 创建序列化器
func NewSerializer(serializerType Type) (Serializer, error) {
	switch serializerType {
	case Json:
		return &JSONSerializer{}, nil
	case MsgPack:
		return &MsgPackSerializer{}, nil
	default:
		return nil, fmt.Errorf("unsupported serializer type: %s", serializerType)
	}
}
