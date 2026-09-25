package mq

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Message is an immutable-by-convention transport envelope. Keep its slices and
// map unchanged until Publish returns or a handler returns.
type Message struct {
	ID        string
	Topic     string
	Key       []byte
	Payload   []byte
	Headers   map[string]string
	CreatedAt time.Time
}

func NewMessage(topic string, payload []byte) (Message, error) {
	if strings.TrimSpace(topic) == "" {
		return Message{}, errors.New("topic is required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Message{}, fmt.Errorf("generate message ID: %w", err)
	}
	return Message{ID: hex.EncodeToString(id[:]), Topic: topic, Payload: payload, CreatedAt: time.Now().UTC()}, nil
}

func (m Message) Validate() error {
	if m.ID == "" {
		return errors.New("message ID is required")
	}
	if strings.TrimSpace(m.Topic) == "" {
		return errors.New("topic is required")
	}
	if m.CreatedAt.IsZero() {
		return errors.New("creation time is required")
	}
	for key := range m.Headers {
		if strings.HasPrefix(strings.ToLower(key), "mq.") {
			return fmt.Errorf("header %q uses reserved mq. prefix", key)
		}
	}
	return nil
}
