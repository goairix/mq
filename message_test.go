package mq

import (
	"strings"
	"testing"
	"time"
)

func TestNewMessage(t *testing.T) {
	payload := []byte("order-created")
	first, err := NewMessage("order.created", payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewMessage("order.created", payload)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || first.ID == second.ID {
		t.Fatalf("IDs are not distinct: %q %q", first.ID, second.ID)
	}
	if first.Topic != "order.created" || string(first.Payload) != "order-created" {
		t.Fatalf("unexpected message: %+v", first)
	}
	if first.CreatedAt.IsZero() || time.Since(first.CreatedAt) > time.Minute {
		t.Fatalf("invalid creation time: %v", first.CreatedAt)
	}
}

func TestMessageValidation(t *testing.T) {
	valid, err := NewMessage("order.created", nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*Message)
	}{
		{"missing ID", func(m *Message) { m.ID = "" }},
		{"missing topic", func(m *Message) { m.Topic = "" }},
		{"missing creation time", func(m *Message) { m.CreatedAt = time.Time{} }},
		{"reserved header", func(m *Message) { m.Headers = map[string]string{"MQ.internal": "x"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := valid
			tc.mutate(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewMessage("  ", nil); err == nil || !strings.Contains(err.Error(), "topic") {
		t.Fatalf("expected topic error, got %v", err)
	}
}

func TestMessageSizeBytes(t *testing.T) {
	m := Message{ID: "a", Topic: "b", Key: []byte{1, 2}, Payload: []byte{3, 4, 5}, Headers: map[string]string{"x": "y"}, CreatedAt: time.Now()}
	if got := m.SizeBytes(); got != 17 {
		t.Fatalf("size = %d, want 17", got)
	}
}
