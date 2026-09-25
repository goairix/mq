package mq

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanentError(t *testing.T) {
	cause := errors.New("bad schema")
	wrapped := fmt.Errorf("handler: %w", Permanent(cause))
	if !IsPermanent(wrapped) {
		t.Fatal("permanent classification lost")
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("cause lost")
	}
	if IsPermanent(cause) || Permanent(nil) != nil {
		t.Fatal("incorrect permanent classification")
	}
}

func TestUnknownOutcome(t *testing.T) {
	cause := errors.New("confirmation timed out")
	wrapped := fmt.Errorf("publish: %w", OutcomeUnknown(cause))
	if !IsOutcomeUnknown(wrapped) {
		t.Fatal("unknown outcome classification lost")
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("cause lost")
	}
	if IsOutcomeUnknown(cause) {
		t.Fatal("plain error was misclassified")
	}
}
