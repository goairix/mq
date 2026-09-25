package mq

import (
	"errors"
	"testing"
	"time"
)

func TestSubscriptionValidation(t *testing.T) {
	if err := (Subscription{Topic: "order.created", Name: "billing"}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []Subscription{{Name: "billing"}, {Topic: "order.created"}} {
		if err := sub.Validate(); err == nil {
			t.Fatalf("accepted invalid subscription: %+v", sub)
		}
	}
}

func TestBatchOptionsValidation(t *testing.T) {
	options := DefaultBatchOptions()
	if err := options.Validate(); err != nil {
		t.Fatal(err)
	}
	options.MaxMessages = 0
	if err := options.Validate(); err == nil {
		t.Fatal("accepted zero batch size")
	}
	options = DefaultBatchOptions()
	options.MaxInFlightBytes = options.MaxBytes - 1
	if err := options.Validate(); err == nil {
		t.Fatal("accepted too small in-flight limit")
	}
	options = DefaultBatchOptions()
	options.MaxWait = -time.Second
	if err := options.Validate(); err == nil {
		t.Fatal("accepted negative wait")
	}
}

func TestBatchResultValidation(t *testing.T) {
	if err := ValidatePublishResults(2, []PublishResult{{State: PublishAccepted}, {State: PublishUnknown, Err: OutcomeUnknown(nil)}}); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePublishResults(2, []PublishResult{{State: PublishAccepted}}); err == nil {
		t.Fatal("accepted wrong result count")
	}
	if err := ValidatePublishResults(1, []PublishResult{{State: PublishUnknown}}); err == nil {
		t.Fatal("accepted unexplained unknown outcome")
	}
	cases := []struct {
		name   string
		result PublishResult
		valid  bool
	}{
		{"unknown state", PublishResult{State: PublishState(255), Err: errors.New("bad")}, false},
		{"accepted with error", PublishResult{State: PublishAccepted, Err: errors.New("bad")}, false},
		{"rejected without error", PublishResult{State: PublishRejected}, false},
		{"unknown without error", PublishResult{State: PublishUnknown}, false},
		{"rejected with error", PublishResult{State: PublishRejected, Err: errors.New("full")}, true},
		{"unknown with error", PublishResult{State: PublishUnknown, Err: OutcomeUnknown(nil)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePublishResults(1, []PublishResult{tc.result})
			if (err == nil) != tc.valid {
				t.Fatalf("valid = %v, error = %v", tc.valid, err)
			}
		})
	}
}
