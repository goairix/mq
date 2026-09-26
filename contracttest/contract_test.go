package contracttest

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/goairix/mq/v2"
)

func TestInvalidResultRequiresHandlerAndNonCancellationError(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		handled bool
		want    bool
	}{
		{"missing handler", errors.New("invalid result"), false, false},
		{"deadline", context.DeadlineExceeded, true, false},
		{"canceled", context.Canceled, true, false},
		{"wrapped deadline", errors.Join(errors.New("read"), context.DeadlineExceeded), true, false},
		{"invalid result", errors.New("invalid result length"), true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validInvalidResult(tc.err, tc.handled); got != tc.want {
				t.Fatalf("got %t, want %t", got, tc.want)
			}
		})
	}
}

func TestAllSuccessRejectsNonAcknowledgingFixture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	probe := func(context.Context, mq.Subscription) (int64, error) { return 2, nil }
	if err := waitOutstandingZero(ctx, probe, mq.Subscription{Topic: "broken", Name: "group"}); err == nil {
		t.Fatal("non-acknowledging fixture was accepted")
	}
}
