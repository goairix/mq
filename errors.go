package mq

import "errors"

var (
	ErrBackpressure = errors.New("mq: backpressure limit reached")
	ErrClosed       = errors.New("mq: publisher closed")
	ErrNoRoute      = errors.New("mq: no route to subscription")
)

type permanentError struct{ cause error }

func (e permanentError) Error() string { return "permanent: " + e.cause.Error() }
func (e permanentError) Unwrap() error { return e.cause }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{cause: err}
}

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

type unknownOutcomeError struct{ cause error }

func (e unknownOutcomeError) Error() string { return "publish outcome unknown: " + e.cause.Error() }
func (e unknownOutcomeError) Unwrap() error { return e.cause }

func OutcomeUnknown(err error) error {
	if err == nil {
		err = errors.New("broker confirmation unavailable")
	}
	return unknownOutcomeError{cause: err}
}

func IsOutcomeUnknown(err error) bool {
	var target unknownOutcomeError
	return errors.As(err, &target)
}
