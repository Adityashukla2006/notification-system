package provider

import (
	"errors"
	"fmt"
)

// PermanentError marks a failure that retrying cannot fix.
//
// The distinction matters more than it looks. A mailbox that does not exist
// fails identically on every attempt, so retrying it five times with backoff
// wastes a worker, delays real work behind it, and ends in the same
// dead-letter it would have reached immediately. Worse, repeatedly pushing
// known-bad addresses at a provider is exactly the behavior that damages
// sender reputation.
//
// Transient failures — a timeout, a refused connection, a 4xx greylisting —
// are the opposite: the same request may well succeed in ten seconds. Those
// are the ones backoff exists for, and they stay unwrapped.
//
// When in doubt, do NOT mark an error permanent. Treating a transient failure
// as permanent silently drops a message that would have been delivered;
// treating a permanent failure as transient merely costs a few retries.
type PermanentError struct {
	Err error
}

// Error implements error.
func (e PermanentError) Error() string {
	return fmt.Sprintf("permanent: %v", e.Err)
}

// Unwrap exposes the underlying error to errors.Is and errors.As.
func (e PermanentError) Unwrap() error {
	return e.Err
}

// Permanent marks err as not worth retrying. It returns nil for a nil error so
// it can wrap a call's result directly.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return PermanentError{Err: err}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var pe PermanentError
	return errors.As(err, &pe)
}
