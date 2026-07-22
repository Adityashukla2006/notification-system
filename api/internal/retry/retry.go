// Package retry computes how long to wait before re-attempting a failed
// delivery, and when to stop trying altogether.
//
// It is deliberately a pure function of (attempt number, policy) with an
// injectable randomness source, so the schedule can be tested exactly rather
// than observed statistically.
package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy describes an exponential backoff schedule: base * 2^attempt, jittered,
// capped, then dead-lettered once attempts are exhausted.
type Policy struct {
	// Base is the delay before the first retry.
	Base time.Duration
	// Max caps the delay. Without a cap, 2^attempt reaches absurd values
	// within a handful of attempts.
	Max time.Duration
}

// DefaultPolicy is a reasonable starting schedule: roughly 1s, 2s, 4s, 8s...
// before jitter, never exceeding 5 minutes.
var DefaultPolicy = Policy{Base: time.Second, Max: 5 * time.Minute}

// Backoff returns how long to wait before the given attempt number, where
// attempt is the count of deliveries already made (1 after the first failure).
//
// The delay grows exponentially and is then jittered. Jitter matters more than
// it looks: without it, a provider outage that fails a thousand notifications at
// once retries all thousand in lockstep, so the recovering provider is hit by
// the same synchronized spike that is still failing. Spreading the retries is
// what lets it actually recover.
//
// This uses EQUAL jitter — half the delay fixed, half random — rather than full
// jitter, so the schedule keeps a guaranteed floor and cannot collapse back to
// retrying almost immediately.
func (p Policy) Backoff(attempt int) time.Duration {
	return p.backoff(attempt, rand.Float64)
}

// backoff is Backoff with an injectable random source for testing.
func (p Policy) backoff(attempt int, random func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := p.Base
	if base <= 0 {
		base = DefaultPolicy.Base
	}
	max := p.Max
	if max <= 0 {
		max = DefaultPolicy.Max
	}

	// Compute in float to detect overflow before it wraps: shifting an int64
	// by a large attempt number silently produces garbage, including negative
	// delays, which would schedule a retry in the past.
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) || math.IsInf(exp, 0) {
		exp = float64(max)
	}

	half := time.Duration(exp / 2)
	return half + time.Duration(random()*float64(half))
}

// Exhausted reports whether a notification has used up its attempts and should
// be dead-lettered instead of retried.
func Exhausted(attempts, maxAttempts int) bool {
	return attempts >= maxAttempts
}
