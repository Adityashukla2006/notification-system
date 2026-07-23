// Package ratelimit caps how fast one client may call the API.
//
// It uses a SLIDING WINDOW COUNTER: two fixed buckets, with the previous one
// weighted by how far the current window has progressed.
//
// The obvious alternative, a plain fixed window, is cheaper but has a flaw that
// matters. With a limit of 100 per minute, a client can send 100 requests at
// 11:59:59 and 100 more at 12:00:00 — 200 in one second, twice the intended
// rate, precisely at the boundary an attacker would target. Weighting the
// previous bucket smooths that edge away.
//
// The other alternative, a true sliding window, stores a timestamp per request
// and is exact — but its memory grows with traffic, which is the wrong thing to
// scale with the load you are trying to survive. Two counters per client per
// window is bounded no matter what arrives.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyPrefix namespaces rate-limit counters in Redis.
const keyPrefix = "ratelimit:"

// Decision is the outcome of one rate-limit check, carrying everything the HTTP
// layer needs to answer the caller.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the configured ceiling per window.
	Limit int
	// Remaining is how many requests are left, never negative.
	Remaining int
	// RetryAfter is how long to wait before trying again. Zero when allowed.
	RetryAfter time.Duration
	// ResetAt is when the current window ends.
	ResetAt time.Time
}

// Limiter enforces a request ceiling per key per window.
type Limiter struct {
	client *redis.Client
	limit  int
	window time.Duration
	now    func() time.Time
}

// New constructs a Limiter allowing limit requests per window.
func New(client *redis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{client: client, limit: limit, window: window, now: time.Now}
}

// Allow records a request against key and reports whether it may proceed.
//
// The request is counted BEFORE the decision, so a client that keeps hammering
// while limited keeps its own counter high. That is deliberate: a rejected
// request still costs the server work, and not counting it would let a client
// stay permanently at the ceiling by ignoring 429s.
func (l *Limiter) Allow(ctx context.Context, key string) (Decision, error) {
	now := l.now()

	// Bucket boundaries are derived from the clock rather than stored, so no
	// coordination is needed: every process computes the same buckets.
	windowNS := l.window.Nanoseconds()
	currentStart := now.UnixNano() / windowNS
	elapsed := time.Duration(now.UnixNano() % windowNS)

	currentKey := fmt.Sprintf("%s%s:%d", keyPrefix, key, currentStart)
	previousKey := fmt.Sprintf("%s%s:%d", keyPrefix, key, currentStart-1)

	// One round trip for all three operations. The TTL is two windows because
	// the previous bucket is still needed while the current one runs.
	pipe := l.client.Pipeline()
	incr := pipe.Incr(ctx, currentKey)
	pipe.Expire(ctx, currentKey, 2*l.window)
	prev := pipe.Get(ctx, previousKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		// redis.Nil here only means the previous bucket does not exist, which
		// is the normal case for a client's first window.
		return Decision{}, fmt.Errorf("checking rate limit for %s: %w", key, err)
	}

	currentCount := incr.Val()
	previousCount, _ := prev.Int64() // absent previous bucket reads as zero

	// Weight the previous window by the portion of it still inside the sliding
	// window: at 25% into the current window, 75% of the previous one counts.
	weight := 1 - (float64(elapsed) / float64(l.window))
	estimated := float64(previousCount)*weight + float64(currentCount)

	resetAt := time.Unix(0, (currentStart+1)*windowNS)

	decision := Decision{
		Limit:     l.limit,
		Allowed:   estimated <= float64(l.limit),
		Remaining: 0,
		ResetAt:   resetAt,
	}
	decision.Remaining = max(l.limit-int(estimated), 0)
	if !decision.Allowed {
		decision.RetryAfter = resetAt.Sub(now)
		if decision.RetryAfter < time.Second {
			// Never advertise a sub-second wait: a client that honors it would
			// retry immediately and simply be rejected again.
			decision.RetryAfter = time.Second
		}
	}
	return decision, nil
}
