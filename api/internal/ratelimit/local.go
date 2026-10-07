package ratelimit

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Local is a token bucket per key held in this process's memory. It is the
// fallback for when Redis is unreachable, so it applies the same arithmetic as
// bucketScript but sees only the traffic that reaches this instance.
//
// That makes it approximate by design. Each instance is given a share of the
// global budget (see NewLocal), which is exact only when the load balancer
// spreads a tenant's requests evenly. The point is not precision — it is that
// a Redis outage degrades the limit instead of removing it.
type Local struct {
	rate  float64
	burst float64
	now   func() time.Time

	mu        sync.Mutex
	buckets   map[string]*localBucket
	lastSweep time.Time
}

type localBucket struct {
	tokens float64
	ts     time.Time
}

// NewLocal constructs a Local limiter holding this instance's share of a global
// budget of rate tokens/s and burst tokens, split across instances. The burst
// share never drops below one, or a small burst split many ways would reject
// everything.
func NewLocal(rate float64, burst, instances int) *Local {
	if instances < 1 {
		instances = 1
	}
	return &Local{
		rate:    rate / float64(instances),
		burst:   math.Max(1, math.Floor(float64(burst)/float64(instances))),
		now:     time.Now,
		buckets: make(map[string]*localBucket),
	}
}

// Allow takes one token from key's bucket if one is available. It never fails;
// the error is there to satisfy the same interface as the Redis Limiter.
func (l *Local) Allow(_ context.Context, key string) (Decision, error) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweep(now)

	b, ok := l.buckets[key]
	if !ok {
		b = &localBucket{tokens: l.burst, ts: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.ts).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
	b.ts = now

	d := Decision{Limit: int(l.burst)}
	if b.tokens >= 1 {
		b.tokens--
		d.Allowed = true
	} else {
		d.RetryAfter = max(secs((1-b.tokens)/l.rate), time.Second)
	}
	d.Remaining = max(int(math.Floor(b.tokens)), 0)
	d.ResetAt = now.Add(secs((l.burst - b.tokens) / l.rate))
	return d, nil
}

// sweep drops buckets that would be full again by now, so memory stays bounded
// by the keys active in one refill period, as the Redis buckets' TTL does.
// Must be called with mu held.
func (l *Local) sweep(now time.Time) {
	refill := secs(l.burst / l.rate)
	if now.Sub(l.lastSweep) < refill {
		return
	}
	l.lastSweep = now
	for key, b := range l.buckets {
		if now.Sub(b.ts) >= refill {
			delete(l.buckets, key)
		}
	}
}

// Allower is anything that can make a rate-limit decision.
type Allower interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// Fallback consults a primary limiter and, when it errors, answers from a
// local one instead. It replaces failing open: with Redis down, every instance
// still enforces its share of the budget rather than none at all.
type Fallback struct {
	primary  Allower
	fallback Allower
	logger   *slog.Logger
	degraded atomic.Bool
}

// NewFallback constructs a Fallback over primary and fallback.
func NewFallback(primary, fallback Allower, logger *slog.Logger) *Fallback {
	return &Fallback{primary: primary, fallback: fallback, logger: logger}
}

// Allow asks the primary limiter, falling back on error. It logs on each change
// of state rather than on every request, so an outage under load does not
// flood the logs.
func (f *Fallback) Allow(ctx context.Context, key string) (Decision, error) {
	d, err := f.primary.Allow(ctx, key)
	if err == nil {
		if f.degraded.CompareAndSwap(true, false) {
			f.logger.Info("rate limiter recovered; using shared limits again")
		}
		return d, nil
	}

	if f.degraded.CompareAndSwap(false, true) {
		f.logger.Error("rate limiter unavailable; enforcing local per-instance limits", "error", err)
	}
	return f.fallback.Allow(ctx, key)
}
