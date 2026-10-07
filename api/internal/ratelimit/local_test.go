package ratelimit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// fakeClock is a settable clock for the local limiter.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestLocal returns a Local limiter driven by a fake clock.
func newTestLocal(rate float64, burst, instances int) (*Local, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := NewLocal(rate, burst, instances)
	l.now = clock.now
	return l, clock
}

// allowedCount calls Allow n times on key and counts how many were allowed.
func allowedCount(t *testing.T, l Allower, key string, n int) int {
	t.Helper()
	allowed := 0
	for range n {
		d, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if d.Allowed {
			allowed++
		}
	}
	return allowed
}

func TestLocalSplitsBudgetAcrossInstances(t *testing.T) {
	tests := []struct {
		name      string
		rate      float64
		burst     int
		instances int
		wantBurst int
	}{
		{name: "single instance keeps whole burst", rate: 1, burst: 20, instances: 1, wantBurst: 20},
		{name: "three instances share burst", rate: 1, burst: 20, instances: 3, wantBurst: 6},
		{name: "burst share never drops below one", rate: 1, burst: 2, instances: 10, wantBurst: 1},
		{name: "invalid instance count treated as one", rate: 1, burst: 5, instances: 0, wantBurst: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, _ := newTestLocal(tt.rate, tt.burst, tt.instances)
			if got := allowedCount(t, l, "k", tt.wantBurst+5); got != tt.wantBurst {
				t.Errorf("allowed %d requests at once, want %d", got, tt.wantBurst)
			}
		})
	}
}

func TestLocalRefillsAtSharedRate(t *testing.T) {
	// Global 3 tokens/s over 3 instances is 1 token/s here.
	l, clock := newTestLocal(3, 3, 3)

	if got := allowedCount(t, l, "k", 2); got != 1 {
		t.Fatalf("allowed %d of initial burst, want 1", got)
	}

	clock.advance(500 * time.Millisecond)
	if got := allowedCount(t, l, "k", 1); got != 0 {
		t.Errorf("allowed after half a token refilled, want rejection")
	}

	clock.advance(500 * time.Millisecond)
	if got := allowedCount(t, l, "k", 1); got != 1 {
		t.Errorf("rejected after a full token refilled, want allowed")
	}
}

func TestLocalRejectionCarriesRetryAfter(t *testing.T) {
	l, _ := newTestLocal(0.1, 1, 1)
	allowedCount(t, l, "k", 1)

	d, _ := l.Allow(context.Background(), "k")
	if d.Allowed {
		t.Fatal("second request allowed, want rejection")
	}
	if d.RetryAfter != 10*time.Second {
		t.Errorf("RetryAfter = %v, want 10s for one token at 0.1/s", d.RetryAfter)
	}
	if d.Remaining != 0 {
		t.Errorf("Remaining = %d, want 0", d.Remaining)
	}
}

func TestLocalKeysAreIndependent(t *testing.T) {
	l, _ := newTestLocal(1, 2, 1)
	allowedCount(t, l, "noisy", 10)

	if got := allowedCount(t, l, "quiet", 2); got != 2 {
		t.Errorf("quiet key allowed %d, want 2: one key's usage must not drain another's", got)
	}
}

func TestLocalSweepsIdleBuckets(t *testing.T) {
	l, clock := newTestLocal(1, 2, 1)
	allowedCount(t, l, "a", 1)
	allowedCount(t, l, "b", 1)

	// Two seconds refills a burst of two at 1/s, so both buckets are full
	// again and indistinguishable from absent ones.
	clock.advance(2 * time.Second)
	allowedCount(t, l, "c", 1)

	if _, ok := l.buckets["a"]; ok {
		t.Error("idle bucket a was not swept")
	}
	if _, ok := l.buckets["c"]; !ok {
		t.Error("active bucket c was swept")
	}
}

// stubAllower returns a fixed decision or error and counts calls.
type stubAllower struct {
	decision Decision
	err      error
	calls    int
}

func (s *stubAllower) Allow(context.Context, string) (Decision, error) {
	s.calls++
	return s.decision, s.err
}

func TestFallback(t *testing.T) {
	tests := []struct {
		name          string
		primaryErr    error
		wantAllowed   bool
		wantFallbacks int
	}{
		{name: "primary healthy uses primary", primaryErr: nil, wantAllowed: true, wantFallbacks: 0},
		{name: "primary failing uses fallback", primaryErr: errors.New("redis down"), wantAllowed: false, wantFallbacks: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			primary := &stubAllower{decision: Decision{Allowed: true}, err: tt.primaryErr}
			fallback := &stubAllower{decision: Decision{Allowed: false}}
			f := NewFallback(primary, fallback, slog.New(slog.NewTextHandler(io.Discard, nil)))

			d, err := f.Allow(context.Background(), "k")
			if err != nil {
				t.Fatalf("Allow returned error %v; the fallback must absorb primary failures", err)
			}
			if d.Allowed != tt.wantAllowed {
				t.Errorf("Allowed = %v, want %v", d.Allowed, tt.wantAllowed)
			}
			if fallback.calls != tt.wantFallbacks {
				t.Errorf("fallback consulted %d times, want %d", fallback.calls, tt.wantFallbacks)
			}
		})
	}
}

// TestFallbackEnforcesLimitDuringOutage is the point of the feature: with the
// primary down, requests past the local budget are rejected, not let through.
func TestFallbackEnforcesLimitDuringOutage(t *testing.T) {
	primary := &stubAllower{err: errors.New("redis down")}
	local, _ := newTestLocal(1, 3, 1)
	f := NewFallback(primary, local, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if got := allowedCount(t, f, "k", 10); got != 3 {
		t.Errorf("allowed %d during outage, want 3 (the local burst)", got)
	}
}
