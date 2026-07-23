package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// requireRedis returns a client for the test Redis, or skips when none is
// configured via TEST_REDIS_ADDR.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run rate limit tests against a real Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("pinging redis at %s: %v", addr, err)
	}
	return client
}

// testKey returns a key unique to one test, so tests never share a counter.
func testKey() string {
	return "test-" + uuid.NewString()
}

func TestAllowUnderLimit(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	l := New(client, 5, time.Minute)
	key := testKey()

	for i := 1; i <= 5; i++ {
		d, err := l.Allow(ctx, key)
		if err != nil {
			t.Fatalf("Allow #%d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request #%d denied, want allowed within a limit of 5", i)
		}
		if want := 5 - i; d.Remaining != want {
			t.Errorf("request #%d remaining = %d, want %d", i, d.Remaining, want)
		}
	}
}

func TestAllowRejectsOverLimit(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	l := New(client, 3, time.Minute)
	key := testKey()

	for i := 1; i <= 3; i++ {
		if d, err := l.Allow(ctx, key); err != nil || !d.Allowed {
			t.Fatalf("request #%d: allowed=%v err=%v, want allowed", i, d.Allowed, err)
		}
	}

	d, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("fourth request allowed, want denied at a limit of 3")
	}
	if d.Remaining != 0 {
		t.Errorf("remaining = %d, want 0", d.Remaining)
	}
	if d.RetryAfter < time.Second {
		t.Errorf("retry after = %v, want at least a second", d.RetryAfter)
	}
	if d.ResetAt.Before(time.Now()) {
		t.Errorf("reset at %v is in the past", d.ResetAt)
	}
}

// TestLimitsAreIsolatedPerKey guards the multi-tenant property: one client
// exhausting its budget must not affect another.
func TestLimitsAreIsolatedPerKey(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	l := New(client, 2, time.Minute)
	noisy, quiet := testKey(), testKey()

	for range 3 {
		if _, err := l.Allow(ctx, noisy); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}
	if d, _ := l.Allow(ctx, noisy); d.Allowed {
		t.Error("noisy key still allowed, want denied")
	}

	d, err := l.Allow(ctx, quiet)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !d.Allowed {
		t.Error("quiet key denied because another key was limited")
	}
}

// TestSlidingWindowPreventsBoundaryBurst is the reason this is a sliding window
// rather than a fixed one.
//
// With fixed windows, a client can spend its entire budget at the end of one
// window and its entire budget again at the start of the next — double the
// intended rate across the boundary. Here the previous window's usage is
// weighted in, so entering a new window does not hand back a full budget.
func TestSlidingWindowPreventsBoundaryBurst(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	const window = 2 * time.Second
	const limit = 4
	key := testKey()

	l := New(client, limit, window)

	// Pin the clock near the very end of a window, then exhaust the budget.
	windowNS := window.Nanoseconds()
	base := time.Now()
	windowStart := time.Unix(0, (base.UnixNano()/windowNS)*windowNS)
	nearEnd := windowStart.Add(window - 100*time.Millisecond)

	l.now = func() time.Time { return nearEnd }
	for i := 1; i <= limit; i++ {
		if d, err := l.Allow(ctx, key); err != nil || !d.Allowed {
			t.Fatalf("setup request #%d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}

	// Step just past the boundary. A fixed window would reset to a full budget
	// here; the weighted previous window must still deny.
	justAfter := windowStart.Add(window + 100*time.Millisecond)
	l.now = func() time.Time { return justAfter }

	d, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if d.Allowed {
		t.Error("request allowed immediately after the window boundary; " +
			"the previous window's usage was not carried over")
	}
}

// TestBudgetRecoversAfterAWholeWindow confirms the limit is temporary: once the
// previous window has fully aged out, the client is served again.
func TestBudgetRecoversAfterAWholeWindow(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	const window = 2 * time.Second
	key := testKey()
	l := New(client, 2, window)

	base := time.Now()
	l.now = func() time.Time { return base }
	for range 3 {
		if _, err := l.Allow(ctx, key); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}
	if d, _ := l.Allow(ctx, key); d.Allowed {
		t.Fatal("client was not limited during setup")
	}

	// Two full windows later, nothing from the burst remains in scope.
	later := base.Add(2 * window)
	l.now = func() time.Time { return later }

	d, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !d.Allowed {
		t.Error("still limited two windows after the burst, want the budget restored")
	}
}

// TestCountersExpire keeps Redis from accumulating a key per client forever.
func TestCountersExpire(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	const window = time.Second
	key := testKey()
	l := New(client, 10, window)

	if _, err := l.Allow(ctx, key); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	keys, err := client.Keys(ctx, keyPrefix+key+":*").Result()
	if err != nil {
		t.Fatalf("listing keys: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("no counter key was written")
	}

	ttl, err := client.TTL(ctx, keys[0]).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("counter TTL = %v, want a positive expiry so keys do not leak", ttl)
	}
	// Two windows: the previous bucket is still needed while the current runs.
	if ttl > 2*window {
		t.Errorf("counter TTL = %v, want at most two windows (%v)", ttl, 2*window)
	}
}
