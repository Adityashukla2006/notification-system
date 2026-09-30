package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Adityashukla2006/notification-system/api/internal/testenv"
)

// requireRedis returns a client for the test Redis, or skips when none is
// configured via TEST_REDIS_ADDR.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := testenv.RequireOrSkip(t, "TEST_REDIS_ADDR",
		"Rate limit tests need a real Redis, e.g. localhost:6379.")
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("pinging redis at %s: %v", addr, err)
	}
	return client
}

// testKey returns a key unique to one test, so tests never share a bucket.
func testKey() string {
	return "test-" + uuid.NewString()
}

// allowN calls Allow n times and returns the decisions.
func allowN(t *testing.T, l *Limiter, key string, n int) []Decision {
	t.Helper()
	out := make([]Decision, 0, n)
	for i := range n {
		d, err := l.Allow(context.Background(), key)
		if err != nil {
			t.Fatalf("Allow #%d: %v", i+1, err)
		}
		out = append(out, d)
	}
	return out
}

// TestBurstThenReject is the core bucket behaviour: a fresh client may spend
// exactly Burst requests at once, then is rejected. The rate is tiny so no
// refill happens during the test.
func TestBurstThenReject(t *testing.T) {
	l := New(requireRedis(t), 0.001, 3)
	ds := allowN(t, l, testKey(), 4)

	for i, d := range ds[:3] {
		if !d.Allowed {
			t.Fatalf("request #%d denied, want allowed within a burst of 3", i+1)
		}
		if want := 2 - i; d.Remaining != want {
			t.Errorf("request #%d remaining = %d, want %d", i+1, d.Remaining, want)
		}
		if d.Limit != 3 {
			t.Errorf("request #%d limit = %d, want 3", i+1, d.Limit)
		}
	}
	if ds[3].Allowed {
		t.Fatal("request #4 allowed, want rejected once the burst is spent")
	}
	if ds[3].RetryAfter < time.Second {
		t.Errorf("RetryAfter = %v, want at least 1s", ds[3].RetryAfter)
	}
	if !ds[3].ResetAt.After(time.Now()) {
		t.Errorf("ResetAt = %v, want a time in the future", ds[3].ResetAt)
	}
}

// TestBurstIsBounded is the reason this replaced a per-minute window: however
// long a client has been idle, it can never get more than Burst requests
// through at once. A per-minute window with the same average (here 600/min)
// would have admitted all 50.
func TestBurstIsBounded(t *testing.T) {
	l := New(requireRedis(t), 10, 5)
	ds := allowN(t, l, testKey(), 50)

	allowed := 0
	for _, d := range ds {
		if d.Allowed {
			allowed++
		}
	}
	// 5 from the full bucket, plus at most a token or two refilled at 10/s
	// during the few milliseconds the 50 calls take.
	if allowed < 5 || allowed > 7 {
		t.Errorf("allowed %d of 50 back-to-back requests, want 5 (+ at most 2 refilled)", allowed)
	}
}

// TestTokensRefillAtRate confirms the limit is temporary and follows the
// configured rate.
func TestTokensRefillAtRate(t *testing.T) {
	l := New(requireRedis(t), 20, 1) // one token every 50 ms
	key := testKey()

	ds := allowN(t, l, key, 2)
	if !ds[0].Allowed || ds[1].Allowed {
		t.Fatalf("got allowed=%v,%v, want true,false", ds[0].Allowed, ds[1].Allowed)
	}

	time.Sleep(120 * time.Millisecond) // > 2 refill intervals

	if d := allowN(t, l, key, 1)[0]; !d.Allowed {
		t.Error("request after refill denied, want allowed")
	}
}

// TestLimitsAreIsolatedPerKey guards the multi-tenant property: one client
// emptying its bucket must not affect another.
func TestLimitsAreIsolatedPerKey(t *testing.T) {
	l := New(requireRedis(t), 0.001, 2)
	noisy, quiet := testKey(), testKey()

	allowN(t, l, noisy, 5)

	if d := allowN(t, l, quiet, 1)[0]; !d.Allowed {
		t.Error("quiet client denied after a noisy client emptied its own bucket")
	}
}

// TestBucketsExpire keeps Redis from holding a bucket per client forever: an
// idle bucket is dropped once it would be full again anyway.
func TestBucketsExpire(t *testing.T) {
	client := requireRedis(t)
	l := New(client, 10, 5) // full again after 0.5 s, so expiry ~1.5 s
	key := testKey()
	allowN(t, l, key, 1)

	ttl, err := client.PTTL(context.Background(), keyPrefix+key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 || ttl > 2*time.Second {
		t.Errorf("bucket TTL = %v, want set and at most ~1.5s", ttl)
	}
}
