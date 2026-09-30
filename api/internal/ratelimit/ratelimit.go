// Package ratelimit caps how fast one client may call the API.
//
// It uses a TOKEN BUCKET per client: a bucket holds up to Burst tokens and
// refills continuously at Rate tokens per second; each request takes one token
// or is rejected.
//
// It replaced a per-minute sliding window, which bounded the average but not
// the burst: a client could spend a whole minute's budget in a few seconds. A
// benchmark showed exactly that — a noisy tenant sent 6,000 requests in 30 s,
// built a backlog, and pushed a quiet tenant's p99 latency to ~17 s. A bucket
// can never be more than Burst requests ahead of its steady rate, whatever the
// window alignment.
//
// State is two numbers per client (tokens, last refill time), so memory is
// bounded no matter what arrives, and idle buckets expire once they would be
// full again anyway.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyPrefix namespaces rate-limit buckets in Redis.
const keyPrefix = "ratelimit:tb:"

// Decision is the outcome of one rate-limit check, carrying everything the HTTP
// layer needs to answer the caller.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the bucket size: the most requests a client may make at once.
	Limit int
	// Remaining is how many whole tokens are left, never negative.
	Remaining int
	// RetryAfter is how long until one token is available. Zero when allowed.
	RetryAfter time.Duration
	// ResetAt is when the bucket will be full again.
	ResetAt time.Time
}

// bucketScript refills and spends from one bucket atomically, using Redis's
// own clock so every API instance agrees on time.
//
// KEYS: 1 bucket hash
// ARGV: 1 rate (tokens/s), 2 burst
// Returns {allowed (0|1), tokens left, now} with numbers as strings.
var bucketScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + elapsed * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', tokens), 'ts', string.format('%.6f', now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, string.format('%.6f', tokens), string.format('%.6f', now)}
`)

// Limiter enforces a token bucket per key.
type Limiter struct {
	client *redis.Client
	rate   float64
	burst  int
}

// New constructs a Limiter that refills rate tokens per second into buckets
// holding at most burst tokens. rate must be > 0 and burst >= 1; config
// validation guarantees both.
func New(client *redis.Client, rate float64, burst int) *Limiter {
	return &Limiter{client: client, rate: rate, burst: burst}
}

// Allow takes one token from key's bucket if one is available.
func (l *Limiter) Allow(ctx context.Context, key string) (Decision, error) {
	res, err := bucketScript.Run(ctx, l.client, []string{keyPrefix + key},
		strconv.FormatFloat(l.rate, 'f', -1, 64), l.burst).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("checking rate limit for %s: %w", key, err)
	}
	if len(res) != 3 {
		return Decision{}, fmt.Errorf("checking rate limit for %s: unexpected reply %v", key, res)
	}

	allowed, _ := res[0].(int64)
	tokens, err := strconv.ParseFloat(fmt.Sprint(res[1]), 64)
	if err != nil {
		return Decision{}, fmt.Errorf("parsing tokens for %s: %w", key, err)
	}
	nowSec, err := strconv.ParseFloat(fmt.Sprint(res[2]), 64)
	if err != nil {
		return Decision{}, fmt.Errorf("parsing time for %s: %w", key, err)
	}
	now := time.Unix(0, int64(nowSec*1e9))

	d := Decision{
		Allowed:   allowed == 1,
		Limit:     l.burst,
		Remaining: max(int(math.Floor(tokens)), 0),
		ResetAt:   now.Add(secs((float64(l.burst) - tokens) / l.rate)),
	}
	if !d.Allowed {
		d.RetryAfter = secs((1 - tokens) / l.rate)
		if d.RetryAfter < time.Second {
			// Never advertise a sub-second wait: Retry-After is whole seconds,
			// and a client that honors "0" would retry immediately and simply
			// be rejected again.
			d.RetryAfter = time.Second
		}
	}
	return d, nil
}

// secs converts fractional seconds to a Duration.
func secs(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}
