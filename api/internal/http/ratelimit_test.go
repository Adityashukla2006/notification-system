package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/ratelimit"
)

// allowAllLimiter permits everything. Tests of other handlers use it so rate
// limiting never interferes with what they are actually asserting.
type allowAllLimiter struct{}

func (allowAllLimiter) Allow(context.Context, string) (ratelimit.Decision, error) {
	return ratelimit.Decision{
		Allowed:   true,
		Limit:     100,
		Remaining: 99,
		ResetAt:   time.Now().Add(time.Minute),
	}, nil
}

// scriptedLimiter returns a fixed decision and records the key it was given.
type scriptedLimiter struct {
	decision ratelimit.Decision
	err      error
	keys     []string
}

func (s *scriptedLimiter) Allow(_ context.Context, key string) (ratelimit.Decision, error) {
	s.keys = append(s.keys, key)
	if s.err != nil {
		return ratelimit.Decision{}, s.err
	}
	return s.decision, nil
}

// limitedRequest issues an authenticated request through a router using limiter.
func limitedRequest(t *testing.T, limiter RateLimiter, clientID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	keys := &fakeKeys{}
	token := mint(t, keys, clientID, nil)
	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, keys, &fakeCreator{}, newFakeReader(), limiter)

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestRateLimitAllowsAndAdvertisesBudget(t *testing.T) {
	reset := time.Now().Add(30 * time.Second)
	limiter := &scriptedLimiter{decision: ratelimit.Decision{
		Allowed: true, Limit: 100, Remaining: 42, ResetAt: reset,
	}}

	rec := limitedRequest(t, limiter, uuid.New())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// Headers appear on successful responses too, so a client can slow down
	// before it is cut off rather than discovering the limit by hitting it.
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "100" {
		t.Errorf("X-RateLimit-Limit = %q, want 100", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "42" {
		t.Errorf("X-RateLimit-Remaining = %q, want 42", got)
	}
	if got := rec.Header().Get("X-RateLimit-Reset"); got != strconv.FormatInt(reset.Unix(), 10) {
		t.Errorf("X-RateLimit-Reset = %q, want %d", got, reset.Unix())
	}
}

func TestRateLimitRejectsWithRetryAfter(t *testing.T) {
	limiter := &scriptedLimiter{decision: ratelimit.Decision{
		Allowed:    false,
		Limit:      100,
		Remaining:  0,
		RetryAfter: 2500 * time.Millisecond,
		ResetAt:    time.Now().Add(2500 * time.Millisecond),
	}}

	rec := limitedRequest(t, limiter, uuid.New())
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	// Rounded UP: 2 seconds would invite a retry that is still too early.
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want 3 (2.5s rounded up)", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

// TestRateLimitKeysByAuthenticatedClient pins the identity used for limiting:
// keying on anything a caller controls would let it rotate around the limit.
func TestRateLimitKeysByAuthenticatedClient(t *testing.T) {
	limiter := &scriptedLimiter{decision: ratelimit.Decision{Allowed: true, Limit: 100, Remaining: 99}}
	clientID := uuid.New()

	if rec := limitedRequest(t, limiter, clientID); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(limiter.keys) != 1 {
		t.Fatalf("limiter consulted %d times, want 1", len(limiter.keys))
	}
	if limiter.keys[0] != clientID.String() {
		t.Errorf("limited on key %q, want the authenticated client %q", limiter.keys[0], clientID)
	}
}

// TestRateLimitFailsOpen documents a deliberate availability trade: a broken
// limiter must not take the API down with it.
func TestRateLimitFailsOpen(t *testing.T) {
	limiter := &scriptedLimiter{err: errors.New("redis down")}

	rec := limitedRequest(t, limiter, uuid.New())
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200: a limiter outage should not reject traffic", rec.Code)
	}
}

// TestRateLimitRunsAfterAuthentication verifies an unauthenticated request is
// rejected before the limiter is consulted, so anonymous traffic cannot consume
// another client's budget or an unkeyed shared one.
func TestRateLimitRunsAfterAuthentication(t *testing.T) {
	limiter := &scriptedLimiter{decision: ratelimit.Decision{Allowed: true}}
	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, &fakeKeys{}, &fakeCreator{}, newFakeReader(), limiter)

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if len(limiter.keys) != 0 {
		t.Errorf("limiter was consulted %d times for an unauthenticated request, want 0", len(limiter.keys))
	}
}

// TestHealthEndpointsAreNotRateLimited keeps probes working under load: a
// limited liveness probe would get the process restarted exactly when it is
// busiest.
func TestHealthEndpointsAreNotRateLimited(t *testing.T) {
	limiter := &scriptedLimiter{decision: ratelimit.Decision{Allowed: false, RetryAfter: time.Minute}}
	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, &fakeKeys{}, &fakeCreator{}, newFakeReader(), limiter)

	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code == http.StatusTooManyRequests {
				t.Errorf("%s was rate limited; probes must always be answerable", path)
			}
		})
	}
	if len(limiter.keys) != 0 {
		t.Errorf("limiter consulted %d times for health endpoints, want 0", len(limiter.keys))
	}
}
