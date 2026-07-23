package http

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Adityashukla2006/notification-system/api/internal/ratelimit"
)

// RateLimiter is the capability the middleware needs. Taking an interface keeps
// the middleware testable without Redis.
type RateLimiter interface {
	Allow(ctx context.Context, key string) (ratelimit.Decision, error)
}

// RateLimit caps how fast one client may call the API.
//
// It must run AFTER authentication, because the limit is per client: keying on
// IP instead would punish everyone behind a shared NAT together and would be
// trivially sidestepped by rotating addresses. The authenticated client id is
// the identity that actually matters.
//
// On a limiter failure this FAILS OPEN — the request proceeds. That is a
// deliberate trade: rate limiting protects against load, but it is not the
// system's security boundary (authentication is), so a Redis outage should not
// take the whole API down with it. The cost is that an attacker who can break
// Redis also escapes rate limiting, which is why the failure is logged loudly
// rather than silently ignored.
func RateLimit(logger *slog.Logger, limiter RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			clientID, ok := ClientIDFrom(req.Context())
			if !ok {
				// Unreachable behind APIKeyAuth, but fail closed rather than
				// apply an unkeyed limit shared by every caller.
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}

			decision, err := limiter.Allow(req.Context(), clientID.String())
			if err != nil {
				logger.Error("rate limiter unavailable; allowing request",
					"client_id", clientID, "error", err)
				next.ServeHTTP(w, req)
				return
			}

			// Advertise the budget on every response, not just rejections, so a
			// well-behaved client can slow down before being cut off.
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(decision.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(decision.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(decision.ResetAt.Unix(), 10))

			if !decision.Allowed {
				// Retry-After is seconds, rounded up: rounding down would invite
				// a retry that is still too early.
				seconds := int(decision.RetryAfter.Seconds())
				if decision.RetryAfter%1e9 != 0 {
					seconds++
				}
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				writeJSON(w, http.StatusTooManyRequests, map[string]string{
					"error": "rate limit exceeded",
				})
				return
			}

			next.ServeHTTP(w, req)
		})
	}
}
