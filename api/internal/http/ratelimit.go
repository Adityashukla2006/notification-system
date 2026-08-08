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
