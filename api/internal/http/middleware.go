package http

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/auth"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// APIKeyLookup is the slice of the store the auth middleware needs. Depending on
// an interface (not *store.Store) lets the middleware be tested with a fake that
// returns whatever key or error a case requires, no database involved.
type APIKeyLookup interface {
	GetAPIKeyByID(ctx context.Context, keyID uuid.UUID) (store.APIKey, error)
	TouchAPIKeyLastUsed(ctx context.Context, keyID uuid.UUID) error
}

// contextKey is unexported so no other package can collide with or overwrite the
// values we store in the request context.
type contextKey int

const clientIDKey contextKey = iota

// ClientIDFrom returns the authenticated client id previously placed in the
// context by the auth middleware. ok is false if the request was not
// authenticated, which should never happen for a handler behind APIKeyAuth.
func ClientIDFrom(ctx context.Context) (id uuid.UUID, ok bool) {
	id, ok = ctx.Value(clientIDKey).(uuid.UUID)
	return id, ok
}

// APIKeyAuth authenticates requests by API key and injects the resolved client
// id into the request context. It rejects with 401 for a missing, malformed,
// unknown, revoked, expired, or mismatched key — deliberately without saying
// which, so a caller cannot probe for valid key ids.
//
// This must run before any per-client middleware (e.g. rate limiting), which
// needs the client id this resolves.
func APIKeyAuth(logger *slog.Logger, keys APIKeyLookup) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			token, ok := bearerToken(req)
			if !ok {
				unauthorized(w)
				return
			}

			keyID, secret, err := auth.ParseToken(token)
			if err != nil {
				unauthorized(w)
				return
			}

			key, err := keys.GetAPIKeyByID(req.Context(), keyID)
			if err != nil {
				// A genuine lookup failure (DB down) is not the client's fault,
				// but we still must not authenticate. Log it; return 401.
				if !errors.Is(err, store.ErrNotFound) {
					logger.Error("api key lookup failed", "error", err)
				}
				unauthorized(w)
				return
			}

			if !usable(key) || !auth.Verify(secret, key.SecretHash) {
				unauthorized(w)
				return
			}

			// Best-effort usage tracking: never fail the request over it.
			if err := keys.TouchAPIKeyLastUsed(req.Context(), keyID); err != nil {
				logger.Warn("failed to update api key last_used_at", "error", err)
			}

			ctx := context.WithValue(req.Context(), clientIDKey, key.ClientID)
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	}
}

// usable reports whether a key is currently valid to authenticate with: not
// revoked and not past expiry.
func usable(key store.APIKey) bool {
	if key.RevokedAt != nil {
		return false
	}
	if key.ExpiresAt != nil && !key.ExpiresAt.After(time.Now()) {
		return false
	}
	return true
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header, reporting ok=false if the header is absent or not a bearer scheme.
func bearerToken(req *http.Request) (token string, ok bool) {
	const prefix = "Bearer "
	h := req.Header.Get("Authorization")
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}

// unauthorized writes a uniform 401 with a WWW-Authenticate challenge and no
// detail about why authentication failed.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
}
