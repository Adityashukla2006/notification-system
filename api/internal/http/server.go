// Package http builds the API's HTTP surface: the router and the operational
// endpoints. Handlers here never call providers directly; the server only
// validates, coordinates, and reports health.
package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Pinger reports whether a backing dependency is reachable. Both the Postgres
// pool and the Redis client satisfy this, and tests can supply fakes. Taking
// an interface (rather than the concrete types) is what lets readiness logic
// be tested without a live Postgres or Redis.
type Pinger interface {
	Ping(ctx context.Context) error
}

// readinessTimeout bounds how long a single /readyz check waits on a
// dependency. It must be short: readiness is polled frequently, and a slow
// check should read as "not ready" rather than hang the probe.
const readinessTimeout = 2 * time.Second

// Router constructs the API's HTTP handler. postgres and redis are pinged by
// the readiness probe; keys backs API-key authentication for protected routes;
// notifications accepts ingestion requests; logger is used for request logging.
func Router(logger *slog.Logger, postgres, redis Pinger, keys APIKeyLookup, notifications NotificationCreator) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	// Operational endpoints are public: probes cannot present credentials.
	r.Get("/healthz", handleLiveness())
	r.Get("/readyz", handleReadiness(postgres, redis))

	// Everything under /v1 requires a valid API key.
	r.Route("/v1", func(r chi.Router) {
		r.Use(APIKeyAuth(logger, keys))
		r.Get("/me", handleMe())
		r.Post("/notifications", handleCreateNotification(notifications))
	})

	return r
}

// handleMe returns the authenticated client's id. It exists to exercise the
// full authentication path end to end; real resource endpoints build on the
// same client id resolved from the context.
func handleMe() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID, ok := ClientIDFrom(req.Context())
		if !ok {
			// Unreachable behind APIKeyAuth, but fail closed rather than serve
			// an empty identity.
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"client_id": clientID.String()})
	}
}

// handleLiveness answers "is this process alive and able to serve?" It
// deliberately touches no dependencies: a liveness failure tells the
// orchestrator to restart this process, and restarting cannot fix a Postgres
// or Redis outage. As long as the HTTP server can respond, we are live.
func handleLiveness() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// handleReadiness answers "should this instance receive traffic right now?" It
// pings each dependency; if any is unreachable it returns 503 so the load
// balancer stops routing here until the dependency recovers — without
// restarting the process.
func handleReadiness(postgres, redis Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), readinessTimeout)
		defer cancel()

		checks := map[string]string{
			"postgres": "ok",
			"redis":    "ok",
		}
		ready := true

		if err := postgres.Ping(ctx); err != nil {
			checks["postgres"] = err.Error()
			ready = false
		}
		if err := redis.Ping(ctx); err != nil {
			checks["redis"] = err.Error()
			ready = false
		}

		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, checks)
	}
}

// writeJSON writes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
