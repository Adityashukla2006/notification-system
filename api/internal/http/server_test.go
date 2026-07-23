package http

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakePinger is a Pinger whose result is fixed, letting readiness tests run
// without a live Postgres or Redis.
type fakePinger struct {
	err error
}

func (f fakePinger) Ping(context.Context) error { return f.err }

func newTestServer() http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return Router(logger, fakePinger{}, fakePinger{}, &fakeKeys{}, &fakeCreator{}, newFakeReader())
}

func TestLiveness(t *testing.T) {
	// Liveness must succeed even when both dependencies are down, because it
	// must not depend on them.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	down := fakePinger{err: errors.New("boom")}
	handler := Router(logger, down, down, &fakeKeys{}, &fakeCreator{}, newFakeReader())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestReadiness(t *testing.T) {
	errBoom := errors.New("connection refused")

	tests := []struct {
		name     string
		postgres Pinger
		redis    Pinger
		wantCode int
	}{
		{
			name:     "both up",
			postgres: fakePinger{},
			redis:    fakePinger{},
			wantCode: http.StatusOK,
		},
		{
			name:     "postgres down",
			postgres: fakePinger{err: errBoom},
			redis:    fakePinger{},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:     "redis down",
			postgres: fakePinger{},
			redis:    fakePinger{err: errBoom},
			wantCode: http.StatusServiceUnavailable,
		},
		{
			name:     "both down",
			postgres: fakePinger{err: errBoom},
			redis:    fakePinger{err: errBoom},
			wantCode: http.StatusServiceUnavailable,
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := Router(logger, tt.postgres, tt.redis, &fakeKeys{}, &fakeCreator{}, newFakeReader())

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("GET /readyz = %d, want %d", rec.Code, tt.wantCode)
			}
		})
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	handler := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
