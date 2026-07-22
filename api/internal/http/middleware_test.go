package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/auth"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// fakeKeys is an APIKeyLookup backed by an in-memory map, so middleware tests
// run without a database.
type fakeKeys struct {
	byID    map[uuid.UUID]store.APIKey
	lookErr error // forced error from GetAPIKeyByID (e.g. simulate DB down)
	touched []uuid.UUID
}

func (f *fakeKeys) GetAPIKeyByID(_ context.Context, id uuid.UUID) (store.APIKey, error) {
	if f.lookErr != nil {
		return store.APIKey{}, f.lookErr
	}
	k, ok := f.byID[id]
	if !ok {
		return store.APIKey{}, store.ErrNotFound
	}
	return k, nil
}

func (f *fakeKeys) TouchAPIKeyLastUsed(_ context.Context, id uuid.UUID) error {
	f.touched = append(f.touched, id)
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mint creates a valid key, stores it in the fake keyed by id, and returns the
// raw token to present and the owning client id.
func mint(t *testing.T, keys *fakeKeys, clientID uuid.UUID, modify func(*store.APIKey)) string {
	t.Helper()
	gen, err := auth.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	k := store.APIKey{
		ID:         gen.KeyID,
		ClientID:   clientID,
		SecretHash: gen.SecretHash,
	}
	if modify != nil {
		modify(&k)
	}
	if keys.byID == nil {
		keys.byID = map[uuid.UUID]store.APIKey{}
	}
	keys.byID[gen.KeyID] = k
	return gen.Token
}

func TestAPIKeyAuth(t *testing.T) {
	clientID := uuid.New()
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)

	tests := []struct {
		name       string
		authHeader func(keys *fakeKeys) string // builds header; may register keys
		lookErr    error
		wantStatus int
	}{
		{
			name: "valid key authenticates",
			authHeader: func(keys *fakeKeys) string {
				return "Bearer " + mint(t, keys, clientID, nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing header",
			authHeader: func(*fakeKeys) string { return "" },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong scheme",
			authHeader: func(*fakeKeys) string { return "Basic abc123" },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "malformed token",
			authHeader: func(*fakeKeys) string { return "Bearer not-a-valid-token" },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "unknown key id",
			authHeader: func(*fakeKeys) string {
				// Well-formed token whose key id is never registered.
				gen, _ := auth.GenerateKey()
				return "Bearer " + gen.Token
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong secret for known key id",
			authHeader: func(keys *fakeKeys) string {
				token := mint(t, keys, clientID, nil)
				// Tamper with the secret while keeping the real key id.
				id, _, _ := auth.ParseToken(token)
				return "Bearer notif_" + id.String() + "_deadbeefdeadbeef"
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "revoked key",
			authHeader: func(keys *fakeKeys) string {
				return "Bearer " + mint(t, keys, clientID, func(k *store.APIKey) { k.RevokedAt = &past })
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "expired key",
			authHeader: func(keys *fakeKeys) string {
				return "Bearer " + mint(t, keys, clientID, func(k *store.APIKey) { k.ExpiresAt = &past })
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "not-yet-expired key authenticates",
			authHeader: func(keys *fakeKeys) string {
				return "Bearer " + mint(t, keys, clientID, func(k *store.APIKey) { k.ExpiresAt = &future })
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "lookup error fails closed",
			authHeader: func(keys *fakeKeys) string {
				return "Bearer " + mint(t, keys, clientID, nil)
			},
			lookErr:    errors.New("db down"),
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := &fakeKeys{}
			header := tt.authHeader(keys)
			keys.lookErr = tt.lookErr

			// A handler that echoes the resolved client id, so a 200 also proves
			// the context was populated correctly.
			var gotClientID uuid.UUID
			var gotOK bool
			protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotClientID, gotOK = ClientIDFrom(r.Context())
				w.WriteHeader(http.StatusOK)
			})

			h := APIKeyAuth(discardLogger(), keys)(protected)

			req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK {
				if !gotOK {
					t.Error("client id not found in context after auth")
				}
				if gotClientID != clientID {
					t.Errorf("client id = %v, want %v", gotClientID, clientID)
				}
			}
		})
	}
}

// TestMeEndpoint checks the wired /v1/me route end to end through the router.
func TestMeEndpoint(t *testing.T) {
	keys := &fakeKeys{}
	clientID := uuid.New()
	token := mint(t, keys, clientID, nil)

	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, keys)

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body["client_id"] != clientID.String() {
		t.Errorf("client_id = %q, want %q", body["client_id"], clientID.String())
	}
}

// TestMeRequiresAuth confirms /v1/me is not reachable without a key.
func TestMeRequiresAuth(t *testing.T) {
	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, &fakeKeys{})

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
