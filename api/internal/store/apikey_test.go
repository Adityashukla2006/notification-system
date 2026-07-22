package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/auth"
)

func TestCreateClientAndAPIKey(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	client, err := s.CreateClient(ctx, "acme")
	if err != nil {
		t.Fatalf("CreateClient: %v", err)
	}
	if client.ID == uuid.Nil {
		t.Error("client id not populated")
	}
	if client.CreatedAt.IsZero() {
		t.Error("created_at not populated")
	}

	gen, err := auth.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	key, err := s.CreateAPIKey(ctx, gen.KeyID, client.ID, gen.SecretHash, "prod")
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if key.ID != gen.KeyID {
		t.Errorf("key id = %v, want %v", key.ID, gen.KeyID)
	}

	got, err := s.GetAPIKeyByID(ctx, gen.KeyID)
	if err != nil {
		t.Fatalf("GetAPIKeyByID: %v", err)
	}
	if got.ClientID != client.ID {
		t.Errorf("client id = %v, want %v", got.ClientID, client.ID)
	}
	if !auth.Verify(mustSecret(t, gen.Token), got.SecretHash) {
		t.Error("stored hash does not verify against the generated secret")
	}
	if got.RevokedAt != nil || got.ExpiresAt != nil || got.LastUsedAt != nil {
		t.Error("expected nullable timestamps to be nil on a fresh key")
	}
}

func TestGetAPIKeyByIDNotFound(t *testing.T) {
	s := requireStore(t)

	_, err := s.GetAPIKeyByID(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestCreateAPIKeyRequiresRealClient(t *testing.T) {
	s := requireStore(t)
	gen, _ := auth.GenerateKey()

	// The foreign key must reject a key for a non-existent client.
	_, err := s.CreateAPIKey(context.Background(), gen.KeyID, uuid.New(), gen.SecretHash, "orphan")
	if err == nil {
		t.Fatal("CreateAPIKey for unknown client = nil error, want FK violation")
	}
}

func TestTouchAPIKeyLastUsed(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	client, err := s.CreateClient(ctx, "acme")
	if err != nil {
		t.Fatalf("CreateClient: %v", err)
	}
	gen, _ := auth.GenerateKey()
	if _, err := s.CreateAPIKey(ctx, gen.KeyID, client.ID, gen.SecretHash, ""); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	if err := s.TouchAPIKeyLastUsed(ctx, gen.KeyID); err != nil {
		t.Fatalf("TouchAPIKeyLastUsed: %v", err)
	}

	got, err := s.GetAPIKeyByID(ctx, gen.KeyID)
	if err != nil {
		t.Fatalf("GetAPIKeyByID: %v", err)
	}
	if got.LastUsedAt == nil {
		t.Error("last_used_at still nil after touch")
	}
}

// mustSecret extracts the secret half from a generated token for verification.
func mustSecret(t *testing.T, token string) string {
	t.Helper()
	_, secret, err := auth.ParseToken(token)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	return secret
}
