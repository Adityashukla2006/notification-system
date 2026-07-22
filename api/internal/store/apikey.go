package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Client is one tenant: the owner of notifications and API keys.
type Client struct {
	ID        uuid.UUID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// APIKey is a stored credential. It never holds the raw secret — only the hash
// of its secret half, against which a presented secret is verified.
type APIKey struct {
	ID         uuid.UUID
	ClientID   uuid.UUID
	SecretHash []byte
	Name       string
	CreatedAt  time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
	ExpiresAt  *time.Time
}

// CreateClient inserts a new tenant with a generated id.
func (s *Store) CreateClient(ctx context.Context, name string) (Client, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Client{}, fmt.Errorf("generating client id: %w", err)
	}

	const q = `
INSERT INTO clients (id, name)
VALUES ($1, $2)
RETURNING created_at, updated_at`

	c := Client{ID: id, Name: name}
	if err := s.pool.QueryRow(ctx, q, c.ID, c.Name).Scan(&c.CreatedAt, &c.UpdatedAt); err != nil {
		return Client{}, fmt.Errorf("inserting client: %w", err)
	}
	return c, nil
}

// CreateAPIKey stores a key for a client. The caller supplies the key id and
// secret hash from auth.GenerateKey; the raw secret is never passed here.
func (s *Store) CreateAPIKey(ctx context.Context, keyID, clientID uuid.UUID, secretHash []byte, name string) (APIKey, error) {
	const q = `
INSERT INTO api_keys (id, client_id, secret_hash, name)
VALUES ($1, $2, $3, $4)
RETURNING created_at`

	k := APIKey{ID: keyID, ClientID: clientID, SecretHash: secretHash, Name: name}
	if err := s.pool.QueryRow(ctx, q, k.ID, k.ClientID, k.SecretHash, k.Name).Scan(&k.CreatedAt); err != nil {
		return APIKey{}, fmt.Errorf("inserting api key: %w", err)
	}
	return k, nil
}

// GetAPIKeyByID loads a key by its public id, or ErrNotFound. Revoked and
// expired keys are still returned; the caller decides how to treat them, so the
// reason for rejection stays a policy decision in one place (the middleware).
func (s *Store) GetAPIKeyByID(ctx context.Context, keyID uuid.UUID) (APIKey, error) {
	const q = `
SELECT id, client_id, secret_hash, name, created_at, last_used_at, revoked_at, expires_at
FROM api_keys
WHERE id = $1`

	var k APIKey
	err := s.pool.QueryRow(ctx, q, keyID).Scan(
		&k.ID, &k.ClientID, &k.SecretHash, &k.Name,
		&k.CreatedAt, &k.LastUsedAt, &k.RevokedAt, &k.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, ErrNotFound
	}
	if err != nil {
		return APIKey{}, fmt.Errorf("querying api key by id: %w", err)
	}
	return k, nil
}

// TouchAPIKeyLastUsed records that a key was just used to authenticate. At
// scale this write would move off the request hot path (batched or sampled);
// for now it is a small inline update.
func (s *Store) TouchAPIKeyLastUsed(ctx context.Context, keyID uuid.UUID) error {
	const q = `UPDATE api_keys SET last_used_at = now() WHERE id = $1`
	if _, err := s.pool.Exec(ctx, q, keyID); err != nil {
		return fmt.Errorf("updating api key last_used_at: %w", err)
	}
	return nil
}
