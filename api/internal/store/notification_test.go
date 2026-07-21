package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool is the shared pool for the package. It is nil when TEST_DATABASE_URL
// is unset, in which case every test skips rather than fails.
var testPool *pgxpool.Pool

// TestMain connects to the test database (if configured) and applies the
// migration files before running the suite, so tests exercise the real schema —
// including its CHECK and UNIQUE constraints — not a hand-maintained copy.
func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		os.Exit(m.Run())
	}

	pool, err := setupTestDB(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store test setup failed: %v\n", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// setupTestDB connects and applies the down then up migration, giving a clean
// schema regardless of prior state.
func setupTestDB(url string) (*pgxpool.Pool, error) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging: %w", err)
	}

	_, thisFile, _, _ := runtime.Caller(0)
	migDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations")
	for _, name := range []string{
		"000001_create_notifications.down.sql",
		"000001_create_notifications.up.sql",
	} {
		sql, rerr := os.ReadFile(filepath.Join(migDir, name))
		if rerr != nil {
			return nil, fmt.Errorf("reading %s: %w", name, rerr)
		}
		if _, eerr := pool.Exec(ctx, string(sql)); eerr != nil {
			return nil, fmt.Errorf("applying %s: %w", name, eerr)
		}
	}
	return pool, nil
}

// requireStore skips the test when no database is configured, and otherwise
// returns a Store over a freshly truncated table.
func requireStore(t *testing.T) *Store {
	t.Helper()
	if testPool == nil {
		t.Skip("set TEST_DATABASE_URL to run store tests against a real Postgres")
	}
	if _, err := testPool.Exec(context.Background(), "TRUNCATE notifications"); err != nil {
		t.Fatalf("truncating: %v", err)
	}
	return New(testPool)
}

// equalJSON reports whether two JSON documents are semantically equal,
// ignoring formatting and key order.
func equalJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal a: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal b: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}

// validNotification returns a complete, valid notification for tests to mutate.
func validNotification() Notification {
	return Notification{
		ClientID:       uuid.New(),
		IdempotencyKey: "key-" + uuid.NewString(),
		Channel:        ChannelEmail,
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"subject":"hello"}`),
	}
}

func TestCreate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(n *Notification)
		wantErr bool
	}{
		{
			name:   "valid email",
			mutate: func(*Notification) {},
		},
		{
			name:   "valid sms",
			mutate: func(n *Notification) { n.Channel = ChannelSMS; n.Recipient = "+15551234567" },
		},
		{
			name:    "invalid channel rejected by CHECK",
			mutate:  func(n *Notification) { n.Channel = "carrier-pigeon" },
			wantErr: true,
		},
		{
			name:    "invalid status rejected by CHECK",
			mutate:  func(n *Notification) { n.Status = "teleported" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := requireStore(t)
			n := validNotification()
			tt.mutate(&n)

			got, created, err := s.Create(context.Background(), n)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Create() = nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Create() unexpected error: %v", err)
			}
			if !created {
				t.Error("created = false, want true for a fresh insert")
			}
			if got.ID == uuid.Nil {
				t.Error("ID not populated")
			}
			if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
				t.Error("timestamps not populated from RETURNING")
			}
		})
	}
}

func TestCreateAppliesDefaults(t *testing.T) {
	s := requireStore(t)

	// Leave ID, Status, MaxAttempts, and ScheduledAt zero.
	n := validNotification()
	before := time.Now()

	got, created, err := s.Create(context.Background(), n)
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if got.ID == uuid.Nil {
		t.Error("ID default not applied")
	}
	if got.Status != StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, StatusPending)
	}
	if got.MaxAttempts != defaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", got.MaxAttempts, defaultMaxAttempts)
	}
	if got.ScheduledAt.Before(before.Add(-time.Second)) {
		t.Errorf("ScheduledAt = %v, want ~now", got.ScheduledAt)
	}
}

// TestCreateIdempotent is the duplicate-key path: the entire idempotency
// guarantee. A second Create with the same (client_id, idempotency_key) must
// return the ORIGINAL row, not insert a second one and not error.
func TestCreateIdempotent(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	first := validNotification()
	original, created, err := s.Create(ctx, first)
	if err != nil {
		t.Fatalf("first Create() error: %v", err)
	}
	if !created {
		t.Fatal("first created = false, want true")
	}

	// Same client + key, but deliberately different contents. The store must
	// ignore these and return the original, proving it did not overwrite.
	second := validNotification()
	second.ClientID = first.ClientID
	second.IdempotencyKey = first.IdempotencyKey
	second.Recipient = "someone-else@example.com"
	second.Payload = json.RawMessage(`{"subject":"different"}`)

	got, created, err := s.Create(ctx, second)
	if err != nil {
		t.Fatalf("second Create() error: %v", err)
	}
	if created {
		t.Error("created = true on duplicate, want false")
	}
	if got.ID != original.ID {
		t.Errorf("returned ID = %v, want original %v", got.ID, original.ID)
	}
	if got.Recipient != first.Recipient {
		t.Errorf("Recipient = %q, want original %q (duplicate must not overwrite)", got.Recipient, first.Recipient)
	}

	// And there must be exactly one row for that pair.
	var count int
	if err := testPool.QueryRow(ctx,
		"SELECT count(*) FROM notifications WHERE client_id = $1 AND idempotency_key = $2",
		first.ClientID, first.IdempotencyKey,
	).Scan(&count); err != nil {
		t.Fatalf("count query error: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1", count)
	}
}

func TestGetByID(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	created, _, err := s.Create(ctx, validNotification())
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	got, err := s.GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetByID() error: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("ID = %v, want %v", got.ID, created.ID)
	}
	if got.Channel != created.Channel {
		t.Errorf("Channel = %q, want %q", got.Channel, created.Channel)
	}
	// jsonb stores a parsed value, not the original text, so it does not
	// round-trip byte-for-byte (canonical spacing, key order). Compare the
	// decoded JSON, not the raw bytes.
	if !equalJSON(t, got.Payload, created.Payload) {
		t.Errorf("Payload = %s, want equivalent JSON %s", got.Payload, created.Payload)
	}
}

func TestGetByIDNotFound(t *testing.T) {
	s := requireStore(t)

	_, err := s.GetByID(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}
