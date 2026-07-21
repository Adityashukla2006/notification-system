// Package store is the hand-written persistence layer over Postgres. It owns
// all SQL for the source-of-truth tables; nothing above it writes SQL. There is
// no ORM — queries are explicit pgx calls.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Channel is the delivery channel for a notification. Values are constrained by
// a CHECK constraint in the database; the constants here are the allowed set.
type Channel string

// Allowed delivery channels.
const (
	ChannelEmail Channel = "email"
	ChannelSMS   Channel = "sms"
	ChannelPush  Channel = "push"
)

// Status is a notification's lifecycle state, constrained by a CHECK constraint
// in the database.
type Status string

// The notification lifecycle. A row starts pending, is queued for a worker,
// moves through delivering to a terminal delivered/failed, and is
// dead_lettered once retries are exhausted.
const (
	StatusPending      Status = "pending"
	StatusQueued       Status = "queued"
	StatusDelivering   Status = "delivering"
	StatusDelivered    Status = "delivered"
	StatusFailed       Status = "failed"
	StatusDeadLettered Status = "dead_lettered"
)

// defaultMaxAttempts mirrors the column default in the migration. Create
// applies it when a caller leaves MaxAttempts zero so callers do not have to
// know the number.
const defaultMaxAttempts = 5

// pgUniqueViolation is the SQLSTATE Postgres returns for a unique-constraint
// violation. We compare against the literal rather than depend on jackc/pgerrcode.
const pgUniqueViolation = "23505"

// idempotencyConstraint is the name of the UNIQUE (client_id, idempotency_key)
// constraint in the migration. Matching on the name (not just the SQLSTATE)
// ensures the idempotency branch only fires for that specific constraint, not
// some other unique index that might be added later.
const idempotencyConstraint = "notifications_client_idem_key"

// ErrNotFound is returned by the Get methods when no matching row exists.
var ErrNotFound = errors.New("notification not found")

// Notification is one row of the notifications table: an accepted request and
// its delivery state.
type Notification struct {
	ID             uuid.UUID
	ClientID       uuid.UUID
	IdempotencyKey string
	Channel        Channel
	Recipient      string
	Payload        json.RawMessage
	Status         Status
	Attempts       int
	MaxAttempts    int
	ScheduledAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Store owns the Postgres connection pool and all notification SQL.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a Store over the given pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// selectColumns is the column list shared by every SELECT, kept in one place so
// it cannot drift from the scan order in scanNotification.
const selectColumns = `
	id, client_id, idempotency_key, channel, recipient, payload,
	status, attempts, max_attempts, scheduled_at, created_at, updated_at
`

// Create inserts n and returns the stored notification with created=true. If a
// notification with the same (ClientID, IdempotencyKey) already exists, Create
// does not insert a duplicate: it returns the ORIGINAL row with created=false.
//
// This is the idempotency guarantee. It is enforced by the database's UNIQUE
// constraint, never by a read-then-insert check in Go — because two concurrent
// requests could both pass such a check and both insert. The constraint is the
// only race-free option.
func (s *Store) Create(ctx context.Context, n Notification) (result Notification, created bool, err error) {
	// Apply server-side defaults the caller may have left zero, so the store is
	// the single place that knows them.
	if n.ID == uuid.Nil {
		id, gerr := uuid.NewV7()
		if gerr != nil {
			return Notification{}, false, fmt.Errorf("generating uuidv7: %w", gerr)
		}
		n.ID = id
	}
	if n.Status == "" {
		n.Status = StatusPending
	}
	if n.MaxAttempts == 0 {
		n.MaxAttempts = defaultMaxAttempts
	}
	if n.ScheduledAt.IsZero() {
		n.ScheduledAt = time.Now()
	}

	const insert = `
INSERT INTO notifications
	(id, client_id, idempotency_key, channel, recipient, payload,
	 status, attempts, max_attempts, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING created_at, updated_at`

	err = s.pool.QueryRow(ctx, insert,
		n.ID, n.ClientID, n.IdempotencyKey, n.Channel, n.Recipient, n.Payload,
		n.Status, n.Attempts, n.MaxAttempts, n.ScheduledAt,
	).Scan(&n.CreatedAt, &n.UpdatedAt)
	if err == nil {
		return n, true, nil
	}

	// --- Idempotency branch: the entire guarantee lives here. ---
	// A unique-violation on the (client_id, idempotency_key) constraint means
	// this exact request was already accepted. Instead of surfacing an error,
	// fetch and return the original row so the caller sees a stable result no
	// matter how many times it retries.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		pgErr.Code == pgUniqueViolation &&
		pgErr.ConstraintName == idempotencyConstraint {

		existing, gerr := s.getByIdempotency(ctx, n.ClientID, n.IdempotencyKey)
		if gerr != nil {
			return Notification{}, false, fmt.Errorf("loading original after idempotency conflict: %w", gerr)
		}
		return existing, false, nil
	}

	return Notification{}, false, fmt.Errorf("inserting notification: %w", err)
}

// GetByID returns the notification with the given id, or ErrNotFound.
func (s *Store) GetByID(ctx context.Context, id uuid.UUID) (Notification, error) {
	const q = `SELECT` + selectColumns + `FROM notifications WHERE id = $1`
	n, err := scanNotification(s.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("querying notification by id: %w", err)
	}
	return n, nil
}

// getByIdempotency loads the row for a (client_id, idempotency_key) pair. It is
// used by Create's idempotency branch to return the original after a conflict.
func (s *Store) getByIdempotency(ctx context.Context, clientID uuid.UUID, key string) (Notification, error) {
	const q = `SELECT` + selectColumns + `FROM notifications WHERE client_id = $1 AND idempotency_key = $2`
	n, err := scanNotification(s.pool.QueryRow(ctx, q, clientID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("querying notification by idempotency key: %w", err)
	}
	return n, nil
}

// scanNotification scans one row in the fixed selectColumns order. Every SELECT
// funnels through here so column order and scan order stay in lockstep.
func scanNotification(row pgx.Row) (Notification, error) {
	var n Notification
	err := row.Scan(
		&n.ID, &n.ClientID, &n.IdempotencyKey, &n.Channel, &n.Recipient, &n.Payload,
		&n.Status, &n.Attempts, &n.MaxAttempts, &n.ScheduledAt, &n.CreatedAt, &n.UpdatedAt,
	)
	return n, err
}
