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

// UpdateStatus sets a notification's status. It returns ErrNotFound if no row
// has the given id.
func (s *Store) UpdateStatus(ctx context.Context, id uuid.UUID, status Status) error {
	const q = `UPDATE notifications SET status = $2, updated_at = now() WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q, id, status)
	if err != nil {
		return fmt.Errorf("updating notification status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RecordFailure marks a delivery attempt as failed and decides what happens
// next, in ONE statement: it increments attempts, then either schedules a retry
// or dead-letters the row once attempts reach max_attempts. It returns the
// updated row so the caller can see which branch was taken.
//
// The decision lives in SQL rather than in Go because attempts must be read and
// written atomically. Two workers delivering the same notification (which
// at-least-once permits) would otherwise both read attempts=4, both compute
// "one left", and together grant one extra attempt past the ceiling. Doing the
// increment and the comparison in a single UPDATE makes that impossible.
//
// nextAttemptAt is written to scheduled_at, which already means "earliest time
// this may be delivered". Reusing it keeps the retry schedule recoverable from
// Postgres alone if Redis is lost.
func (s *Store) RecordFailure(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time) (Notification, error) {
	const q = `
UPDATE notifications
SET attempts     = attempts + 1,
    status       = CASE WHEN attempts + 1 >= max_attempts THEN $2::text ELSE $3::text END,
    scheduled_at = CASE WHEN attempts + 1 >= max_attempts THEN scheduled_at ELSE $4 END,
    updated_at   = now()
WHERE id = $1
RETURNING` + selectColumns

	n, err := scanNotification(s.pool.QueryRow(ctx, q, id, StatusDeadLettered, StatusFailed, nextAttemptAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("recording delivery failure: %w", err)
	}
	return n, nil
}

// ReapStuck finds notifications that should be moving but are not, marks them
// queued, and returns their ids so the caller can put them back on the queue.
//
// This is the backstop that makes "Postgres is the source of truth" real. The
// Redis reclaimer recovers claims a dead worker held, but it cannot recover
// what Redis itself never held or has lost:
//
//   - pending: persisted by the API, which then died before enqueueing.
//   - queued: enqueued, but the queue was wiped before any worker claimed it.
//   - delivering: a worker crashed mid-delivery and its processing list is
//     gone too (wiped, or the worker's id never returned).
//   - failed and due: a retry whose scheduled-set entry was lost.
//
// Terminal rows (delivered, dead_lettered) are never touched.
//
// stuckBefore is the cutoff: only rows untouched since then are swept, so a
// notification currently being worked on is not yanked out from under a live
// worker. It must exceed the longest legitimate delivery.
//
// The UPDATE is the claim. Two reapers racing cannot both take a row: the first
// to commit changes updated_at, so the second no longer matches the predicate.
// FOR UPDATE SKIP LOCKED means the loser moves on to other rows instead of
// blocking behind the winner.
func (s *Store) ReapStuck(ctx context.Context, stuckBefore time.Time, limit int) ([]uuid.UUID, error) {
	const q = `
UPDATE notifications
SET status = $3, updated_at = now()
WHERE id IN (
	SELECT id
	FROM notifications
	WHERE status = ANY($1)
	  AND scheduled_at <= now()
	  AND updated_at < $2
	ORDER BY scheduled_at
	LIMIT $4
	FOR UPDATE SKIP LOCKED
)
RETURNING id`

	reapable := []Status{StatusPending, StatusQueued, StatusDelivering, StatusFailed}

	rows, err := s.pool.Query(ctx, q, reapable, stuckBefore, StatusQueued, limit)
	if err != nil {
		return nil, fmt.Errorf("reaping stuck notifications: %w", err)
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning reaped notification id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating reaped notifications: %w", err)
	}
	return ids, nil
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
