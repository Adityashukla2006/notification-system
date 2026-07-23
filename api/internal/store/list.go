package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Pagination bounds for listing. A caller-supplied limit is clamped rather than
// rejected: a page size is a hint, and no client should be able to ask for the
// whole table in one request.
const (
	DefaultListLimit = 25
	MaxListLimit     = 100
)

// ListFilter selects a page of one client's notifications.
//
// ClientID is mandatory and is never taken from user input — it comes from the
// authenticated request. Every list query is scoped to it, so one tenant cannot
// read another's notifications even by guessing ids.
type ListFilter struct {
	ClientID uuid.UUID
	// Status, when set, restricts to one lifecycle state.
	Status Status
	// Channel, when set, restricts to one delivery channel.
	Channel Channel
	// Cursor is the last id from the previous page; results continue strictly
	// after it. Nil starts at the newest.
	Cursor uuid.UUID
	// Limit is the maximum rows to return, clamped to MaxListLimit.
	Limit int
}

// List returns a page of notifications, newest first.
//
// Pagination is by cursor, never OFFSET. OFFSET makes the database walk and
// discard every skipped row, so page 500 costs 500 pages of work; worse, a row
// inserted while a client pages causes rows to shift and be seen twice or
// skipped. A cursor is a stable position: "everything older than this id",
// which costs the same on page 500 as on page 1 and cannot skip or repeat.
//
// The cursor is an id because ids are UUIDv7 — time-ordered, so id order is
// creation order and one column serves as both sort key and cursor.
func (s *Store) List(ctx context.Context, f ListFilter) ([]Notification, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}

	// Optional filters are passed as nullable parameters rather than by
	// concatenating SQL, so there is exactly one query plan and no possibility
	// of injection through a filter value.
	var status, channel *string
	if f.Status != "" {
		v := string(f.Status)
		status = &v
	}
	if f.Channel != "" {
		v := string(f.Channel)
		channel = &v
	}
	var cursor *uuid.UUID
	if f.Cursor != uuid.Nil {
		cursor = &f.Cursor
	}

	const q = `SELECT` + selectColumns + `
FROM notifications
WHERE client_id = $1
  AND ($2::text IS NULL OR status = $2)
  AND ($3::text IS NULL OR channel = $3)
  AND ($4::uuid IS NULL OR id < $4)
ORDER BY id DESC
LIMIT $5`

	rows, err := s.pool.Query(ctx, q, f.ClientID, status, channel, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("listing notifications: %w", err)
	}
	defer rows.Close()

	notifications := make([]Notification, 0, limit)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning notification: %w", err)
		}
		notifications = append(notifications, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating notifications: %w", err)
	}
	return notifications, nil
}

// GetForClient returns one of a client's notifications, or ErrNotFound.
//
// A notification belonging to a DIFFERENT client returns ErrNotFound, not a
// permission error. Distinguishing "not yours" from "does not exist" would let
// a caller probe for which ids are real, so both look identical from outside.
func (s *Store) GetForClient(ctx context.Context, clientID, id uuid.UUID) (Notification, error) {
	const q = `SELECT` + selectColumns + `FROM notifications WHERE id = $1 AND client_id = $2`

	n, err := scanNotification(s.pool.QueryRow(ctx, q, id, clientID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("querying notification for client: %w", err)
	}
	return n, nil
}
