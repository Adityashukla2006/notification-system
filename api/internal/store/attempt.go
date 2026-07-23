package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AttemptOutcome is the result of a single delivery attempt, constrained by a
// CHECK constraint in the database.
type AttemptOutcome string

// The possible outcomes of one attempt.
const (
	AttemptSucceeded AttemptOutcome = "succeeded"
	AttemptFailed    AttemptOutcome = "failed"
)

// Attempt is one row of delivery_attempts: a single call to a provider and what
// came back.
type Attempt struct {
	ID             uuid.UUID
	NotificationID uuid.UUID
	AttemptNumber  int
	Outcome        AttemptOutcome
	Error          string
	StartedAt      time.Time
	FinishedAt     time.Time
	CreatedAt      time.Time
}

// Duration is how long the provider call took.
func (a Attempt) Duration() time.Duration {
	return a.FinishedAt.Sub(a.StartedAt)
}

// attemptColumns is the column list shared by every attempt SELECT, kept in one
// place so it cannot drift from the scan order in scanAttempt.
const attemptColumns = `
	id, notification_id, attempt_number, outcome, error,
	started_at, finished_at, created_at
`

// RecordAttempt appends one attempt to a notification's history.
//
// Callers should treat a failure here as a visibility problem, not a delivery
// problem: the notification's authoritative state lives on its own row, so a
// missing history entry must never stop or change a delivery.
func (s *Store) RecordAttempt(ctx context.Context, a Attempt) (Attempt, error) {
	if a.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return Attempt{}, fmt.Errorf("generating uuidv7: %w", err)
		}
		a.ID = id
	}

	const insert = `
INSERT INTO delivery_attempts
	(id, notification_id, attempt_number, outcome, error, started_at, finished_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING created_at`

	err := s.pool.QueryRow(ctx, insert,
		a.ID, a.NotificationID, a.AttemptNumber, a.Outcome, a.Error, a.StartedAt, a.FinishedAt,
	).Scan(&a.CreatedAt)
	if err != nil {
		return Attempt{}, fmt.Errorf("recording delivery attempt: %w", err)
	}
	return a, nil
}

// ListAttempts returns a notification's attempts, newest first. It returns an
// empty slice, not ErrNotFound, when a notification has no attempts yet —
// "never attempted" is a normal state, not a missing row.
func (s *Store) ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]Attempt, error) {
	const q = `SELECT` + attemptColumns + `
FROM delivery_attempts
WHERE notification_id = $1
ORDER BY started_at DESC`

	rows, err := s.pool.Query(ctx, q, notificationID)
	if err != nil {
		return nil, fmt.Errorf("querying delivery attempts: %w", err)
	}
	defer rows.Close()

	attempts := make([]Attempt, 0)
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning delivery attempt: %w", err)
		}
		attempts = append(attempts, a)
	}
	// rows.Err reports an error that ended iteration early; without this check a
	// partial result would be indistinguishable from a complete one.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating delivery attempts: %w", err)
	}
	return attempts, nil
}

// scanAttempt scans one row in the fixed attemptColumns order.
func scanAttempt(row interface {
	Scan(dest ...any) error
}) (Attempt, error) {
	var a Attempt
	err := row.Scan(
		&a.ID, &a.NotificationID, &a.AttemptNumber, &a.Outcome, &a.Error,
		&a.StartedAt, &a.FinishedAt, &a.CreatedAt,
	)
	return a, err
}
