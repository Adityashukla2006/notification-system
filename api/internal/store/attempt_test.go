package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// seedNotificationForAttempts inserts a notification (and its client) so
// attempts have a valid foreign key to point at.
func seedNotificationForAttempts(t *testing.T, s *Store) Notification {
	t.Helper()
	ctx := context.Background()

	clientID := seedClient(t, s)
	n, _, err := s.Create(ctx, Notification{
		ClientID:       clientID,
		IdempotencyKey: "attempt-" + uuid.NewString(),
		Channel:        ChannelEmail,
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"body":"hi"}`),
	})
	if err != nil {
		t.Fatalf("seeding notification: %v", err)
	}
	return n
}

func TestRecordAttemptAndList(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedNotificationForAttempts(t, s)
	start := time.Now().Truncate(time.Millisecond)

	first, err := s.RecordAttempt(ctx, Attempt{
		NotificationID: n.ID,
		AttemptNumber:  1,
		Outcome:        AttemptFailed,
		Error:          "smtp refused",
		StartedAt:      start,
		FinishedAt:     start.Add(150 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}
	if first.ID == uuid.Nil {
		t.Error("RecordAttempt returned a nil id, want a generated one")
	}
	if first.CreatedAt.IsZero() {
		t.Error("RecordAttempt returned a zero created_at")
	}
	if got := first.Duration(); got != 150*time.Millisecond {
		t.Errorf("Duration() = %v, want 150ms", got)
	}

	second, err := s.RecordAttempt(ctx, Attempt{
		NotificationID: n.ID,
		AttemptNumber:  2,
		Outcome:        AttemptSucceeded,
		StartedAt:      start.Add(time.Second),
		FinishedAt:     start.Add(time.Second + 50*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("RecordAttempt (second): %v", err)
	}

	attempts, err := s.ListAttempts(ctx, n.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("listed %d attempts, want 2", len(attempts))
	}
	// Newest first.
	if attempts[0].ID != second.ID {
		t.Errorf("first listed attempt = %s, want the newest (%s)", attempts[0].ID, second.ID)
	}
	if attempts[0].Outcome != AttemptSucceeded {
		t.Errorf("newest outcome = %s, want %s", attempts[0].Outcome, AttemptSucceeded)
	}
	if attempts[1].Error != "smtp refused" {
		t.Errorf("oldest error = %q, want %q", attempts[1].Error, "smtp refused")
	}
}

// TestListAttemptsEmpty covers the "never attempted" case, which is normal and
// must not look like a missing notification.
func TestListAttemptsEmpty(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedNotificationForAttempts(t, s)
	attempts, err := s.ListAttempts(ctx, n.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("listed %d attempts, want 0", len(attempts))
	}
	if attempts == nil {
		t.Error("ListAttempts returned nil, want an empty slice")
	}
}

// TestRecordAttemptAllowsDuplicateAttemptNumbers is deliberate: at-least-once
// delivery permits the same notification to be claimed twice, and both physical
// attempts must be visible rather than one being rejected.
func TestRecordAttemptAllowsDuplicateAttemptNumbers(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedNotificationForAttempts(t, s)
	now := time.Now()

	for range 2 {
		if _, err := s.RecordAttempt(ctx, Attempt{
			NotificationID: n.ID,
			AttemptNumber:  1,
			Outcome:        AttemptFailed,
			Error:          "timeout",
			StartedAt:      now,
			FinishedAt:     now.Add(time.Second),
		}); err != nil {
			t.Fatalf("RecordAttempt: %v", err)
		}
	}

	attempts, err := s.ListAttempts(ctx, n.ID)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Errorf("listed %d attempts, want 2 duplicate-numbered rows", len(attempts))
	}
}

// TestRecordAttemptRejectsUnknownOutcome verifies the CHECK constraint is doing
// its job, so a typo cannot silently enter the history.
func TestRecordAttemptRejectsUnknownOutcome(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedNotificationForAttempts(t, s)
	now := time.Now()

	_, err := s.RecordAttempt(ctx, Attempt{
		NotificationID: n.ID,
		AttemptNumber:  1,
		Outcome:        AttemptOutcome("maybe"),
		StartedAt:      now,
		FinishedAt:     now,
	})
	if err == nil {
		t.Fatal("RecordAttempt with an invalid outcome succeeded, want a constraint violation")
	}
}

// TestRecordAttemptRequiresRealNotification verifies the foreign key, so
// history cannot accumulate against notifications that do not exist.
func TestRecordAttemptRequiresRealNotification(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	now := time.Now()
	_, err := s.RecordAttempt(ctx, Attempt{
		NotificationID: uuid.New(),
		AttemptNumber:  1,
		Outcome:        AttemptSucceeded,
		StartedAt:      now,
		FinishedAt:     now,
	})
	if err == nil {
		t.Fatal("RecordAttempt against an unknown notification succeeded, want a foreign-key violation")
	}
}

// TestAttemptsCascadeOnNotificationDelete confirms history does not outlive the
// notification it describes.
func TestAttemptsCascadeOnNotificationDelete(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedNotificationForAttempts(t, s)
	now := time.Now()
	if _, err := s.RecordAttempt(ctx, Attempt{
		NotificationID: n.ID,
		AttemptNumber:  1,
		Outcome:        AttemptSucceeded,
		StartedAt:      now,
		FinishedAt:     now,
	}); err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}

	if _, err := s.pool.Exec(ctx, `DELETE FROM notifications WHERE id = $1`, n.ID); err != nil {
		t.Fatalf("deleting notification: %v", err)
	}

	attempts, err := s.ListAttempts(ctx, n.ID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(attempts) != 0 {
		t.Errorf("listed %d attempts after the notification was deleted, want 0", len(attempts))
	}
}
