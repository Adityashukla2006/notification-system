package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestDeadLetterIncrementsAttempts is the regression guard for a bug
// integration testing surfaced: a permanently-failed notification reported zero
// attempts while its delivery_attempts history showed one, because the
// permanent path bypassed the only statement that incremented the counter.
func TestDeadLetterIncrementsAttempts(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedForClient(t, s, seedClient(t, s), StatusDelivering, ChannelEmail)

	updated, err := s.DeadLetter(ctx, n.ID)
	if err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if updated.Status != StatusDeadLettered {
		t.Errorf("status = %s, want %s", updated.Status, StatusDeadLettered)
	}
	if updated.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", updated.Attempts)
	}

	// And it is durable, not just returned.
	reloaded, err := s.GetByID(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if reloaded.Attempts != 1 || reloaded.Status != StatusDeadLettered {
		t.Errorf("reloaded attempts=%d status=%s, want 1 and %s",
			reloaded.Attempts, reloaded.Status, StatusDeadLettered)
	}
}

// TestDeadLetterFromAPriorAttemptCount confirms the increment builds on
// whatever retries were already spent.
func TestDeadLetterFromAPriorAttemptCount(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedForClient(t, s, seedClient(t, s), StatusDelivering, ChannelEmail)

	// Spend two attempts through the ordinary retry path first.
	for range 2 {
		if _, err := s.RecordFailure(ctx, n.ID, time.Now().Add(time.Minute)); err != nil {
			t.Fatalf("RecordFailure: %v", err)
		}
	}

	updated, err := s.DeadLetter(ctx, n.ID)
	if err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if updated.Attempts != 3 {
		t.Errorf("attempts = %d, want 3 (two retries plus this one)", updated.Attempts)
	}
}

func TestDeadLetterUnknownID(t *testing.T) {
	s := requireStore(t)

	if _, err := s.DeadLetter(context.Background(), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeadLetter(unknown) = %v, want ErrNotFound", err)
	}
}
