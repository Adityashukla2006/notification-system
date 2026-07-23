package store

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

// seedWithState inserts a notification and forces it into a given status and
// staleness, which is how a stranded row looks in production.
func seedWithState(t *testing.T, s *Store, status Status, updatedAt, scheduledAt time.Time) Notification {
	t.Helper()
	ctx := context.Background()

	n, _, err := s.Create(ctx, Notification{
		ClientID:       seedClient(t, s),
		IdempotencyKey: "reap-" + uuid.NewString(),
		Channel:        ChannelEmail,
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"body":"hi"}`),
	})
	if err != nil {
		t.Fatalf("seeding notification: %v", err)
	}

	// Set status, staleness, and due time directly: there is no legitimate API
	// for "pretend this has been untouched for an hour".
	if _, err := s.pool.Exec(ctx,
		`UPDATE notifications SET status = $2, updated_at = $3, scheduled_at = $4 WHERE id = $1`,
		n.ID, status, updatedAt, scheduledAt,
	); err != nil {
		t.Fatalf("forcing notification state: %v", err)
	}
	n.Status = status
	return n
}

func TestReapStuck(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	stale := time.Now().Add(-time.Hour)
	fresh := time.Now()
	due := time.Now().Add(-time.Minute)
	notDue := time.Now().Add(time.Hour)
	cutoff := time.Now().Add(-30 * time.Minute)

	tests := []struct {
		name      string
		status    Status
		updatedAt time.Time
		scheduled time.Time
		wantReap  bool
	}{
		// Recoverable: Redis either never held these or lost them.
		{name: "pending and stale is reaped", status: StatusPending, updatedAt: stale, scheduled: due, wantReap: true},
		{name: "queued and stale is reaped", status: StatusQueued, updatedAt: stale, scheduled: due, wantReap: true},
		{name: "delivering and stale is reaped", status: StatusDelivering, updatedAt: stale, scheduled: due, wantReap: true},
		{name: "failed, due, and stale is reaped", status: StatusFailed, updatedAt: stale, scheduled: due, wantReap: true},

		// Not recoverable, and must be left alone.
		{name: "delivered is never reaped", status: StatusDelivered, updatedAt: stale, scheduled: due},
		{name: "dead lettered is never reaped", status: StatusDeadLettered, updatedAt: stale, scheduled: due},
		{name: "recently touched is left alone", status: StatusDelivering, updatedAt: fresh, scheduled: due},
		{name: "failed but not yet due is left alone", status: StatusFailed, updatedAt: stale, scheduled: notDue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := seedWithState(t, s, tt.status, tt.updatedAt, tt.scheduled)

			ids, err := s.ReapStuck(ctx, cutoff, 100)
			if err != nil {
				t.Fatalf("ReapStuck: %v", err)
			}

			reaped := false
			for _, id := range ids {
				if id == n.ID {
					reaped = true
				}
			}
			if reaped != tt.wantReap {
				t.Errorf("reaped = %v, want %v", reaped, tt.wantReap)
			}

			if tt.wantReap {
				got, err := s.GetByID(ctx, n.ID)
				if err != nil {
					t.Fatalf("GetByID: %v", err)
				}
				if got.Status != StatusQueued {
					t.Errorf("status after reap = %s, want %s", got.Status, StatusQueued)
				}
			}
		})
	}
}

// TestReapStuckIsIdempotent verifies the UPDATE acts as the claim: because the
// sweep refreshes updated_at, an immediate second sweep finds nothing, so two
// reapers racing cannot both recover the same row.
func TestReapStuckIsIdempotent(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	n := seedWithState(t, s, StatusDelivering, time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	cutoff := time.Now().Add(-30 * time.Minute)

	first, err := s.ReapStuck(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("ReapStuck (first): %v", err)
	}
	if !containsID(first, n.ID) {
		t.Fatalf("first sweep did not reap %s", n.ID)
	}

	second, err := s.ReapStuck(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("ReapStuck (second): %v", err)
	}
	if containsID(second, n.ID) {
		t.Errorf("second sweep reaped %s again, want the refreshed updated_at to exclude it", n.ID)
	}
}

// TestReapStuckRespectsLimit checks a large backlog is drained in batches.
func TestReapStuckRespectsLimit(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	stale := time.Now().Add(-time.Hour)
	due := time.Now().Add(-time.Minute)
	for range 3 {
		seedWithState(t, s, StatusQueued, stale, due)
	}

	ids, err := s.ReapStuck(ctx, time.Now().Add(-30*time.Minute), 2)
	if err != nil {
		t.Fatalf("ReapStuck: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("reaped %d, want 2 (the limit)", len(ids))
	}
}

// TestReapStuckEmptyReturnsEmptySlice covers the ordinary case where nothing is
// stranded.
func TestReapStuckEmptyReturnsEmptySlice(t *testing.T) {
	s := requireStore(t)

	// A cutoff far in the past matches nothing.
	ids, err := s.ReapStuck(context.Background(), time.Now().Add(-100*time.Hour), 100)
	if err != nil {
		t.Fatalf("ReapStuck: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("reaped %d, want 0", len(ids))
	}
	if ids == nil {
		t.Error("ReapStuck returned nil, want an empty slice")
	}
}

func containsID(ids []uuid.UUID, want uuid.UUID) bool {
	return slices.Contains(ids, want)
}
