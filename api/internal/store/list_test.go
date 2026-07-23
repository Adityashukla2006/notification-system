package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// seedForClient inserts a notification owned by clientID in a given state.
func seedForClient(t *testing.T, s *Store, clientID uuid.UUID, status Status, channel Channel) Notification {
	t.Helper()
	ctx := context.Background()

	n, _, err := s.Create(ctx, Notification{
		ClientID:       clientID,
		IdempotencyKey: "list-" + uuid.NewString(),
		Channel:        channel,
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"body":"hi"}`),
	})
	if err != nil {
		t.Fatalf("seeding notification: %v", err)
	}
	if status != StatusPending {
		if err := s.UpdateStatus(ctx, n.ID, status); err != nil {
			t.Fatalf("setting status: %v", err)
		}
		n.Status = status
	}
	// UUIDv7 embeds a millisecond timestamp; a gap keeps creation order
	// unambiguous, which is what the cursor relies on.
	time.Sleep(2 * time.Millisecond)
	return n
}

// TestListIsScopedByClient is the multi-tenancy guarantee at the storage layer:
// no filter combination may return another client's rows.
func TestListIsScopedByClient(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	mine := seedClient(t, s)
	theirs := seedClient(t, s)

	seedForClient(t, s, mine, StatusDelivered, ChannelEmail)
	seedForClient(t, s, mine, StatusFailed, ChannelSMS)
	seedForClient(t, s, theirs, StatusDelivered, ChannelEmail)

	got, err := s.List(ctx, ListFilter{ClientID: mine})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("listed %d notifications, want 2", len(got))
	}
	for _, n := range got {
		if n.ClientID != mine {
			t.Errorf("listed a notification owned by %s, want only %s", n.ClientID, mine)
		}
	}
}

func TestListFilters(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	clientID := seedClient(t, s)
	seedForClient(t, s, clientID, StatusDelivered, ChannelEmail)
	seedForClient(t, s, clientID, StatusFailed, ChannelSMS)
	seedForClient(t, s, clientID, StatusFailed, ChannelEmail)

	tests := []struct {
		name    string
		status  Status
		channel Channel
		want    int
	}{
		{name: "no filter", want: 3},
		{name: "status only", status: StatusFailed, want: 2},
		{name: "channel only", channel: ChannelEmail, want: 2},
		{name: "status and channel", status: StatusFailed, channel: ChannelEmail, want: 1},
		{name: "no matches", status: StatusDeadLettered, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.List(ctx, ListFilter{ClientID: clientID, Status: tt.status, Channel: tt.channel})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(got) != tt.want {
				t.Errorf("listed %d, want %d", len(got), tt.want)
			}
		})
	}
}

// TestListOrdersNewestFirst pins the ordering the cursor depends on.
func TestListOrdersNewestFirst(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	clientID := seedClient(t, s)
	first := seedForClient(t, s, clientID, StatusPending, ChannelEmail)
	second := seedForClient(t, s, clientID, StatusPending, ChannelEmail)
	third := seedForClient(t, s, clientID, StatusPending, ChannelEmail)

	got, err := s.List(ctx, ListFilter{ClientID: clientID})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []uuid.UUID{third.ID, second.ID, first.ID}
	if len(got) != len(want) {
		t.Fatalf("listed %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Errorf("position %d = %s, want %s", i, got[i].ID, want[i])
		}
	}
}

// TestListCursorWalksEveryRowExactlyOnce is the property that makes cursor
// pagination worth its complexity: no row repeated, none skipped.
func TestListCursorWalksEveryRowExactlyOnce(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	clientID := seedClient(t, s)
	const total = 5
	for range total {
		seedForClient(t, s, clientID, StatusPending, ChannelEmail)
	}

	seen := map[uuid.UUID]bool{}
	cursor := uuid.Nil
	for range 10 {
		got, err := s.List(ctx, ListFilter{ClientID: clientID, Cursor: cursor, Limit: 2})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) == 0 {
			break
		}
		for _, n := range got {
			if seen[n.ID] {
				t.Errorf("notification %s appeared on two pages", n.ID)
			}
			seen[n.ID] = true
		}
		cursor = got[len(got)-1].ID
	}

	if len(seen) != total {
		t.Errorf("walked %d notifications, want %d", len(seen), total)
	}
}

func TestListLimitIsClamped(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	clientID := seedClient(t, s)
	for range 3 {
		seedForClient(t, s, clientID, StatusPending, ChannelEmail)
	}

	tests := []struct {
		name  string
		limit int
		want  int
	}{
		{name: "explicit limit is honored", limit: 2, want: 2},
		{name: "zero falls back to the default", limit: 0, want: 3},
		{name: "negative falls back to the default", limit: -10, want: 3},
		{name: "absurd limit is clamped, not rejected", limit: 10_000, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.List(ctx, ListFilter{ClientID: clientID, Limit: tt.limit})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(got) != tt.want {
				t.Errorf("listed %d, want %d", len(got), tt.want)
			}
		})
	}
}

func TestListEmptyReturnsEmptySlice(t *testing.T) {
	s := requireStore(t)

	got, err := s.List(context.Background(), ListFilter{ClientID: uuid.New()})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("listed %d, want 0", len(got))
	}
	if got == nil {
		t.Error("List returned nil, want an empty slice")
	}
}

// TestGetForClient checks that ownership is enforced in the query, and that a
// row belonging to someone else is reported exactly like a missing one.
func TestGetForClient(t *testing.T) {
	s := requireStore(t)
	ctx := context.Background()

	mine := seedClient(t, s)
	theirs := seedClient(t, s)

	n := seedForClient(t, s, mine, StatusDelivered, ChannelEmail)

	got, err := s.GetForClient(ctx, mine, n.ID)
	if err != nil {
		t.Fatalf("GetForClient (owner): %v", err)
	}
	if got.ID != n.ID {
		t.Errorf("got %s, want %s", got.ID, n.ID)
	}

	if _, err := s.GetForClient(ctx, theirs, n.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetForClient (other client) = %v, want ErrNotFound", err)
	}

	if _, err := s.GetForClient(ctx, mine, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetForClient (unknown id) = %v, want ErrNotFound", err)
	}
}
