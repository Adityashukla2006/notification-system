package notification

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// fakeStore is an in-memory Store. Create mimics the real idempotency behavior:
// a repeated (client_id, idempotency_key) returns the original with created=false.
type fakeStore struct {
	byKey     map[string]store.Notification // client_id+"|"+idempotency_key -> row
	byID      map[uuid.UUID]store.Notification
	createErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		byKey: map[string]store.Notification{},
		byID:  map[uuid.UUID]store.Notification{},
	}
}

func (f *fakeStore) Create(_ context.Context, n store.Notification) (store.Notification, bool, error) {
	if f.createErr != nil {
		return store.Notification{}, false, f.createErr
	}
	key := n.ClientID.String() + "|" + n.IdempotencyKey
	if existing, ok := f.byKey[key]; ok {
		return existing, false, nil
	}
	// Emulate the store filling server-side fields.
	if n.ID == uuid.Nil {
		n.ID = uuid.New()
	}
	if n.Status == "" {
		n.Status = store.StatusPending
	}
	f.byKey[key] = n
	f.byID[n.ID] = n
	return n, true, nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, id uuid.UUID, status store.Status) error {
	n, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	n.Status = status
	f.byID[id] = n
	for k, v := range f.byKey {
		if v.ID == id {
			v.Status = status
			f.byKey[k] = v
		}
	}
	return nil
}

// fakeQueue records enqueued ids and can be made to fail.
type fakeQueue struct {
	enqueued []uuid.UUID
	err      error
}

func (q *fakeQueue) Enqueue(_ context.Context, id uuid.UUID) error {
	if q.err != nil {
		return q.err
	}
	q.enqueued = append(q.enqueued, id)
	return nil
}

func newService(s Store, q Enqueuer) *Service {
	return New(s, q, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func validInput() CreateInput {
	return CreateInput{
		ClientID:       uuid.New(),
		IdempotencyKey: "idem-1",
		Channel:        "email",
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"subject":"hi"}`),
	}
}

func TestServiceCreateAcceptsAndEnqueues(t *testing.T) {
	st, q := newFakeStore(), &fakeQueue{}
	svc := newService(st, q)

	res, err := svc.Create(context.Background(), validInput())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if res.Outcome != OutcomeCreated {
		t.Errorf("outcome = %v, want OutcomeCreated", res.Outcome)
	}
	if res.Notification.Status != store.StatusQueued {
		t.Errorf("status = %q, want queued", res.Notification.Status)
	}
	if len(q.enqueued) != 1 || q.enqueued[0] != res.Notification.ID {
		t.Errorf("enqueued = %v, want [%v]", q.enqueued, res.Notification.ID)
	}
}

func TestServiceCreateIdempotentReplay(t *testing.T) {
	st, q := newFakeStore(), &fakeQueue{}
	svc := newService(st, q)
	ctx := context.Background()

	in := validInput()
	first, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Identical request, same key: replay, no second enqueue.
	res, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if res.Outcome != OutcomeReplayed {
		t.Errorf("outcome = %v, want OutcomeReplayed", res.Outcome)
	}
	if res.Notification.ID != first.Notification.ID {
		t.Errorf("replay id = %v, want %v", res.Notification.ID, first.Notification.ID)
	}
	if len(q.enqueued) != 1 {
		t.Errorf("enqueued %d times, want 1 (replay must not re-enqueue an already-queued row)", len(q.enqueued))
	}
}

func TestServiceCreateConflictOnDifferentBody(t *testing.T) {
	st, q := newFakeStore(), &fakeQueue{}
	svc := newService(st, q)
	ctx := context.Background()

	in := validInput()
	if _, err := svc.Create(ctx, in); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Same client and key, different recipient.
	conflicting := in
	conflicting.Recipient = "someone-else@example.com"
	res, err := svc.Create(ctx, conflicting)
	if err != nil {
		t.Fatalf("conflict Create: %v", err)
	}
	if res.Outcome != OutcomeConflict {
		t.Errorf("outcome = %v, want OutcomeConflict", res.Outcome)
	}
}

// TestServiceReenqueuesPendingReplay covers the failure-then-retry path: if the
// first attempt persisted but failed to enqueue (row left pending), a retry with
// the same key must enqueue it rather than treat it as a completed replay.
func TestServiceReenqueuesPendingReplay(t *testing.T) {
	st := newFakeStore()
	failing := &fakeQueue{err: errors.New("redis down")}
	svc := newService(st, failing)
	ctx := context.Background()

	// First attempt: persists, then enqueue fails -> error, row stays pending.
	in := validInput()
	if _, err := svc.Create(ctx, in); err == nil {
		t.Fatal("first Create = nil error, want enqueue failure")
	}

	// Retry with a working queue: must enqueue the still-pending row.
	working := &fakeQueue{}
	svc = newService(st, working)
	res, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatalf("retry Create: %v", err)
	}
	if res.Notification.Status != store.StatusQueued {
		t.Errorf("status = %q, want queued after retry", res.Notification.Status)
	}
	if len(working.enqueued) != 1 {
		t.Errorf("retry enqueued %d, want 1", len(working.enqueued))
	}
}

func TestServiceEnqueueFailureSurfaces(t *testing.T) {
	st := newFakeStore()
	q := &fakeQueue{err: errors.New("redis down")}
	svc := newService(st, q)

	_, err := svc.Create(context.Background(), validInput())
	if err == nil {
		t.Fatal("Create = nil error, want enqueue failure surfaced")
	}
}

func TestServiceValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(in *CreateInput)
		field  string
	}{
		{"missing idempotency key", func(in *CreateInput) { in.IdempotencyKey = "" }, "idempotency_key"},
		{"bad channel", func(in *CreateInput) { in.Channel = "pigeon" }, "channel"},
		{"missing recipient", func(in *CreateInput) { in.Recipient = "" }, "recipient"},
		{"missing payload", func(in *CreateInput) { in.Payload = nil }, "payload"},
		{"payload not object", func(in *CreateInput) { in.Payload = json.RawMessage(`[1,2]`) }, "payload"},
		{"payload null", func(in *CreateInput) { in.Payload = json.RawMessage(`null`) }, "payload"},
		{"zero max attempts", func(in *CreateInput) { z := 0; in.MaxAttempts = &z }, "max_attempts"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, q := newFakeStore(), &fakeQueue{}
			svc := newService(st, q)

			in := validInput()
			tt.mutate(&in)

			_, err := svc.Create(context.Background(), in)
			ve, ok := AsValidationError(err)
			if !ok {
				t.Fatalf("error = %v, want ValidationError", err)
			}
			if ve.Field != tt.field {
				t.Errorf("field = %q, want %q", ve.Field, tt.field)
			}
			if len(q.enqueued) != 0 {
				t.Error("enqueued despite validation failure")
			}
		})
	}
}
