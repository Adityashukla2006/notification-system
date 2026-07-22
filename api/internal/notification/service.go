// Package notification holds the domain orchestration for accepting a
// notification: validate the request, persist it (idempotently), enqueue it for
// the worker, and record the status transition. It sits between the HTTP
// handler and the store/queue so this logic is testable without HTTP, Postgres,
// or Redis.
package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// Store is the slice of the persistence layer the service needs. Defining it
// here (rather than importing the concrete *store.Store) lets the service be
// tested with a fake.
type Store interface {
	Create(ctx context.Context, n store.Notification) (store.Notification, bool, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status store.Status) error
}

// Enqueuer hands a persisted notification off to the worker for immediate
// delivery.
type Enqueuer interface {
	Enqueue(ctx context.Context, id uuid.UUID) error
}

// Scheduler defers a notification until its scheduled time, at which point a
// worker's promoter moves it onto the ready queue.
type Scheduler interface {
	Schedule(ctx context.Context, id uuid.UUID, at time.Time) error
}

// Outcome describes what happened to a Create call, so the HTTP layer can pick
// the right status code without re-deriving it.
type Outcome int

const (
	// OutcomeCreated: a new notification was accepted.
	OutcomeCreated Outcome = iota
	// OutcomeReplayed: the idempotency key matched an existing, identical
	// request; the original is returned unchanged.
	OutcomeReplayed
	// OutcomeConflict: the idempotency key was reused with a different request,
	// which is almost always a client bug.
	OutcomeConflict
)

// CreateInput is a validated-on-entry request to accept a notification.
type CreateInput struct {
	ClientID       uuid.UUID
	IdempotencyKey string
	Channel        string
	Recipient      string
	Payload        json.RawMessage
	ScheduledAt    *time.Time
	MaxAttempts    *int
}

// Result is the outcome of Create.
type Result struct {
	Notification store.Notification
	Outcome      Outcome
}

// ValidationError is a bad request the caller can fix. The HTTP layer maps it to
// 400 and surfaces Field/Message.
type ValidationError struct {
	Field   string
	Message string
}

// Error implements error.
func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// Service orchestrates accepting notifications.
type Service struct {
	store     Store
	queue     Enqueuer
	scheduler Scheduler
	logger    *slog.Logger
	nowFunc   func() time.Time
}

// New constructs a Service.
func New(s Store, q Enqueuer, sch Scheduler, logger *slog.Logger) *Service {
	return &Service{store: s, queue: q, scheduler: sch, logger: logger, nowFunc: time.Now}
}

// now returns the current time through the service's clock, which tests replace.
func (s *Service) now() time.Time {
	return s.nowFunc()
}

// Create validates, persists, and enqueues a notification. It is safe to retry:
// the same idempotency key returns the original row rather than duplicating it,
// and a row left un-enqueued by an earlier failure is enqueued on retry.
func (s *Service) Create(ctx context.Context, in CreateInput) (Result, error) {
	if err := validate(in); err != nil {
		return Result{}, err
	}

	stored, created, err := s.store.Create(ctx, toNotification(in))
	if err != nil {
		return Result{}, fmt.Errorf("persisting notification: %w", err)
	}

	// Idempotent replay: the key already existed. Reject a reused key that
	// carries a different request; otherwise fall through and make sure the
	// existing row is queued (it may have been left pending by a prior failure).
	if !created && !sameRequest(in, stored) {
		return Result{Notification: stored, Outcome: OutcomeConflict}, nil
	}

	// A freshly created row is pending; a replay may be pending or already
	// queued. Hand off only while still pending so retries don't double-queue.
	if stored.Status == store.StatusPending {
		if err := s.handOff(ctx, stored); err != nil {
			// Persisted but not handed off. Surface the error so the client
			// retries; idempotency guarantees the retry re-queues the same
			// row without creating a duplicate.
			return Result{}, err
		}
		if err := s.store.UpdateStatus(ctx, stored.ID, store.StatusQueued); err != nil {
			// The id is on the queue, which is what matters for delivery; the
			// status write is bookkeeping. Log and proceed rather than fail a
			// request whose work is already handed off.
			s.logger.Warn("failed to mark notification queued", "id", stored.ID, "error", err)
		}
		stored.Status = store.StatusQueued
	}

	outcome := OutcomeReplayed
	if created {
		outcome = OutcomeCreated
	}
	return Result{Notification: stored, Outcome: outcome}, nil
}

// handOff routes a persisted notification to the right Redis structure: the
// ready queue for an immediate send, or the schedule for a future-dated one.
//
// This is where scheduled_at becomes real. Enqueueing a future-dated
// notification directly would have a worker claim and deliver it at once,
// silently ignoring the time the client asked for — so a notification that is
// not yet due must never touch the ready queue.
func (s *Service) handOff(ctx context.Context, n store.Notification) error {
	if n.ScheduledAt.After(s.now()) {
		if err := s.scheduler.Schedule(ctx, n.ID, n.ScheduledAt); err != nil {
			return fmt.Errorf("scheduling notification: %w", err)
		}
		return nil
	}
	if err := s.queue.Enqueue(ctx, n.ID); err != nil {
		return fmt.Errorf("enqueueing notification: %w", err)
	}
	return nil
}

// toNotification builds a store.Notification from validated input, leaving
// server-side defaults (id, status, and — when unset — scheduled_at and
// max_attempts) for the store to fill.
func toNotification(in CreateInput) store.Notification {
	n := store.Notification{
		ClientID:       in.ClientID,
		IdempotencyKey: in.IdempotencyKey,
		Channel:        store.Channel(in.Channel),
		Recipient:      in.Recipient,
		Payload:        in.Payload,
	}
	if in.ScheduledAt != nil {
		n.ScheduledAt = *in.ScheduledAt
	}
	if in.MaxAttempts != nil {
		n.MaxAttempts = *in.MaxAttempts
	}
	return n
}

// sameRequest reports whether an incoming request matches an existing
// notification for the purpose of idempotency. It compares the essential
// content — channel, recipient, and payload — so a reused key carrying a
// genuinely different request is caught as a conflict. Payload is compared as
// decoded JSON because jsonb does not preserve byte formatting.
func sameRequest(in CreateInput, existing store.Notification) bool {
	if store.Channel(in.Channel) != existing.Channel || in.Recipient != existing.Recipient {
		return false
	}
	return jsonEqual(in.Payload, existing.Payload)
}

// jsonEqual reports semantic JSON equality, ignoring formatting and key order.
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// validate enforces the request rules that produce a clean 400 rather than a
// database constraint error (a 500) further down.
func validate(in CreateInput) error {
	if in.IdempotencyKey == "" {
		return ValidationError{Field: "idempotency_key", Message: "required"}
	}
	if !validChannel(in.Channel) {
		return ValidationError{Field: "channel", Message: "must be one of email, sms, push"}
	}
	if in.Recipient == "" {
		return ValidationError{Field: "recipient", Message: "required"}
	}
	if err := validatePayload(in.Payload); err != nil {
		return err
	}
	if in.MaxAttempts != nil && *in.MaxAttempts <= 0 {
		return ValidationError{Field: "max_attempts", Message: "must be greater than zero"}
	}
	return nil
}

func validChannel(c string) bool {
	switch store.Channel(c) {
	case store.ChannelEmail, store.ChannelSMS, store.ChannelPush:
		return true
	default:
		return false
	}
}

// validatePayload requires a non-null JSON object. A JSON array, string,
// number, or literal null is rejected.
func validatePayload(payload json.RawMessage) error {
	if len(payload) == 0 {
		return ValidationError{Field: "payload", Message: "required"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return ValidationError{Field: "payload", Message: "must be a JSON object"}
	}
	return nil
}

// AsValidationError reports whether err is a ValidationError, for the HTTP layer
// to map to 400.
func AsValidationError(err error) (ValidationError, bool) {
	var ve ValidationError
	if errors.As(err, &ve) {
		return ve, true
	}
	return ValidationError{}, false
}
