// Package worker is the delivery loop: claim a notification id from Redis, load
// the authoritative row from Postgres, hand it to the provider for its channel,
// and record the outcome.
//
// The loop deliberately owns no delivery logic of its own. It coordinates —
// claim, load, dispatch, record, ack — so that providers stay dumb and the
// ordering guarantees live in exactly one place.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/provider"
	"github.com/Adityashukla2006/notification-system/api/internal/queue"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// Store is the slice of the persistence layer the worker needs. Declaring it
// here rather than importing *store.Store keeps the loop testable with a fake.
type Store interface {
	GetByID(ctx context.Context, id uuid.UUID) (store.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status store.Status) error
}

// Claimer is the worker's end of the queue: a reliable claim that survives a
// crash, plus an explicit acknowledgement that releases it.
type Claimer interface {
	Claim(ctx context.Context, timeout time.Duration) (uuid.UUID, error)
	Ack(ctx context.Context, id uuid.UUID) error
}

// defaultErrorBackoff is how long the loop pauses after an unexpected claim
// error, so that a Redis outage produces a slow retry rather than a hot spin
// that floods the logs and saturates a recovering server.
const defaultErrorBackoff = time.Second

// Worker runs the delivery loop.
type Worker struct {
	store        Store
	claimer      Claimer
	providers    provider.Registry
	logger       *slog.Logger
	claimTimeout time.Duration
	errorBackoff time.Duration
}

// New constructs a Worker. claimTimeout bounds how long a single claim blocks;
// it is the upper bound on how long shutdown waits for the loop to notice.
func New(s Store, c Claimer, providers provider.Registry, logger *slog.Logger, claimTimeout time.Duration) *Worker {
	return &Worker{
		store:        s,
		claimer:      c,
		providers:    providers,
		logger:       logger,
		claimTimeout: claimTimeout,
		errorBackoff: defaultErrorBackoff,
	}
}

// Run claims and delivers notifications until ctx is cancelled. It returns nil
// on clean shutdown; a cancelled context is the expected way to stop, not an
// error.
//
// Shutdown is graceful by construction: cancellation is only ever observed
// between deliveries or while blocked on a claim, so an in-flight delivery is
// always allowed to finish and record its outcome before the loop exits.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started", "claim_timeout", w.claimTimeout)

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped")
			return nil
		}

		id, err := w.claimer.Claim(ctx, w.claimTimeout)
		switch {
		case err == nil:
			// Deliver on a context detached from shutdown, so a signal
			// arriving mid-delivery cannot abort a send that is already in
			// flight or, worse, prevent its outcome from being recorded.
			w.process(context.WithoutCancel(ctx), id)
		case errors.Is(err, queue.ErrNoWork):
			// Idle tick: nothing arrived within the timeout. Loop and re-claim.
		case ctx.Err() != nil:
			// The claim failed because we are shutting down, not because Redis
			// is unhealthy.
		default:
			w.logger.Error("claiming notification failed", "error", err)
			w.sleep(ctx, w.errorBackoff)
		}
	}
}

// process delivers one claimed notification and records the result. It never
// returns an error: every failure mode is either recorded on the row or logged,
// because a single bad notification must not stop the loop.
func (w *Worker) process(ctx context.Context, id uuid.UUID) {
	logger := w.logger.With("notification_id", id)

	n, err := w.store.GetByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		// The queue referenced a row that does not exist. Nothing can ever
		// deliver it, so ack it rather than let it be re-claimed forever.
		logger.Warn("claimed notification not found, discarding")
		w.ack(ctx, logger, id)
		return
	}
	if err != nil {
		// Postgres is unreachable or erroring. Do NOT ack: leaving the id on
		// the processing list is what preserves it for a later reclaim.
		logger.Error("loading claimed notification failed", "error", err)
		return
	}

	// At-least-once means the same id can legitimately be claimed twice — for
	// example after a worker died between delivering and acking. Re-sending a
	// notification that already reached a terminal state would turn that
	// duplicate claim into a duplicate message, so stop here.
	if isTerminal(n.Status) {
		logger.Info("notification already in terminal state, skipping", "status", n.Status)
		w.ack(ctx, logger, id)
		return
	}

	p, err := w.providers.For(string(n.Channel))
	if err != nil {
		// A missing provider is a deployment fault that retrying cannot fix.
		// Mark the row failed so it is visible, and ack so it stops circulating.
		logger.Error("no provider for channel", "channel", n.Channel, "error", err)
		w.setStatus(ctx, logger, id, store.StatusFailed)
		w.ack(ctx, logger, id)
		return
	}

	// Mark delivering before calling out, so a crash during the provider call
	// leaves evidence that this row was mid-flight rather than untouched.
	if err := w.setStatus(ctx, logger, id, store.StatusDelivering); err != nil {
		// Without this write the row's state would silently diverge from
		// reality, so bail and leave the claim for a reclaim.
		return
	}

	deliverErr := p.Deliver(ctx, provider.Message{
		ID:        n.ID,
		Recipient: n.Recipient,
		Payload:   n.Payload,
	})

	status := store.StatusDelivered
	if deliverErr != nil {
		// Terminal for now. Retry scheduling and dead-lettering land in the
		// next feature; until then a failed attempt stops here.
		logger.Error("delivery failed", "channel", n.Channel, "error", deliverErr)
		status = store.StatusFailed
	}

	if err := w.setStatus(ctx, logger, id, status); err != nil {
		// The outcome is not durable, so do not ack: the id stays claimed and
		// the terminal-state check above makes a re-delivery safe to repeat.
		return
	}

	// Ack last, and only now: the outcome is durable in Postgres, so releasing
	// the claim can no longer lose work.
	w.ack(ctx, logger, id)
}

// setStatus writes a status transition, logging and returning any error.
func (w *Worker) setStatus(ctx context.Context, logger *slog.Logger, id uuid.UUID, status store.Status) error {
	if err := w.store.UpdateStatus(ctx, id, status); err != nil {
		logger.Error("updating notification status failed", "status", status, "error", err)
		return fmt.Errorf("updating status to %s: %w", status, err)
	}
	return nil
}

// ack releases a claim, logging failures. A failed ack is not fatal: the id
// stays on the processing list and the terminal-state check makes the eventual
// re-claim a no-op.
func (w *Worker) ack(ctx context.Context, logger *slog.Logger, id uuid.UUID) {
	if err := w.claimer.Ack(ctx, id); err != nil {
		logger.Error("acking notification failed", "error", err)
	}
}

// sleep pauses for d, returning early if ctx is cancelled so a shutdown is not
// held up by a backoff.
func (w *Worker) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// isTerminal reports whether a status is an end state that must never be
// delivered again.
func isTerminal(s store.Status) bool {
	switch s {
	case store.StatusDelivered, store.StatusDeadLettered:
		return true
	default:
		return false
	}
}
