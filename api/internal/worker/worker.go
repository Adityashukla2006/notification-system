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
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/provider"
	"github.com/Adityashukla2006/notification-system/api/internal/queue"
	"github.com/Adityashukla2006/notification-system/api/internal/retry"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// Store is the slice of the persistence layer the worker needs. Declaring it
// here rather than importing *store.Store keeps the loop testable with a fake.
type Store interface {
	GetByID(ctx context.Context, id uuid.UUID) (store.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status store.Status) error
	RecordFailure(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time) (store.Notification, error)
}

// Scheduler defers a notification until it is due, and moves due notifications
// onto the ready queue.
type Scheduler interface {
	Schedule(ctx context.Context, id uuid.UUID, at time.Time) error
	PromoteDue(ctx context.Context, now time.Time, limit int64) (int, error)
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
	scheduler    Scheduler
	providers    provider.Registry
	policy       retry.Policy
	logger       *slog.Logger
	claimTimeout time.Duration
	errorBackoff time.Duration
	promoteEvery time.Duration
	promoteLimit int64
	nowFunc      func() time.Time
}

// Config holds the Worker's tunables, grouped so New does not take an unreadable
// row of positional arguments.
type Config struct {
	// ClaimTimeout bounds how long a single claim blocks; it is the upper
	// bound on how long shutdown waits for the loop to notice.
	ClaimTimeout time.Duration
	// PromoteEvery is how often the promoter sweeps for due notifications. It
	// is the granularity of scheduled delivery: a notification due at T is
	// delivered somewhere in [T, T+PromoteEvery).
	PromoteEvery time.Duration
	// PromoteLimit caps how many notifications one sweep promotes, so a large
	// backlog becoming due at once is drained in bounded batches rather than
	// in a single unbounded burst.
	PromoteLimit int64
	// Policy is the retry backoff schedule.
	Policy retry.Policy
}

// New constructs a Worker.
func New(s Store, c Claimer, sch Scheduler, providers provider.Registry, logger *slog.Logger, cfg Config) *Worker {
	if cfg.PromoteEvery <= 0 {
		cfg.PromoteEvery = time.Second
	}
	if cfg.PromoteLimit <= 0 {
		cfg.PromoteLimit = 100
	}
	return &Worker{
		store:        s,
		claimer:      c,
		scheduler:    sch,
		providers:    providers,
		policy:       cfg.Policy,
		logger:       logger,
		claimTimeout: cfg.ClaimTimeout,
		errorBackoff: defaultErrorBackoff,
		promoteEvery: cfg.PromoteEvery,
		promoteLimit: cfg.PromoteLimit,
		nowFunc:      time.Now,
	}
}

// now returns the current time through the worker's clock, which tests replace.
func (w *Worker) now() time.Time {
	return w.nowFunc()
}

// Run claims and delivers notifications until ctx is cancelled. It returns nil
// on clean shutdown; a cancelled context is the expected way to stop, not an
// error.
//
// Shutdown is graceful by construction: cancellation is only ever observed
// between deliveries or while blocked on a claim, so an in-flight delivery is
// always allowed to finish and record its outcome before the loop exits.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("worker started",
		"claim_timeout", w.claimTimeout,
		"promote_every", w.promoteEvery,
	)

	// The promoter runs alongside the delivery loop rather than inside it: a
	// blocking claim can park for the full claim timeout, and scheduled
	// notifications must not wait on that to become due.
	var promoter sync.WaitGroup
	promoter.Add(1)
	go func() {
		defer promoter.Done()
		w.runPromoter(ctx)
	}()
	defer promoter.Wait()

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

	if deliverErr != nil {
		logger.Error("delivery failed", "channel", n.Channel, "error", deliverErr)
		if !w.recordFailure(ctx, logger, n) {
			// Not durable, so do not ack: the id stays claimed for reclaim.
			return
		}
		w.ack(ctx, logger, id)
		return
	}

	if err := w.setStatus(ctx, logger, id, store.StatusDelivered); err != nil {
		// The outcome is not durable, so do not ack: the id stays claimed and
		// the terminal-state check above makes a re-delivery safe to repeat.
		return
	}

	// Ack last, and only now: the outcome is durable in Postgres, so releasing
	// the claim can no longer lose work.
	w.ack(ctx, logger, id)
}

// recordFailure increments the attempt count and either schedules a retry or
// lets the row be dead-lettered. It reports whether the outcome is durable
// enough to release the claim.
//
// Postgres decides the branch, not this function: RecordFailure does the
// increment and the max_attempts comparison in one statement, so concurrent
// deliveries of the same notification cannot together exceed the ceiling.
func (w *Worker) recordFailure(ctx context.Context, logger *slog.Logger, n store.Notification) bool {
	// Compute the next due time from the attempt number this failure produces.
	nextAttempt := n.Attempts + 1
	backoff := w.policy.Backoff(nextAttempt)
	nextAttemptAt := w.now().Add(backoff)

	updated, err := w.store.RecordFailure(ctx, n.ID, nextAttemptAt)
	if err != nil {
		logger.Error("recording delivery failure failed", "error", err)
		return false
	}

	if updated.Status == store.StatusDeadLettered {
		logger.Warn("notification dead-lettered, attempts exhausted",
			"attempts", updated.Attempts,
			"max_attempts", updated.MaxAttempts,
		)
		return true
	}

	// Retries live on the schedule, not the ready queue, so the backoff is
	// actually honored instead of the notification being re-claimed instantly.
	if err := w.scheduler.Schedule(ctx, n.ID, nextAttemptAt); err != nil {
		// Postgres already holds the next due time in scheduled_at, so the
		// retry is recoverable — but nothing will promote it until a reaper
		// exists, so this is loud.
		logger.Error("scheduling retry failed; retry is recorded in postgres but not queued",
			"error", err, "next_attempt_at", nextAttemptAt)
		return false
	}

	logger.Info("retry scheduled",
		"attempts", updated.Attempts,
		"max_attempts", updated.MaxAttempts,
		"backoff", backoff,
		"next_attempt_at", nextAttemptAt,
	)
	return true
}

// runPromoter periodically moves due notifications onto the ready queue. It
// exits when ctx is cancelled.
func (w *Worker) runPromoter(ctx context.Context) {
	ticker := time.NewTicker(w.promoteEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.scheduler.PromoteDue(ctx, w.now(), w.promoteLimit)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Error("promoting due notifications failed", "error", err)
				continue
			}
			if n > 0 {
				w.logger.Debug("promoted due notifications", "count", n)
			}
		}
	}
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
