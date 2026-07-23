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
	RecordAttempt(ctx context.Context, a store.Attempt) (store.Attempt, error)
	ReapStuck(ctx context.Context, stuckBefore time.Time, limit int) ([]uuid.UUID, error)
	DeadLetter(ctx context.Context, id uuid.UUID) (store.Notification, error)
}

// Enqueuer puts a notification back on the ready queue. The reaper needs it to
// return recovered rows to Redis.
type Enqueuer interface {
	Enqueue(ctx context.Context, id uuid.UUID) error
}

// Scheduler defers a notification until it is due, and moves due notifications
// onto the ready queue.
type Scheduler interface {
	Schedule(ctx context.Context, id uuid.UUID, at time.Time) error
	PromoteDue(ctx context.Context, now time.Time, limit int64) (int, error)
}

// Claimer is the worker's end of the queue: a reliable claim that survives a
// crash, an explicit acknowledgement that releases it, and a way to recover
// claims this worker left behind in a previous life.
type Claimer interface {
	Claim(ctx context.Context, timeout time.Duration) (uuid.UUID, error)
	Ack(ctx context.Context, id uuid.UUID) error
	Drain(ctx context.Context) (int, error)
}

// Reclaimer keeps this worker's liveness visible and returns other workers'
// abandoned claims to the ready queue.
type Reclaimer interface {
	Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error
	ReclaimAbandoned(ctx context.Context) (notifications int, workers int, err error)
}

// defaultErrorBackoff is how long the loop pauses after an unexpected claim
// error, so that a Redis outage produces a slow retry rather than a hot spin
// that floods the logs and saturates a recovering server.
const defaultErrorBackoff = time.Second

// Worker runs the delivery loop.
type Worker struct {
	store           Store
	claimer         Claimer
	scheduler       Scheduler
	reclaimer       Reclaimer
	enqueuer        Enqueuer
	providers       provider.Registry
	policy          retry.Policy
	logger          *slog.Logger
	workerID        string
	claimTimeout    time.Duration
	errorBackoff    time.Duration
	promoteEvery    time.Duration
	promoteLimit    int64
	heartbeatEvery  time.Duration
	livenessTTL     time.Duration
	reclaimEvery    time.Duration
	reapEvery       time.Duration
	stuckAfter      time.Duration
	reapLimit       int
	deliveryTimeout time.Duration
	nowFunc         func() time.Time
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
	// WorkerID identifies this worker's processing list and liveness key.
	WorkerID string
	// HeartbeatEvery is how often this worker refreshes its liveness key.
	HeartbeatEvery time.Duration
	// LivenessTTL is how long that key survives without a refresh. It must
	// comfortably exceed HeartbeatEvery, or a merely slow worker is declared
	// dead and its in-flight work is re-delivered underneath it.
	LivenessTTL time.Duration
	// ReclaimEvery is how often this worker sweeps for abandoned claims.
	ReclaimEvery time.Duration
	// ReapEvery is how often this worker sweeps Postgres for stuck rows.
	ReapEvery time.Duration
	// StuckAfter is how long a notification may sit untouched in a
	// non-terminal state before the reaper treats it as stranded. It must
	// exceed the longest legitimate delivery, or the reaper requeues work that
	// is merely slow.
	StuckAfter time.Duration
	// ReapLimit caps how many rows one reap sweep recovers.
	ReapLimit int
	// DeliveryTimeout bounds a single provider call. It must be shorter than
	// StuckAfter, or the reaper would declare a delivery stranded while it is
	// still legitimately running.
	DeliveryTimeout time.Duration
}

// Defaults applied when a Config leaves a duration unset.
const (
	defaultPromoteEvery   = time.Second
	defaultPromoteLimit   = 100
	defaultHeartbeatEvery = 5 * time.Second
	defaultLivenessTTL    = 30 * time.Second
	defaultReclaimEvery   = 30 * time.Second
	defaultReapEvery      = time.Minute
	defaultStuckAfter     = 5 * time.Minute
	defaultReapLimit      = 100
	defaultDeliveryTime   = 30 * time.Second
)

// New constructs a Worker.
func New(s Store, c Claimer, sch Scheduler, rec Reclaimer, enq Enqueuer, providers provider.Registry, logger *slog.Logger, cfg Config) *Worker {
	if cfg.ReapEvery <= 0 {
		cfg.ReapEvery = defaultReapEvery
	}
	if cfg.StuckAfter <= 0 {
		cfg.StuckAfter = defaultStuckAfter
	}
	if cfg.ReapLimit <= 0 {
		cfg.ReapLimit = defaultReapLimit
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = defaultDeliveryTime
	}
	// A delivery allowed to outlive the stuck threshold would be reaped and
	// re-queued while it is still running, producing a duplicate send that the
	// timeout exists to prevent.
	if cfg.DeliveryTimeout >= cfg.StuckAfter {
		logger.Warn("delivery timeout must be shorter than the stuck threshold; shortening it",
			"delivery_timeout", cfg.DeliveryTimeout,
			"stuck_after", cfg.StuckAfter,
			"using_timeout", cfg.StuckAfter/2,
		)
		cfg.DeliveryTimeout = cfg.StuckAfter / 2
	}
	if cfg.PromoteEvery <= 0 {
		cfg.PromoteEvery = defaultPromoteEvery
	}
	if cfg.PromoteLimit <= 0 {
		cfg.PromoteLimit = defaultPromoteLimit
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = defaultHeartbeatEvery
	}
	if cfg.LivenessTTL <= 0 {
		cfg.LivenessTTL = defaultLivenessTTL
	}
	if cfg.ReclaimEvery <= 0 {
		cfg.ReclaimEvery = defaultReclaimEvery
	}
	// A TTL at or below the heartbeat interval guarantees the key lapses
	// between refreshes, so every worker would continuously declare itself
	// dead. Widen it rather than run in that state.
	if cfg.LivenessTTL <= cfg.HeartbeatEvery {
		logger.Warn("liveness ttl must exceed the heartbeat interval; widening it",
			"heartbeat_every", cfg.HeartbeatEvery,
			"configured_ttl", cfg.LivenessTTL,
			"using_ttl", 3*cfg.HeartbeatEvery,
		)
		cfg.LivenessTTL = 3 * cfg.HeartbeatEvery
	}

	return &Worker{
		store:           s,
		claimer:         c,
		scheduler:       sch,
		reclaimer:       rec,
		enqueuer:        enq,
		providers:       providers,
		policy:          cfg.Policy,
		logger:          logger,
		workerID:        cfg.WorkerID,
		claimTimeout:    cfg.ClaimTimeout,
		errorBackoff:    defaultErrorBackoff,
		promoteEvery:    cfg.PromoteEvery,
		promoteLimit:    cfg.PromoteLimit,
		heartbeatEvery:  cfg.HeartbeatEvery,
		livenessTTL:     cfg.LivenessTTL,
		reclaimEvery:    cfg.ReclaimEvery,
		reapEvery:       cfg.ReapEvery,
		stuckAfter:      cfg.StuckAfter,
		reapLimit:       cfg.ReapLimit,
		deliveryTimeout: cfg.DeliveryTimeout,
		nowFunc:         time.Now,
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
	// worker_id is deliberately absent: the injected logger already carries it,
	// and repeating it emits the same JSON key twice, which consumers may drop
	// or resolve inconsistently.
	w.logger.Info("worker started",
		"claim_timeout", w.claimTimeout,
		"promote_every", w.promoteEvery,
		"heartbeat_every", w.heartbeatEvery,
		"liveness_ttl", w.livenessTTL,
		"reclaim_every", w.reclaimEvery,
	)

	// Publish liveness before anything else. Until this key exists, another
	// worker's reclaim sweep would see this worker's processing list with no
	// live owner and start pulling work out from under it.
	if err := w.reclaimer.Heartbeat(ctx, w.workerID, w.livenessTTL); err != nil {
		w.logger.Error("initial heartbeat failed", "error", err)
	}

	// Recover anything this worker left claimed when it last stopped. Whatever
	// is on our own processing list now predates this process, so it is safe to
	// requeue immediately rather than wait for a liveness key to lapse.
	if moved, err := w.claimer.Drain(ctx); err != nil {
		w.logger.Error("draining own processing list failed", "error", err)
	} else if moved > 0 {
		w.logger.Info("recovered claims from a previous run", "count", moved)
	}

	// The background loops run alongside the delivery loop rather than inside
	// it: a blocking claim can park for the full claim timeout, and neither
	// scheduled notifications nor liveness may wait on that.
	var background sync.WaitGroup
	background.Add(4)
	go func() {
		defer background.Done()
		w.runReaper(ctx)
	}()
	go func() {
		defer background.Done()
		w.runPromoter(ctx)
	}()
	go func() {
		defer background.Done()
		w.runHeartbeat(ctx)
	}()
	go func() {
		defer background.Done()
		w.runReclaimer(ctx)
	}()
	defer background.Wait()

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

	// Bound the provider call. Without this a provider that accepts a
	// connection and then stops responding holds this worker forever: the
	// delivery loop is sequential, so one hung call stops the worker entirely,
	// and the notification is never acked or retried.
	deliverCtx, cancelDelivery := context.WithTimeout(ctx, w.deliveryTimeout)
	startedAt := w.now()
	deliverErr := p.Deliver(deliverCtx, provider.Message{
		ID:        n.ID,
		Recipient: n.Recipient,
		Payload:   n.Payload,
	})
	cancelDelivery()

	w.recordAttempt(ctx, logger, n, startedAt, deliverErr)

	// A permanent failure will fail identically on every future attempt, so
	// spending the remaining retries on it only delays real work and, for
	// email, keeps pushing known-bad addresses at the provider.
	if deliverErr != nil && provider.IsPermanent(deliverErr) {
		logger.Error("delivery failed permanently, dead-lettering without retry",
			"channel", n.Channel, "error", deliverErr)
		// DeadLetter, not a plain status write: it also increments attempts, so
		// the row does not claim zero attempts while its history shows one.
		if _, err := w.store.DeadLetter(ctx, id); err != nil {
			logger.Error("dead-lettering failed", "error", err)
			return
		}
		w.ack(ctx, logger, id)
		return
	}

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

// recordAttempt appends this delivery attempt to the notification's history.
//
// It deliberately does not report failure to its caller and never blocks the
// delivery path. The history is observational — nothing reads it to decide what
// happens next — so losing an entry costs visibility, not correctness. Failing
// a delivery because its audit row could not be written would trade something
// that matters for something that does not.
func (w *Worker) recordAttempt(ctx context.Context, logger *slog.Logger, n store.Notification, startedAt time.Time, deliverErr error) {
	outcome := store.AttemptSucceeded
	errText := ""
	if deliverErr != nil {
		outcome = store.AttemptFailed
		errText = deliverErr.Error()
	}

	// attempt_number is 1-based: the row's counter holds attempts completed
	// before this one.
	if _, err := w.store.RecordAttempt(ctx, store.Attempt{
		NotificationID: n.ID,
		AttemptNumber:  n.Attempts + 1,
		Outcome:        outcome,
		Error:          errText,
		StartedAt:      startedAt,
		FinishedAt:     w.now(),
	}); err != nil {
		logger.Error("recording delivery attempt failed; history is incomplete", "error", err)
	}
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

// runHeartbeat refreshes this worker's liveness key until ctx is cancelled.
//
// It does NOT delete the key on shutdown. Letting it expire naturally means a
// worker restarting within the TTL keeps ownership of its own claims and
// recovers them itself via Drain, instead of another worker grabbing them
// mid-restart.
func (w *Worker) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.reclaimer.Heartbeat(ctx, w.workerID, w.livenessTTL); err != nil {
				if ctx.Err() != nil {
					return
				}
				// Serious: if this keeps failing, the key lapses and another
				// worker will reclaim work this one is actively delivering.
				w.logger.Error("heartbeat failed; this worker may be declared dead", "error", err)
			}
		}
	}
}

// runReclaimer periodically returns dead workers' claims to the ready queue.
func (w *Worker) runReclaimer(ctx context.Context) {
	ticker := time.NewTicker(w.reclaimEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			notifications, workers, err := w.reclaimer.ReclaimAbandoned(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Error("reclaiming abandoned claims failed", "error", err)
				continue
			}
			if notifications > 0 {
				w.logger.Warn("reclaimed claims from workers that are no longer alive",
					"notifications", notifications,
					"workers", workers,
				)
			}
		}
	}
}

// runReaper periodically recovers notifications that are stranded in Postgres.
//
// This is the last line of recovery, and the only one that survives losing
// Redis entirely: it consults nothing but the source of truth.
func (w *Worker) runReaper(ctx context.Context) {
	ticker := time.NewTicker(w.reapEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.reapOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Error("reaping stuck notifications failed", "error", err)
			}
		}
	}
}

// reapOnce performs a single reap sweep, returning stranded notifications to
// the ready queue.
func (w *Worker) reapOnce(ctx context.Context) error {
	stuckBefore := w.now().Add(-w.stuckAfter)

	ids, err := w.store.ReapStuck(ctx, stuckBefore, w.reapLimit)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	// The rows are already marked queued. Enqueue is what actually makes them
	// deliverable again; if it fails, the row simply goes stale once more and
	// the next sweep picks it up, so this is self-healing rather than a leak.
	enqueued := 0
	for _, id := range ids {
		if err := w.enqueuer.Enqueue(ctx, id); err != nil {
			w.logger.Error("re-enqueueing reaped notification failed", "notification_id", id, "error", err)
			continue
		}
		enqueued++
	}

	w.logger.Warn("recovered stranded notifications from postgres",
		"reaped", len(ids),
		"enqueued", enqueued,
		"stuck_before", stuckBefore,
	)
	return nil
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
