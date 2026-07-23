package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/provider"
	"github.com/Adityashukla2006/notification-system/api/internal/queue"
	"github.com/Adityashukla2006/notification-system/api/internal/retry"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// fakeStore is an in-memory Store that records the sequence of status writes,
// so tests can assert on the transition order (delivering before delivered),
// not merely the final state.
type fakeStore struct {
	byID             map[uuid.UUID]store.Notification
	statuses         []store.Status
	attempts         []store.Attempt
	getErr           error
	updateErr        error
	recordFailureErr error
	recordAttemptErr error
	failStatus       store.Status // when set, UpdateStatus fails only for this status
}

func newFakeStore(rows ...store.Notification) *fakeStore {
	f := &fakeStore{byID: map[uuid.UUID]store.Notification{}}
	for _, n := range rows {
		f.byID[n.ID] = n
	}
	return f
}

func (f *fakeStore) GetByID(_ context.Context, id uuid.UUID) (store.Notification, error) {
	if f.getErr != nil {
		return store.Notification{}, f.getErr
	}
	n, ok := f.byID[id]
	if !ok {
		return store.Notification{}, store.ErrNotFound
	}
	return n, nil
}

func (f *fakeStore) UpdateStatus(_ context.Context, id uuid.UUID, status store.Status) error {
	if f.updateErr != nil && (f.failStatus == "" || f.failStatus == status) {
		return f.updateErr
	}
	n, ok := f.byID[id]
	if !ok {
		return store.ErrNotFound
	}
	n.Status = status
	f.byID[id] = n
	f.statuses = append(f.statuses, status)
	return nil
}

// RecordFailure mirrors the real store's single-statement semantics: increment
// attempts, then dead-letter at the ceiling or mark failed with a new due time.
func (f *fakeStore) RecordFailure(_ context.Context, id uuid.UUID, nextAttemptAt time.Time) (store.Notification, error) {
	if f.recordFailureErr != nil {
		return store.Notification{}, f.recordFailureErr
	}
	n, ok := f.byID[id]
	if !ok {
		return store.Notification{}, store.ErrNotFound
	}
	n.Attempts++
	if n.Attempts >= n.MaxAttempts {
		n.Status = store.StatusDeadLettered
	} else {
		n.Status = store.StatusFailed
		n.ScheduledAt = nextAttemptAt
	}
	f.byID[id] = n
	f.statuses = append(f.statuses, n.Status)
	return n, nil
}

// RecordAttempt appends to the recorded history.
func (f *fakeStore) RecordAttempt(_ context.Context, a store.Attempt) (store.Attempt, error) {
	if f.recordAttemptErr != nil {
		return store.Attempt{}, f.recordAttemptErr
	}
	a.ID = uuid.New()
	f.attempts = append(f.attempts, a)
	return a, nil
}

// fakeScheduler records what was deferred and to when.
type fakeScheduler struct {
	scheduled map[uuid.UUID]time.Time
	err       error
}

func newFakeScheduler() *fakeScheduler {
	return &fakeScheduler{scheduled: map[uuid.UUID]time.Time{}}
}

func (f *fakeScheduler) Schedule(_ context.Context, id uuid.UUID, at time.Time) error {
	if f.err != nil {
		return f.err
	}
	f.scheduled[id] = at
	return nil
}

func (f *fakeScheduler) PromoteDue(_ context.Context, _ time.Time, _ int64) (int, error) {
	return 0, nil
}

// fakeClaimer serves a fixed script of ids, then blocks the loop by reporting
// ErrNoWork forever. Every claim after the script is exhausted cancels the
// context, so Run terminates without the test depending on wall-clock timing.
type fakeClaimer struct {
	ids        []uuid.UUID
	acked      []uuid.UUID
	ackErr     error
	drainCalls int
	drainCount int
	drainErr   error
	stop       context.CancelFunc
}

func (f *fakeClaimer) Claim(_ context.Context, _ time.Duration) (uuid.UUID, error) {
	if len(f.ids) == 0 {
		f.stop()
		return uuid.Nil, queue.ErrNoWork
	}
	id := f.ids[0]
	f.ids = f.ids[1:]
	return id, nil
}

func (f *fakeClaimer) Ack(_ context.Context, id uuid.UUID) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = append(f.acked, id)
	return nil
}

func (f *fakeClaimer) Drain(_ context.Context) (int, error) {
	f.drainCalls++
	return f.drainCount, f.drainErr
}

// fakeReclaimer records heartbeats and reclaim sweeps.
type fakeReclaimer struct {
	mu            sync.Mutex
	heartbeats    int
	lastTTL       time.Duration
	lastWorkerID  string
	sweeps        int
	heartbeatErr  error
	reclaimErr    error
	reclaimResult int
	onSweep       func()
}

func (r *fakeReclaimer) Heartbeat(_ context.Context, workerID string, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeats++
	r.lastWorkerID = workerID
	r.lastTTL = ttl
	return r.heartbeatErr
}

func (r *fakeReclaimer) ReclaimAbandoned(_ context.Context) (int, int, error) {
	r.mu.Lock()
	sweep := r.onSweep
	r.sweeps++
	result := r.reclaimResult
	err := r.reclaimErr
	r.mu.Unlock()
	if sweep != nil {
		sweep()
	}
	return result, 0, err
}

func (r *fakeReclaimer) counts() (heartbeats, sweeps int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeats, r.sweeps
}

// fakeProvider records deliveries and can be made to fail.
type fakeProvider struct {
	delivered []provider.Message
	err       error
}

func (f *fakeProvider) Deliver(_ context.Context, msg provider.Message) error {
	f.delivered = append(f.delivered, msg)
	if f.err != nil {
		return f.err
	}
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// notification builds a claimable row in the given status.
func notification(id uuid.UUID, status store.Status, channel store.Channel) store.Notification {
	return store.Notification{
		ID:          id,
		ClientID:    uuid.New(),
		Channel:     channel,
		Recipient:   "user@example.com",
		Payload:     json.RawMessage(`{"body":"hi"}`),
		Status:      status,
		MaxAttempts: 5,
	}
}

// runOnce drives the loop over a single claimed id and returns once the script
// is exhausted, along with the scheduler so retries can be asserted on.
func runOnce(t *testing.T, st *fakeStore, c *fakeClaimer, reg provider.Registry) *fakeScheduler {
	t.Helper()
	sch := newFakeScheduler()
	runWith(t, st, c, sch, reg)
	return sch
}

// runWith drives the loop with an explicit scheduler.
func runWith(t *testing.T, st *fakeStore, c *fakeClaimer, sch Scheduler, reg provider.Registry) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.stop = cancel

	w := New(st, c, sch, &fakeReclaimer{}, reg, discardLogger(), Config{
		ClaimTimeout: time.Millisecond,
		// Long enough that the background loops never fire during a test; they
		// are covered separately.
		PromoteEvery:   time.Hour,
		HeartbeatEvery: time.Hour,
		LivenessTTL:    3 * time.Hour,
		ReclaimEvery:   time.Hour,
		Policy:         retry.Policy{Base: time.Second, Max: time.Minute},
	})
	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestProcess(t *testing.T) {
	deliverErr := errors.New("smtp refused")

	tests := []struct {
		name string
		// row is the notification in the store; zero ID means "not stored",
		// exercising the claimed-but-missing path.
		row          *store.Notification
		providerErr  error
		registry     func(p *fakeProvider) provider.Registry
		wantStatuses []store.Status
		wantDelivers int
		wantAcked    bool
	}{
		{
			name:         "successful delivery marks delivering then delivered",
			row:          ptr(notification(uuid.New(), store.StatusQueued, store.ChannelEmail)),
			wantStatuses: []store.Status{store.StatusDelivering, store.StatusDelivered},
			wantDelivers: 1,
			wantAcked:    true,
		},
		{
			name:         "provider failure marks failed and still acks",
			row:          ptr(notification(uuid.New(), store.StatusQueued, store.ChannelEmail)),
			providerErr:  deliverErr,
			wantStatuses: []store.Status{store.StatusDelivering, store.StatusFailed},
			wantDelivers: 1,
			wantAcked:    true,
		},
		{
			name: "already delivered is skipped, never re-sent",
			// The at-least-once duplicate-claim case: a second claim of a row
			// that already reached a terminal state must not send again.
			row:          ptr(notification(uuid.New(), store.StatusDelivered, store.ChannelEmail)),
			wantStatuses: nil,
			wantDelivers: 0,
			wantAcked:    true,
		},
		{
			name:         "dead lettered is skipped",
			row:          ptr(notification(uuid.New(), store.StatusDeadLettered, store.ChannelEmail)),
			wantStatuses: nil,
			wantDelivers: 0,
			wantAcked:    true,
		},
		{
			name:         "missing row is discarded",
			row:          nil,
			wantStatuses: nil,
			wantDelivers: 0,
			wantAcked:    true,
		},
		{
			name: "unregistered channel marks failed without delivering",
			row:  ptr(notification(uuid.New(), store.StatusQueued, store.ChannelSMS)),
			registry: func(p *fakeProvider) provider.Registry {
				// Only email is registered; the row is sms.
				return provider.Registry{string(store.ChannelEmail): p}
			},
			wantStatuses: []store.Status{store.StatusFailed},
			wantDelivers: 0,
			wantAcked:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProvider{err: tt.providerErr}

			var st *fakeStore
			id := uuid.New()
			if tt.row != nil {
				id = tt.row.ID
				st = newFakeStore(*tt.row)
			} else {
				st = newFakeStore()
			}

			reg := provider.Registry{
				string(store.ChannelEmail): p,
				string(store.ChannelSMS):   p,
				string(store.ChannelPush):  p,
			}
			if tt.registry != nil {
				reg = tt.registry(p)
			}

			c := &fakeClaimer{ids: []uuid.UUID{id}}
			runOnce(t, st, c, reg)

			if got := len(p.delivered); got != tt.wantDelivers {
				t.Errorf("delivered %d messages, want %d", got, tt.wantDelivers)
			}
			if !equalStatuses(st.statuses, tt.wantStatuses) {
				t.Errorf("status transitions = %v, want %v", st.statuses, tt.wantStatuses)
			}
			acked := len(c.acked) == 1 && c.acked[0] == id
			if acked != tt.wantAcked {
				t.Errorf("acked = %v, want %v (acked ids: %v)", acked, tt.wantAcked, c.acked)
			}
		})
	}
}

// TestProcessDoesNotAckWhenOutcomeIsNotDurable covers the rule the whole
// reliable-queue design rests on: if the outcome could not be written to
// Postgres, the claim must stay on the processing list so it can be reclaimed.
func TestProcessDoesNotAckWhenOutcomeIsNotDurable(t *testing.T) {
	tests := []struct {
		name       string
		getErr     error
		updateErr  error
		failStatus store.Status
	}{
		{
			name:   "load failure leaves claim outstanding",
			getErr: errors.New("connection refused"),
		},
		{
			name:       "delivering write failure leaves claim outstanding",
			updateErr:  errors.New("connection refused"),
			failStatus: store.StatusDelivering,
		},
		{
			name:       "terminal write failure leaves claim outstanding",
			updateErr:  errors.New("connection refused"),
			failStatus: store.StatusDelivered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
			st := newFakeStore(row)
			st.getErr = tt.getErr
			st.updateErr = tt.updateErr
			st.failStatus = tt.failStatus

			p := &fakeProvider{}
			c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
			runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): p})

			if len(c.acked) != 0 {
				t.Errorf("acked %v, want no ack while the outcome is not durable", c.acked)
			}
		})
	}
}

// TestFailureSchedulesRetry covers the retry path: a failure below the ceiling
// increments attempts, marks the row failed, and defers it rather than putting
// it straight back on the ready queue (which would spin without any backoff).
func TestFailureSchedulesRetry(t *testing.T) {
	row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
	row.Attempts = 1
	row.MaxAttempts = 5
	st := newFakeStore(row)

	p := &fakeProvider{err: errors.New("smtp refused")}
	c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
	sch := runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): p})

	got := st.byID[row.ID]
	if got.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", got.Attempts)
	}
	if got.Status != store.StatusFailed {
		t.Errorf("status = %s, want %s", got.Status, store.StatusFailed)
	}
	at, ok := sch.scheduled[row.ID]
	if !ok {
		t.Fatal("retry was not scheduled")
	}
	if !at.After(time.Now()) {
		t.Errorf("retry scheduled at %v, want a time in the future", at)
	}
	if len(c.acked) != 1 {
		t.Errorf("acked %v, want the claim released once the retry is durable", c.acked)
	}
}

// TestFailureDeadLettersAtCeiling covers the other branch: the final attempt
// dead-letters instead of scheduling yet another retry.
func TestFailureDeadLettersAtCeiling(t *testing.T) {
	row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
	row.Attempts = 4
	row.MaxAttempts = 5
	st := newFakeStore(row)

	p := &fakeProvider{err: errors.New("smtp refused")}
	c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
	sch := runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): p})

	got := st.byID[row.ID]
	if got.Status != store.StatusDeadLettered {
		t.Errorf("status = %s, want %s", got.Status, store.StatusDeadLettered)
	}
	if len(sch.scheduled) != 0 {
		t.Errorf("scheduled %v, want no retry after the ceiling is reached", sch.scheduled)
	}
	if len(c.acked) != 1 {
		t.Errorf("acked %v, want the claim released once dead-lettered", c.acked)
	}
}

// TestRetryNotAckedWhenNotDurable extends the durability rule to the retry
// path: if the failure cannot be recorded, or the retry cannot be scheduled,
// the claim must stay outstanding.
func TestRetryNotAckedWhenNotDurable(t *testing.T) {
	tests := []struct {
		name        string
		recordErr   error
		scheduleErr error
	}{
		{name: "record failure errors", recordErr: errors.New("connection refused")},
		{name: "scheduling the retry errors", scheduleErr: errors.New("redis down")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
			row.Attempts = 1
			st := newFakeStore(row)
			st.recordFailureErr = tt.recordErr

			sch := newFakeScheduler()
			sch.err = tt.scheduleErr

			p := &fakeProvider{err: errors.New("smtp refused")}
			c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
			runWith(t, st, c, sch, provider.Registry{string(store.ChannelEmail): p})

			if len(c.acked) != 0 {
				t.Errorf("acked %v, want no ack while the retry is not durable", c.acked)
			}
		})
	}
}

// TestAttemptHistoryIsRecorded covers the per-attempt audit trail on both the
// success and failure paths.
func TestAttemptHistoryIsRecorded(t *testing.T) {
	deliverErr := errors.New("smtp refused")

	tests := []struct {
		name        string
		providerErr error
		priorTries  int
		wantOutcome store.AttemptOutcome
		wantNumber  int
		wantError   string
	}{
		{
			name:        "successful delivery records a succeeded attempt",
			priorTries:  0,
			wantOutcome: store.AttemptSucceeded,
			wantNumber:  1,
		},
		{
			name:        "failed delivery records the provider error",
			providerErr: deliverErr,
			priorTries:  0,
			wantOutcome: store.AttemptFailed,
			wantNumber:  1,
			wantError:   "smtp refused",
		},
		{
			name:        "attempt number follows the notification's counter",
			providerErr: deliverErr,
			priorTries:  2,
			wantOutcome: store.AttemptFailed,
			wantNumber:  3,
			wantError:   "smtp refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
			row.Attempts = tt.priorTries
			st := newFakeStore(row)

			p := &fakeProvider{err: tt.providerErr}
			c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
			runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): p})

			if len(st.attempts) != 1 {
				t.Fatalf("recorded %d attempts, want 1", len(st.attempts))
			}
			got := st.attempts[0]
			if got.NotificationID != row.ID {
				t.Errorf("notification_id = %s, want %s", got.NotificationID, row.ID)
			}
			if got.Outcome != tt.wantOutcome {
				t.Errorf("outcome = %s, want %s", got.Outcome, tt.wantOutcome)
			}
			if got.AttemptNumber != tt.wantNumber {
				t.Errorf("attempt_number = %d, want %d", got.AttemptNumber, tt.wantNumber)
			}
			if got.Error != tt.wantError {
				t.Errorf("error = %q, want %q", got.Error, tt.wantError)
			}
			if got.FinishedAt.Before(got.StartedAt) {
				t.Errorf("finished_at %v is before started_at %v", got.FinishedAt, got.StartedAt)
			}
		})
	}
}

// TestAttemptHistoryFailureDoesNotBlockDelivery pins the rule that the audit
// trail is observational: if history cannot be written, the delivery still
// completes and is still acked.
func TestAttemptHistoryFailureDoesNotBlockDelivery(t *testing.T) {
	row := notification(uuid.New(), store.StatusQueued, store.ChannelEmail)
	st := newFakeStore(row)
	st.recordAttemptErr = errors.New("connection refused")

	p := &fakeProvider{}
	c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
	runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): p})

	if got := st.byID[row.ID].Status; got != store.StatusDelivered {
		t.Errorf("status = %s, want %s despite the history write failing", got, store.StatusDelivered)
	}
	if len(c.acked) != 1 {
		t.Errorf("acked %v, want the claim released despite the history write failing", c.acked)
	}
}

// TestNoAttemptRecordedWithoutADelivery confirms paths that never call a
// provider do not fabricate history.
func TestNoAttemptRecordedWithoutADelivery(t *testing.T) {
	// An unregistered channel fails before any provider call.
	row := notification(uuid.New(), store.StatusQueued, store.ChannelSMS)
	st := newFakeStore(row)

	c := &fakeClaimer{ids: []uuid.UUID{row.ID}}
	runOnce(t, st, c, provider.Registry{string(store.ChannelEmail): &fakeProvider{}})

	if len(st.attempts) != 0 {
		t.Errorf("recorded %d attempts, want 0 when no provider was ever called", len(st.attempts))
	}
}

// TestPromoterRunsAndStops verifies the promoter sweeps on its interval and
// shuts down with the worker.
func TestPromoterRunsAndStops(t *testing.T) {
	promoted := make(chan struct{}, 1)
	sch := &countingScheduler{onPromote: func() {
		select {
		case promoted <- struct{}{}:
		default:
		}
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &fakeClaimer{stop: func() {}}
	w := New(newFakeStore(), c, sch, &fakeReclaimer{}, provider.Registry{}, discardLogger(), Config{
		ClaimTimeout:   time.Millisecond,
		PromoteEvery:   5 * time.Millisecond,
		HeartbeatEvery: time.Hour,
		LivenessTTL:    3 * time.Hour,
		ReclaimEvery:   time.Hour,
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-promoted:
	case <-time.After(2 * time.Second):
		t.Fatal("promoter never swept")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// countingScheduler signals each promote sweep.
type countingScheduler struct {
	onPromote func()
}

func (s *countingScheduler) Schedule(context.Context, uuid.UUID, time.Time) error { return nil }

func (s *countingScheduler) PromoteDue(context.Context, time.Time, int64) (int, error) {
	s.onPromote()
	return 0, nil
}

// TestStartupRecoversOwnClaims covers the restart case: a worker coming back
// under the same id must return its own leftover claims to the ready queue
// immediately, rather than leaving them stranded until a liveness key lapses.
func TestStartupRecoversOwnClaims(t *testing.T) {
	c := &fakeClaimer{stop: func() {}, drainCount: 3}
	rec := &fakeReclaimer{}

	ctx, cancel := context.WithCancel(context.Background())
	c.stop = cancel

	w := New(newFakeStore(), c, newFakeScheduler(), rec, provider.Registry{}, discardLogger(), Config{
		ClaimTimeout:   time.Millisecond,
		PromoteEvery:   time.Hour,
		HeartbeatEvery: time.Hour,
		LivenessTTL:    3 * time.Hour,
		ReclaimEvery:   time.Hour,
		WorkerID:       "worker-a",
	})
	if err := w.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if c.drainCalls != 1 {
		t.Errorf("Drain called %d times, want exactly 1 at startup", c.drainCalls)
	}

	// Liveness must be published before the worker starts working, or another
	// worker's sweep could reclaim this one's list out from under it.
	heartbeats, _ := rec.counts()
	if heartbeats < 1 {
		t.Error("no heartbeat published at startup, want liveness before any claim")
	}
	if rec.lastWorkerID != "worker-a" {
		t.Errorf("heartbeat worker id = %q, want %q", rec.lastWorkerID, "worker-a")
	}
}

// TestHeartbeatAndReclaimLoopsRun verifies both background loops tick and stop
// with the worker.
func TestHeartbeatAndReclaimLoopsRun(t *testing.T) {
	swept := make(chan struct{}, 1)
	rec := &fakeReclaimer{onSweep: func() {
		select {
		case swept <- struct{}{}:
		default:
		}
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &fakeClaimer{stop: func() {}}
	w := New(newFakeStore(), c, newFakeScheduler(), rec, provider.Registry{}, discardLogger(), Config{
		ClaimTimeout:   time.Millisecond,
		PromoteEvery:   time.Hour,
		HeartbeatEvery: 5 * time.Millisecond,
		LivenessTTL:    time.Second,
		ReclaimEvery:   5 * time.Millisecond,
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatal("reclaim sweep never ran")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	heartbeats, sweeps := rec.counts()
	if heartbeats < 2 {
		t.Errorf("heartbeats = %d, want the loop to have ticked at least once beyond startup", heartbeats)
	}
	if sweeps < 1 {
		t.Errorf("reclaim sweeps = %d, want at least 1", sweeps)
	}
}

// TestLivenessTTLIsWidenedWhenTooTight guards a configuration that would make
// every worker continuously declare itself dead: if the key expires faster than
// it is refreshed, it is never present when another worker looks.
func TestLivenessTTLIsWidenedWhenTooTight(t *testing.T) {
	tests := []struct {
		name      string
		heartbeat time.Duration
		ttl       time.Duration
		wantTTL   time.Duration
	}{
		{name: "ttl below heartbeat is widened", heartbeat: 10 * time.Second, ttl: time.Second, wantTTL: 30 * time.Second},
		{name: "ttl equal to heartbeat is widened", heartbeat: 10 * time.Second, ttl: 10 * time.Second, wantTTL: 30 * time.Second},
		{name: "comfortable ttl is left alone", heartbeat: 5 * time.Second, ttl: 30 * time.Second, wantTTL: 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := New(newFakeStore(), &fakeClaimer{}, newFakeScheduler(), &fakeReclaimer{},
				provider.Registry{}, discardLogger(), Config{
					HeartbeatEvery: tt.heartbeat,
					LivenessTTL:    tt.ttl,
				})
			if w.livenessTTL != tt.wantTTL {
				t.Errorf("livenessTTL = %v, want %v", w.livenessTTL, tt.wantTTL)
			}
		})
	}
}

// TestRunStopsOnContextCancel confirms shutdown is observed rather than
// requiring the process to be killed.
func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeClaimer{stop: func() {}}
	w := New(newFakeStore(), c, newFakeScheduler(), &fakeReclaimer{}, provider.Registry{}, discardLogger(), Config{
		ClaimTimeout:   time.Millisecond,
		PromoteEvery:   time.Hour,
		HeartbeatEvery: time.Hour,
		LivenessTTL:    3 * time.Hour,
		ReclaimEvery:   time.Hour,
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func ptr(n store.Notification) *store.Notification { return &n }

func equalStatuses(got, want []store.Status) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
