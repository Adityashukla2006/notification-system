package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
	getErr           error
	updateErr        error
	recordFailureErr error
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
	ids    []uuid.UUID
	acked  []uuid.UUID
	ackErr error
	stop   context.CancelFunc
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

	w := New(st, c, sch, reg, discardLogger(), Config{
		ClaimTimeout: time.Millisecond,
		// Long enough that the promoter never fires during a test; promotion
		// is covered separately.
		PromoteEvery: time.Hour,
		Policy:       retry.Policy{Base: time.Second, Max: time.Minute},
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
	w := New(newFakeStore(), c, sch, provider.Registry{}, discardLogger(), Config{
		ClaimTimeout: time.Millisecond,
		PromoteEvery: 5 * time.Millisecond,
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

// TestRunStopsOnContextCancel confirms shutdown is observed rather than
// requiring the process to be killed.
func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeClaimer{stop: func() {}}
	w := New(newFakeStore(), c, newFakeScheduler(), provider.Registry{}, discardLogger(), Config{
		ClaimTimeout: time.Millisecond,
		PromoteEvery: time.Hour,
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
