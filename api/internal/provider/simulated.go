package provider

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// Error messages the simulation produces. They are stable strings so a
// benchmark can classify recorded attempts by reading delivery_attempts.error.
var (
	errSimTransient = errors.New("simulated transient failure")
	errSimPermanent = errors.New("simulated permanent failure")
	errSimOutage    = errors.New("simulated outage")
)

// SimulatedConfig describes the behavior of a Simulated provider.
type SimulatedConfig struct {
	// Latency and Jitter set how long a call takes: uniform in
	// [Latency-Jitter, Latency+Jitter], never negative.
	Latency time.Duration
	Jitter  time.Duration
	// FailRate, PermanentRate, and TimeoutRate are independent probabilities
	// checked in that order after the latency elapses (timeouts before it).
	FailRate      float64
	PermanentRate float64
	TimeoutRate   float64
	// OutageAfter and OutageFor define a window relative to Start in which
	// every call fails transiently. OutageFor of zero disables it.
	OutageAfter time.Duration
	OutageFor   time.Duration
	// Start anchors the outage window. Zero means "now".
	Start time.Time
	// Ledger, when set, is told about every call that the simulation reports
	// as a success. Nil disables it.
	Ledger Ledger
}

// Ledger records each successful simulated send outside Postgres.
//
// It exists to count duplicate deliveries honestly. A worker killed after the
// provider accepted a message but before the outcome reached Postgres leaves no
// trace in the database, and the redelivery that follows looks like a first
// send. Only the provider side can see both, so the simulation keeps its own
// count per notification.
type Ledger interface {
	RecordSend(ctx context.Context, id uuid.UUID) error
}

// Simulated is a Provider that sends nothing and instead behaves like a
// downstream service with configurable latency and failure modes. It exists so
// retry, dead-letter, and recovery paths can be load tested deterministically
// without a real provider, and without sending real messages.
type Simulated struct {
	cfg    SimulatedConfig
	random func() float64
	now    func() time.Time
}

// NewSimulated constructs a Simulated provider.
func NewSimulated(cfg SimulatedConfig) *Simulated {
	if cfg.Start.IsZero() {
		cfg.Start = time.Now()
	}
	return &Simulated{cfg: cfg, random: rand.Float64, now: time.Now}
}

// Deliver waits out the simulated latency and then succeeds or fails according
// to the configured rates. A timeout blocks until ctx is done, exactly like a
// provider that accepted the connection and stopped responding.
func (s *Simulated) Deliver(ctx context.Context, msg Message) error {
	if s.inOutage() {
		return errSimOutage
	}

	if s.random() < s.cfg.TimeoutRate {
		<-ctx.Done()
		return fmt.Errorf("simulated timeout: %w", ctx.Err())
	}

	if d := s.latency(); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return fmt.Errorf("simulated call interrupted: %w", ctx.Err())
		case <-timer.C:
		}
	}

	if s.random() < s.cfg.PermanentRate {
		return Permanent(errSimPermanent)
	}
	if s.random() < s.cfg.FailRate {
		return errSimTransient
	}
	if s.cfg.Ledger != nil {
		// An unrecorded success would make the duplicate count undercount, so
		// report it as a transient failure: the send is retried and recorded.
		if err := s.cfg.Ledger.RecordSend(ctx, msg.ID); err != nil {
			return fmt.Errorf("simulated send not recorded in ledger: %w", err)
		}
	}
	return nil
}

// inOutage reports whether the current time falls in the configured outage.
func (s *Simulated) inOutage() bool {
	if s.cfg.OutageFor <= 0 {
		return false
	}
	start := s.cfg.Start.Add(s.cfg.OutageAfter)
	now := s.now()
	return !now.Before(start) && now.Before(start.Add(s.cfg.OutageFor))
}

// latency draws one call duration.
func (s *Simulated) latency() time.Duration {
	if s.cfg.Jitter <= 0 {
		return s.cfg.Latency
	}
	offset := time.Duration((s.random()*2 - 1) * float64(s.cfg.Jitter))
	return max(s.cfg.Latency+offset, 0)
}
