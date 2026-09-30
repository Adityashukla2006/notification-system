package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSimulatedDeliver(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		cfg           SimulatedConfig
		random        float64
		now           time.Time
		wantErr       error
		wantPermanent bool
	}{
		{
			name:   "succeeds when no failure rate triggers",
			cfg:    SimulatedConfig{FailRate: 0.1, PermanentRate: 0.1},
			random: 0.5,
			now:    start,
		},
		{
			name:    "transient failure",
			cfg:     SimulatedConfig{FailRate: 0.6},
			random:  0.5,
			now:     start,
			wantErr: errSimTransient,
		},
		{
			name:          "permanent failure is marked permanent",
			cfg:           SimulatedConfig{PermanentRate: 0.6},
			random:        0.5,
			now:           start,
			wantErr:       errSimPermanent,
			wantPermanent: true,
		},
		{
			name:    "inside outage window fails",
			cfg:     SimulatedConfig{OutageAfter: time.Minute, OutageFor: time.Minute},
			random:  0.99,
			now:     start.Add(90 * time.Second),
			wantErr: errSimOutage,
		},
		{
			name:   "after outage window succeeds",
			cfg:    SimulatedConfig{OutageAfter: time.Minute, OutageFor: time.Minute},
			random: 0.99,
			now:    start.Add(2 * time.Minute),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.Start = start
			s := NewSimulated(tt.cfg)
			s.random = func() float64 { return tt.random }
			s.now = func() time.Time { return tt.now }

			err := s.Deliver(context.Background(), Message{})
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("Deliver() error = %v, want %v", err, tt.wantErr)
			}
			if got := IsPermanent(err); got != tt.wantPermanent {
				t.Fatalf("IsPermanent() = %v, want %v", got, tt.wantPermanent)
			}
		})
	}
}

func TestSimulatedTimeoutHonorsContext(t *testing.T) {
	s := NewSimulated(SimulatedConfig{TimeoutRate: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := s.Deliver(ctx, Message{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Deliver() error = %v, want deadline exceeded", err)
	}
	if IsPermanent(err) {
		t.Fatal("a timeout must be transient so it is retried")
	}
}

// fakeLedger records the ids it is told about, or fails every call.
type fakeLedger struct {
	ids []uuid.UUID
	err error
}

func (f *fakeLedger) RecordSend(_ context.Context, id uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.ids = append(f.ids, id)
	return nil
}

func TestSimulatedLedger(t *testing.T) {
	id := uuid.New()

	tests := []struct {
		name      string
		failRate  float64
		ledgerErr error
		wantErr   bool
		wantIDs   int
	}{
		{name: "success is recorded", wantIDs: 1},
		{name: "failure is not recorded", failRate: 1, wantErr: true},
		{name: "unrecorded success is reported as a transient failure", ledgerErr: errors.New("redis down"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger := &fakeLedger{err: tt.ledgerErr}
			s := NewSimulated(SimulatedConfig{FailRate: tt.failRate, Ledger: ledger})
			s.random = func() float64 { return 0.5 }

			err := s.Deliver(context.Background(), Message{ID: id})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Deliver() error = %v, wantErr %v", err, tt.wantErr)
			}
			if IsPermanent(err) {
				t.Fatal("a ledger or transient failure must not be permanent")
			}
			if len(ledger.ids) != tt.wantIDs {
				t.Fatalf("ledger recorded %d ids, want %d", len(ledger.ids), tt.wantIDs)
			}
			if tt.wantIDs == 1 && ledger.ids[0] != id {
				t.Fatalf("ledger recorded %s, want %s", ledger.ids[0], id)
			}
		})
	}
}
