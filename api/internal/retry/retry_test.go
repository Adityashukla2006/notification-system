package retry

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	policy := Policy{Base: time.Second, Max: time.Minute}

	tests := []struct {
		name    string
		policy  Policy
		attempt int
		random  float64
		want    time.Duration
	}{
		// Equal jitter: half the exponential delay is fixed, half scales with
		// the random draw. random=0 is the floor, random=1 the ceiling.
		{name: "first attempt floor", policy: policy, attempt: 1, random: 0, want: 500 * time.Millisecond},
		{name: "first attempt ceiling", policy: policy, attempt: 1, random: 1, want: time.Second},
		{name: "second attempt doubles", policy: policy, attempt: 2, random: 0, want: time.Second},
		{name: "third attempt doubles again", policy: policy, attempt: 3, random: 0, want: 2 * time.Second},
		{name: "growth is capped", policy: policy, attempt: 20, random: 0, want: 30 * time.Second},
		{name: "capped ceiling never exceeds max", policy: policy, attempt: 20, random: 1, want: time.Minute},

		// Guard rails.
		{name: "attempt below one is treated as the first", policy: policy, attempt: 0, random: 0, want: 500 * time.Millisecond},
		{name: "zero policy falls back to defaults", policy: Policy{}, attempt: 1, random: 0, want: 500 * time.Millisecond},

		// A huge attempt number must clamp, not overflow into a negative or
		// nonsensical delay that would schedule a retry in the past.
		{name: "absurd attempt clamps to max", policy: policy, attempt: 5000, random: 0, want: 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.backoff(tt.attempt, func() float64 { return tt.random })
			if got != tt.want {
				t.Errorf("backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

// TestBackoffIsAlwaysPositive is the property that matters most: a
// non-positive delay would schedule a retry at or before now, turning backoff
// into a hot loop.
func TestBackoffIsAlwaysPositive(t *testing.T) {
	policy := Policy{Base: time.Second, Max: time.Minute}
	for attempt := 0; attempt < 200; attempt++ {
		for _, r := range []float64{0, 0.5, 1} {
			if got := policy.backoff(attempt, func() float64 { return r }); got <= 0 {
				t.Fatalf("backoff(%d) with random=%v = %v, want > 0", attempt, r, got)
			}
		}
	}
}

func TestBackoffUsesRealRandomness(t *testing.T) {
	policy := Policy{Base: time.Second, Max: time.Minute}
	// Exercise the exported path to be sure it is wired to a real source.
	// Attempt 3 is base*2^2 = 4s, so equal jitter puts it within [2s, 4s].
	if got := policy.Backoff(3); got < 2*time.Second || got > 4*time.Second {
		t.Errorf("Backoff(3) = %v, want within [2s, 4s]", got)
	}
}

func TestExhausted(t *testing.T) {
	tests := []struct {
		name        string
		attempts    int
		maxAttempts int
		want        bool
	}{
		{name: "below ceiling", attempts: 1, maxAttempts: 5, want: false},
		{name: "one short of ceiling", attempts: 4, maxAttempts: 5, want: false},
		{name: "at ceiling", attempts: 5, maxAttempts: 5, want: true},
		{name: "past ceiling", attempts: 6, maxAttempts: 5, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Exhausted(tt.attempts, tt.maxAttempts); got != tt.want {
				t.Errorf("Exhausted(%d, %d) = %v, want %v", tt.attempts, tt.maxAttempts, got, tt.want)
			}
		})
	}
}
