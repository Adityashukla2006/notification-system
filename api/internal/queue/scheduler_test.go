package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// clearScheduler wipes the keys a scheduler test touches.
func clearScheduler(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.client.Del(context.Background(), s.scheduledKey, s.queueKey).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}
}

// TestPromoteDueOnlyPromotesWhatIsDue is the core scheduling property: a
// notification must not reach the ready queue before its time.
func TestPromoteDueOnlyPromotesWhatIsDue(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	s := NewScheduler(client)
	clearScheduler(t, s)

	now := time.Now()
	dueID := uuid.New()
	futureID := uuid.New()

	if err := s.Schedule(ctx, dueID, now.Add(-time.Minute)); err != nil {
		t.Fatalf("Schedule due: %v", err)
	}
	if err := s.Schedule(ctx, futureID, now.Add(time.Hour)); err != nil {
		t.Fatalf("Schedule future: %v", err)
	}

	n, err := s.PromoteDue(ctx, now, 100)
	if err != nil {
		t.Fatalf("PromoteDue: %v", err)
	}
	if n != 1 {
		t.Errorf("promoted %d, want 1", n)
	}

	queued, err := client.LRange(ctx, s.queueKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(queued) != 1 || queued[0] != dueID.String() {
		t.Errorf("ready queue = %v, want exactly [%s]", queued, dueID)
	}

	// The future one must still be waiting on the schedule.
	remaining, err := client.ZCard(ctx, s.scheduledKey).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if remaining != 1 {
		t.Errorf("schedule holds %d entries, want 1 (the future notification)", remaining)
	}
}

// TestPromoteDueIsIdempotentAcrossWorkers verifies the ZREM-as-a-claim rule:
// concurrent promoters must not each push the same id onto the ready queue.
func TestPromoteDueIsIdempotentAcrossWorkers(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	s := NewScheduler(client)
	clearScheduler(t, s)

	id := uuid.New()
	if err := s.Schedule(ctx, id, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	now := time.Now()
	total := 0
	// A second scheduler stands in for another worker sweeping the same set.
	for _, sweeper := range []*Scheduler{s, NewScheduler(client)} {
		n, err := sweeper.PromoteDue(ctx, now, 100)
		if err != nil {
			t.Fatalf("PromoteDue: %v", err)
		}
		total += n
	}

	if total != 1 {
		t.Errorf("promoted %d times in total, want exactly 1", total)
	}
	length, err := client.LLen(ctx, s.queueKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if length != 1 {
		t.Errorf("ready queue length = %d, want 1", length)
	}
}

// TestScheduleOverwritesDueTime confirms re-scheduling moves the due time
// rather than creating a second entry, which is what makes a retried Schedule
// call safe.
func TestScheduleOverwritesDueTime(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	s := NewScheduler(client)
	clearScheduler(t, s)

	id := uuid.New()
	if err := s.Schedule(ctx, id, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := s.Schedule(ctx, id, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("re-Schedule: %v", err)
	}

	count, err := client.ZCard(ctx, s.scheduledKey).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if count != 1 {
		t.Errorf("schedule holds %d entries, want 1", count)
	}

	n, err := s.PromoteDue(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("PromoteDue: %v", err)
	}
	if n != 1 {
		t.Errorf("promoted %d, want 1 (the due time should have moved into the past)", n)
	}
}

// TestPromoteDueRespectsLimit checks a large backlog drains in bounded batches.
func TestPromoteDueRespectsLimit(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	s := NewScheduler(client)
	clearScheduler(t, s)

	past := time.Now().Add(-time.Minute)
	for range 5 {
		if err := s.Schedule(ctx, uuid.New(), past); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	n, err := s.PromoteDue(ctx, time.Now(), 2)
	if err != nil {
		t.Fatalf("PromoteDue: %v", err)
	}
	if n != 2 {
		t.Errorf("promoted %d, want 2 (the limit)", n)
	}
}

func TestCancel(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	s := NewScheduler(client)
	clearScheduler(t, s)

	id := uuid.New()
	if err := s.Schedule(ctx, id, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	removed, err := s.Cancel(ctx, id)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !removed {
		t.Error("Cancel reported nothing removed, want true")
	}

	removed, err = s.Cancel(ctx, id)
	if err != nil {
		t.Fatalf("Cancel (second): %v", err)
	}
	if removed {
		t.Error("Cancel on an absent id reported removed, want false")
	}
}
