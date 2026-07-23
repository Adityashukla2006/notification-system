package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// clearReclaim wipes the ready queue and EVERY processing and liveness key.
//
// Clearing only the workers a given test names is not enough: ReclaimAbandoned
// deliberately scans every processing list in Redis, so a list left behind by
// any other test is real work as far as it is concerned, and would be reclaimed
// into this test's results.
func clearReclaim(t *testing.T, r *Reclaimer) {
	t.Helper()
	ctx := context.Background()

	keys := []string{r.queueKey}
	for _, pattern := range []string{r.processingPrefix + "*", r.livenessPrefix + "*"} {
		found, err := r.client.Keys(ctx, pattern).Result()
		if err != nil {
			t.Fatalf("listing %s: %v", pattern, err)
		}
		keys = append(keys, found...)
	}

	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}
}

// TestReclaimAbandonedRequeuesDeadWorkersClaims is the property the whole
// reliable-claim design depends on: work held by a worker that died must come
// back, not sit in its processing list forever.
func TestReclaimAbandonedRequeuesDeadWorkersClaims(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const dead = "worker-dead"
	clearReclaim(t, r)

	// A dead worker: claims outstanding, no liveness key.
	deadConsumer := NewConsumer(client, dead)
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	for _, id := range ids {
		if err := client.LPush(ctx, deadConsumer.processingKey, id.String()).Err(); err != nil {
			t.Fatalf("seeding processing list: %v", err)
		}
	}

	notifications, workers, err := r.ReclaimAbandoned(ctx)
	if err != nil {
		t.Fatalf("ReclaimAbandoned: %v", err)
	}
	if notifications != 2 {
		t.Errorf("reclaimed %d notifications, want 2", notifications)
	}
	if workers != 1 {
		t.Errorf("drained %d workers, want 1", workers)
	}

	length, err := client.LLen(ctx, deadConsumer.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen processing: %v", err)
	}
	if length != 0 {
		t.Errorf("dead worker's processing list still holds %d entries, want 0", length)
	}

	queued, err := client.LLen(ctx, r.queueKey).Result()
	if err != nil {
		t.Fatalf("LLen queue: %v", err)
	}
	if queued != 2 {
		t.Errorf("ready queue holds %d entries, want 2", queued)
	}
}

// TestReclaimAbandonedLeavesLiveWorkersAlone is the other half: reclaiming from
// a worker that is merely busy would deliver its in-flight notification twice.
func TestReclaimAbandonedLeavesLiveWorkersAlone(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const alive = "worker-alive"
	clearReclaim(t, r)

	liveConsumer := NewConsumer(client, alive)
	if err := client.LPush(ctx, liveConsumer.processingKey, uuid.NewString()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}
	if err := r.Heartbeat(ctx, alive, time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	notifications, _, err := r.ReclaimAbandoned(ctx)
	if err != nil {
		t.Fatalf("ReclaimAbandoned: %v", err)
	}
	if notifications != 0 {
		t.Errorf("reclaimed %d notifications from a live worker, want 0", notifications)
	}

	length, err := client.LLen(ctx, liveConsumer.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if length != 1 {
		t.Errorf("live worker's processing list holds %d entries, want its claim untouched (1)", length)
	}
}

// TestHeartbeatExpiryMakesClaimsReclaimable ties the two together: a worker
// becomes reclaimable precisely when it stops refreshing its liveness key.
func TestHeartbeatExpiryMakesClaimsReclaimable(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const id = "worker-expiring"
	clearReclaim(t, r)

	consumer := NewConsumer(client, id)
	if err := client.LPush(ctx, consumer.processingKey, uuid.NewString()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}

	// A short TTL stands in for a worker that stopped heartbeating. One second
	// is the floor: Redis rejects sub-second expiry on SET EX, and go-redis
	// silently rounds anything smaller up to it.
	if err := r.Heartbeat(ctx, id, time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// While alive, nothing is reclaimed.
	if n, _, err := r.ReclaimAbandoned(ctx); err != nil {
		t.Fatalf("ReclaimAbandoned (alive): %v", err)
	} else if n != 0 {
		t.Fatalf("reclaimed %d while the worker was alive, want 0", n)
	}

	// Wait for the key to lapse, then the same claim is fair game.
	deadline := time.Now().Add(5 * time.Second)
	var reclaimed int
	for time.Now().Before(deadline) {
		n, _, err := r.ReclaimAbandoned(ctx)
		if err != nil {
			t.Fatalf("ReclaimAbandoned: %v", err)
		}
		if n > 0 {
			reclaimed = n
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if reclaimed != 1 {
		t.Errorf("reclaimed %d after the liveness key expired, want 1", reclaimed)
	}
}

// TestDrainRecoversOwnClaims covers the restart path, where a worker recovers
// its own leftovers without waiting for its liveness key to expire.
func TestDrainRecoversOwnClaims(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const id = "worker-restarting"
	clearReclaim(t, r)

	c := NewConsumer(client, id)
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	for _, s := range ids {
		if err := client.LPush(ctx, c.processingKey, s).Err(); err != nil {
			t.Fatalf("seeding processing list: %v", err)
		}
	}

	moved, err := c.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if moved != 3 {
		t.Errorf("drained %d, want 3", moved)
	}

	length, err := client.LLen(ctx, c.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen processing: %v", err)
	}
	if length != 0 {
		t.Errorf("processing list holds %d after Drain, want 0", length)
	}
	queued, err := client.LLen(ctx, c.queueKey).Result()
	if err != nil {
		t.Fatalf("LLen queue: %v", err)
	}
	if queued != 3 {
		t.Errorf("ready queue holds %d, want 3", queued)
	}
}

// TestDrainOnEmptyListIsANoOp guards the common case: most restarts have
// nothing outstanding.
func TestDrainOnEmptyListIsANoOp(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const id = "worker-clean"
	clearReclaim(t, r)

	moved, err := NewConsumer(client, id).Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if moved != 0 {
		t.Errorf("drained %d from an empty list, want 0", moved)
	}
}

// TestReclaimedWorkIsClaimedFirst verifies reclaimed notifications go to the
// end of the queue that Claim reads from, so work that already survived a
// worker's death is not stuck behind everything enqueued since.
func TestReclaimedWorkIsClaimedFirst(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	r := NewReclaimer(client)
	const dead = "worker-dead-order"
	clearReclaim(t, r)

	q := New(client)
	fresh := uuid.New()
	if err := q.Enqueue(ctx, fresh); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	abandoned := uuid.New()
	deadConsumer := NewConsumer(client, dead)
	if err := client.LPush(ctx, deadConsumer.processingKey, abandoned.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}

	if _, _, err := r.ReclaimAbandoned(ctx); err != nil {
		t.Fatalf("ReclaimAbandoned: %v", err)
	}

	// Deliberately not clearing here: the ready queue now holds both the fresh
	// and the reclaimed id, and clearing would wipe exactly what is under test.
	claimer := NewConsumer(client, "worker-consumer")
	got, err := claimer.Claim(ctx, time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != abandoned {
		t.Errorf("claimed %s, want the reclaimed notification %s first", got, abandoned)
	}
}
