package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestReclaimAbandonedRequeuesDeadWorkersClaims is the property the whole
// reliable-claim design depends on: work held by a worker that died must come
// back, not sit in its processing list forever — and with per-tenant queues it
// must come back on its OWN tenant's queue.
func TestReclaimAbandonedRequeuesDeadWorkersClaims(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	r := NewReclaimer(client)
	dead := NewConsumer(client, "worker-dead")
	a, b := Item{Tenant: uuid.New(), ID: uuid.New()}, Item{Tenant: uuid.New(), ID: uuid.New()}
	for _, it := range []Item{a, b} {
		if err := client.LPush(ctx, dead.processingKey, it.String()).Err(); err != nil {
			t.Fatalf("seeding processing list: %v", err)
		}
	}

	notifications, workers, err := r.ReclaimAbandoned(ctx)
	if err != nil {
		t.Fatalf("ReclaimAbandoned: %v", err)
	}
	if notifications != 2 || workers != 1 {
		t.Errorf("reclaimed %d notifications from %d workers, want 2 from 1", notifications, workers)
	}
	if n, _ := client.LLen(ctx, dead.processingKey).Result(); n != 0 {
		t.Errorf("dead worker's processing list still holds %d entries, want 0", n)
	}
	for _, it := range []Item{a, b} {
		got, err := client.LRange(ctx, tenantQueuePrefix+it.Tenant.String(), 0, -1).Result()
		if err != nil {
			t.Fatalf("LRange: %v", err)
		}
		if len(got) != 1 || got[0] != it.String() {
			t.Errorf("tenant %s queue = %v, want exactly [%s]", it.Tenant, got, it)
		}
	}

	// And they are claimable again through the rotation.
	c := NewConsumer(client, "worker-survivor")
	seen := map[uuid.UUID]bool{claim(t, c).ID: true, claim(t, c).ID: true}
	if !seen[a.ID] || !seen[b.ID] {
		t.Errorf("claimed %v after reclaim, want both %s and %s", seen, a.ID, b.ID)
	}
}

// TestReclaimAbandonedLeavesLiveWorkersAlone is the other half: reclaiming from
// a worker that is merely busy would deliver its in-flight notification twice.
func TestReclaimAbandonedLeavesLiveWorkersAlone(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	r := NewReclaimer(client)
	const alive = "worker-alive"
	live := NewConsumer(client, alive)
	it := Item{Tenant: uuid.New(), ID: uuid.New()}
	if err := client.LPush(ctx, live.processingKey, it.String()).Err(); err != nil {
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
	if n, _ := client.LLen(ctx, live.processingKey).Result(); n != 1 {
		t.Errorf("live worker's processing list holds %d entries, want its claim untouched (1)", n)
	}
}

// TestHeartbeatExpiryMakesClaimsReclaimable ties the two together: a worker
// becomes reclaimable precisely when it stops refreshing its liveness key.
func TestHeartbeatExpiryMakesClaimsReclaimable(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	r := NewReclaimer(client)
	const id = "worker-expiring"
	consumer := NewConsumer(client, id)
	if err := client.LPush(ctx, consumer.processingKey, Item{Tenant: uuid.New(), ID: uuid.New()}.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}

	// A short TTL stands in for a worker that stopped heartbeating. One second
	// is the floor: Redis rejects sub-second expiry on SET EX, and go-redis
	// silently rounds anything smaller up to it.
	if err := r.Heartbeat(ctx, id, time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if n, _, err := r.ReclaimAbandoned(ctx); err != nil {
		t.Fatalf("ReclaimAbandoned (alive): %v", err)
	} else if n != 0 {
		t.Fatalf("reclaimed %d while the worker was alive, want 0", n)
	}

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

// TestCrashRecoveryOnTenantPath is the end-to-end crash story on the new claim
// path: a worker claims through the rotation and dies without acking; when it
// restarts under the same id, Drain returns the claim to its tenant's queue and
// another worker delivers it. Nothing is lost at any step.
func TestCrashRecoveryOnTenantPath(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	tenant := uuid.New()
	it := Item{Tenant: tenant, ID: uuid.New()}
	enqueue(t, New(client), it)

	// Claim, then "crash": no Ack. The entry lives only on the processing list.
	crashed := NewConsumer(client, "worker-crash")
	claim(t, crashed)
	if n, _ := client.LLen(ctx, tenantQueuePrefix+tenant.String()).Result(); n != 0 {
		t.Fatalf("tenant queue holds %d after the claim, want 0", n)
	}

	// Restart under the same id.
	moved, err := NewConsumer(client, "worker-crash").Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if moved != 1 {
		t.Fatalf("Drain moved %d, want 1", moved)
	}

	got := claim(t, NewConsumer(client, "worker-other"))
	if got.ID != it.ID || got.Tenant != tenant {
		t.Errorf("claimed %+v after recovery, want %+v", got, it)
	}
}

// TestDrainRecoversOwnClaims covers the restart path, where a worker recovers
// its own leftovers without waiting for its liveness key to expire.
func TestDrainRecoversOwnClaims(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	c := NewConsumer(client, "worker-restarting")
	tenant := uuid.New()
	for _, it := range itemsFor(tenant, 3) {
		if err := client.LPush(ctx, c.processingKey, it.String()).Err(); err != nil {
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
	if n, _ := client.LLen(ctx, c.processingKey).Result(); n != 0 {
		t.Errorf("processing list holds %d after Drain, want 0", n)
	}
	if n, _ := client.LLen(ctx, tenantQueuePrefix+tenant.String()).Result(); n != 3 {
		t.Errorf("tenant queue holds %d, want 3", n)
	}
}

// TestDrainRecoversLegacyEntry: a bare id left on a processing list by an
// older deployment is recovered too, onto the nil tenant's queue, and stays
// claimable.
func TestDrainRecoversLegacyEntry(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	c := NewConsumer(client, "worker-legacy-restart")
	id := uuid.New()
	if err := client.LPush(ctx, c.processingKey, id.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}
	if moved, err := c.Drain(ctx); err != nil || moved != 1 {
		t.Fatalf("Drain = %d, %v; want 1, nil", moved, err)
	}
	if got := claim(t, NewConsumer(client, "worker-legacy-other")); got.ID != id {
		t.Errorf("claimed %s, want the recovered legacy id %s", got.ID, id)
	}
}

// TestDrainOnEmptyListIsANoOp guards the common case: most restarts have
// nothing outstanding.
func TestDrainOnEmptyListIsANoOp(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)

	moved, err := NewConsumer(client, "worker-clean").Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if moved != 0 {
		t.Errorf("drained %d from an empty list, want 0", moved)
	}
}

// TestReclaimedWorkIsClaimedFirst verifies reclaimed notifications go to the
// end of their tenant's queue that Claim reads from, so work that already
// survived a worker's death is not stuck behind everything enqueued since.
func TestReclaimedWorkIsClaimedFirst(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	tenant := uuid.New()
	fresh := Item{Tenant: tenant, ID: uuid.New()}
	enqueue(t, New(client), fresh)

	abandoned := Item{Tenant: tenant, ID: uuid.New()}
	dead := NewConsumer(client, "worker-dead-order")
	if err := client.LPush(ctx, dead.processingKey, abandoned.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}
	if _, _, err := NewReclaimer(client).ReclaimAbandoned(ctx); err != nil {
		t.Fatalf("ReclaimAbandoned: %v", err)
	}

	if got := claim(t, NewConsumer(client, "worker-consumer")); got.ID != abandoned.ID {
		t.Errorf("claimed %s, want the reclaimed notification %s first", got.ID, abandoned.ID)
	}
}
