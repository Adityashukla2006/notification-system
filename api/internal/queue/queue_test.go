package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Adityashukla2006/notification-system/api/internal/testenv"
)

// requireRedis returns a client for the test Redis. Without TEST_REDIS_ADDR it
// skips locally and fails in CI, where a silently skipped integration test
// would report a green build that verified nothing.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := testenv.RequireOrSkip(t, "TEST_REDIS_ADDR",
		"Queue tests need a real Redis, e.g. localhost:6379.")
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("pinging redis at %s: %v", addr, err)
	}
	return client
}

// clearAll wipes every queue key. Claims scan the whole tenant rotation, and
// the reclaimer scans every processing list, so anything another test left
// behind would otherwise leak into this test's results.
func clearAll(t *testing.T, client *redis.Client) {
	t.Helper()
	ctx := context.Background()
	keys, err := client.Keys(ctx, "notifications:*").Result()
	if err != nil {
		t.Fatalf("listing keys: %v", err)
	}
	if len(keys) > 0 {
		if err := client.Del(ctx, keys...).Err(); err != nil {
			t.Fatalf("clearing keys: %v", err)
		}
	}
}

// enqueue puts items on the queue, failing the test on error.
func enqueue(t *testing.T, q *Queue, items ...Item) {
	t.Helper()
	for _, it := range items {
		if err := q.Enqueue(context.Background(), it); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
}

// itemsFor returns n fresh items for one tenant.
func itemsFor(tenant uuid.UUID, n int) []Item {
	out := make([]Item, n)
	for i := range out {
		out[i] = Item{Tenant: tenant, ID: uuid.New()}
	}
	return out
}

// claim claims one item, failing the test on error.
func claim(t *testing.T, c *Consumer) Item {
	t.Helper()
	it, err := c.Claim(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return it
}

func TestParseItem(t *testing.T) {
	tenant, id := uuid.New(), uuid.New()
	tests := []struct {
		name    string
		in      string
		want    Item
		wantErr bool
	}{
		{name: "tenant and id", in: tenant.String() + ":" + id.String(), want: Item{Tenant: tenant, ID: id}},
		{name: "legacy bare id parses with the nil tenant", in: id.String(), want: Item{ID: id}},
		{name: "bad tenant", in: "nope:" + id.String(), wantErr: true},
		{name: "bad id", in: tenant.String() + ":nope", wantErr: true},
		{name: "junk", in: "not-a-uuid", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseItem(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseItem(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.Tenant != tt.want.Tenant || got.ID != tt.want.ID {
				t.Errorf("ParseItem(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
			// Round-trips to the exact string, so Ack removes what was claimed.
			if got.String() != tt.in {
				t.Errorf("String() = %q, want %q", got.String(), tt.in)
			}
		})
	}
}

// TestEnqueueFilesUnderTenant: an item lands on its own tenant's list, and the
// tenant joins the rotation exactly once however many items it has.
func TestEnqueueFilesUnderTenant(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	tenant := uuid.New()
	items := itemsFor(tenant, 2)
	enqueue(t, New(client), items...)

	got, err := client.LRange(ctx, tenantQueuePrefix+tenant.String(), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("tenant list = %v, want 2 entries", got)
	}
	ring, err := client.LRange(ctx, ringKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange ring: %v", err)
	}
	if len(ring) != 1 || ring[0] != tenant.String() {
		t.Errorf("ring = %v, want exactly [%s]", ring, tenant)
	}
}

// TestClaimMovesToProcessing is the core reliability property: a claim must not
// destroy the work item, only relocate it to a list this worker owns.
func TestClaimMovesToProcessing(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	it := Item{Tenant: uuid.New(), ID: uuid.New()}
	enqueue(t, New(client), it)
	c := NewConsumer(client, "test-worker")

	got := claim(t, c)
	if got.ID != it.ID || got.Tenant != it.Tenant {
		t.Errorf("claimed %+v, want %+v", got, it)
	}

	// The entry must now be on the processing list — this is what survives a crash.
	inflight, err := client.LRange(ctx, c.processingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(inflight) != 1 || inflight[0] != it.String() {
		t.Errorf("processing list = %v, want exactly [%s]", inflight, it)
	}

	if err := c.Ack(ctx, got); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, _ := client.LLen(ctx, c.processingKey).Result(); n != 0 {
		t.Errorf("processing list length after Ack = %d, want 0", n)
	}
}

// TestClaimIsRoundRobinAcrossTenants: consecutive claims visit tenants in turn,
// and each tenant's own items stay FIFO.
func TestClaimIsRoundRobinAcrossTenants(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)

	a, b, c := uuid.New(), uuid.New(), uuid.New()
	q := New(client)
	aItems, bItems, cItems := itemsFor(a, 4), itemsFor(b, 2), itemsFor(c, 1)
	enqueue(t, q, aItems...) // a joins the rotation first
	enqueue(t, q, bItems...)
	enqueue(t, q, cItems...)

	want := []Item{aItems[0], bItems[0], cItems[0], aItems[1], bItems[1], aItems[2], aItems[3]}
	consumer := NewConsumer(client, "test-worker-rr")
	for i, w := range want {
		if got := claim(t, consumer); got.ID != w.ID {
			t.Errorf("claim #%d = %s (tenant %s), want %s (tenant %s)", i+1, got.ID, got.Tenant, w.ID, w.Tenant)
		}
	}
}

// TestNoisyTenantCannotStarveQuietOne is the property the design exists for:
// a quiet tenant's single notification is served within one rotation, however
// large the backlog another tenant queued ahead of it.
func TestNoisyTenantCannotStarveQuietOne(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)

	noisy, quiet := uuid.New(), uuid.New()
	q := New(client)
	enqueue(t, q, itemsFor(noisy, 1000)...)
	quietItem := Item{Tenant: quiet, ID: uuid.New()}
	enqueue(t, q, quietItem)

	consumer := NewConsumer(client, "test-worker-fair")
	for i := 1; i <= 2; i++ {
		if claim(t, consumer).ID == quietItem.ID {
			return
		}
	}
	t.Error("quiet tenant's notification was not among the first 2 claims behind a 1,000-item backlog")
}

// TestEmptyTenantLeavesRotation: once a tenant's list is empty it is dropped
// from the ring on its next visit, and it rejoins when it enqueues again.
func TestEmptyTenantLeavesRotation(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	tenant := uuid.New()
	q := New(client)
	enqueue(t, q, itemsFor(tenant, 1)...)
	consumer := NewConsumer(client, "test-worker-empty")
	claim(t, consumer)

	// Nothing left: the next claim reports ErrNoWork and prunes the tenant.
	if _, err := consumer.Claim(ctx, time.Millisecond); !errors.Is(err, ErrNoWork) {
		t.Fatalf("Claim on an emptied tenant = %v, want ErrNoWork", err)
	}
	if n, _ := client.LLen(ctx, ringKey).Result(); n != 0 {
		t.Errorf("ring holds %d tenants, want 0 after the empty tenant was visited", n)
	}
	if n, _ := client.SCard(ctx, activeKey).Result(); n != 0 {
		t.Errorf("active set holds %d tenants, want 0", n)
	}

	// A new enqueue brings it back.
	again := Item{Tenant: tenant, ID: uuid.New()}
	enqueue(t, q, again)
	if got := claim(t, consumer); got.ID != again.ID {
		t.Errorf("claimed %s after re-enqueue, want %s", got.ID, again.ID)
	}
}

// TestClaimReturnsErrNoWorkWhenEmpty verifies an idle timeout is reported as a
// normal tick rather than an error the loop would back off on.
func TestClaimReturnsErrNoWorkWhenEmpty(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)

	_, err := NewConsumer(client, "test-worker-idle").Claim(context.Background(), 100*time.Millisecond)
	if !errors.Is(err, ErrNoWork) {
		t.Errorf("Claim on empty queue = %v, want ErrNoWork", err)
	}
}

// TestIdleClaimWakesOnEnqueue: a worker blocked waiting for work is woken by
// the doorbell as soon as something is enqueued, not at the end of its timeout.
func TestIdleClaimWakesOnEnqueue(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)

	it := Item{Tenant: uuid.New(), ID: uuid.New()}
	type result struct {
		it      Item
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		got, err := NewConsumer(client, "test-worker-wake").Claim(context.Background(), 5*time.Second)
		done <- result{got, err, time.Since(start)}
	}()

	time.Sleep(200 * time.Millisecond)
	enqueue(t, New(client), it)

	r := <-done
	if r.err != nil {
		t.Fatalf("Claim: %v", r.err)
	}
	if r.it.ID != it.ID {
		t.Errorf("claimed %s, want %s", r.it.ID, it.ID)
	}
	if r.elapsed > 2*time.Second {
		t.Errorf("claim returned after %v, want well before its 5s timeout", r.elapsed)
	}
}

// TestAckRemovesOnlyOneEntry guards the LREM count of 1: a duplicate claim of
// the same entry must not have both copies erased by a single Ack.
func TestAckRemovesOnlyOneEntry(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	c := NewConsumer(client, "test-worker-dupe")
	it := Item{Tenant: uuid.New(), ID: uuid.New()}
	if err := client.LPush(ctx, c.processingKey, it.String(), it.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}

	if err := c.Ack(ctx, it); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, _ := client.LLen(ctx, c.processingKey).Result(); n != 1 {
		t.Errorf("processing list length after one Ack = %d, want 1", n)
	}
}

// TestClaimDiscardsMalformedEntry ensures a junk value cannot wedge the worker
// by being re-claimed on every pass.
func TestClaimDiscardsMalformedEntry(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	c := NewConsumer(client, "test-worker-junk")
	if err := client.LPush(ctx, legacyQueueKey, "not-a-uuid").Err(); err != nil {
		t.Fatalf("seeding queue: %v", err)
	}

	if _, err := c.Claim(ctx, time.Second); err == nil {
		t.Fatal("Claim on malformed entry = nil error, want a parse error")
	}
	if n, _ := client.LLen(ctx, c.processingKey).Result(); n != 0 {
		t.Errorf("processing list length = %d, want 0 (malformed entry should be discarded)", n)
	}
}

// TestLegacyQueueIsStillDrained: bare ids left on the old single queue by a
// previous deployment are claimed and acked, not stranded.
func TestLegacyQueueIsStillDrained(t *testing.T) {
	client := requireRedis(t)
	clearAll(t, client)
	ctx := context.Background()

	id := uuid.New()
	if err := client.LPush(ctx, legacyQueueKey, id.String()).Err(); err != nil {
		t.Fatalf("seeding legacy queue: %v", err)
	}

	c := NewConsumer(client, "test-worker-legacy")
	got := claim(t, c)
	if got.ID != id || got.Tenant != uuid.Nil {
		t.Errorf("claimed %+v, want id %s with the nil tenant", got, id)
	}
	if err := c.Ack(ctx, got); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if n, _ := client.LLen(ctx, c.processingKey).Result(); n != 0 {
		t.Errorf("processing list length after Ack = %d, want 0 (legacy entry acked by its exact string)", n)
	}
}
