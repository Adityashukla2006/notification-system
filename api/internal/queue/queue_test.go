package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// requireRedis returns a client for the test Redis, or skips when none is
// configured via TEST_REDIS_ADDR.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run queue tests against a real Redis")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("pinging redis at %s: %v", addr, err)
	}
	return client
}

func TestEnqueue(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	q := New(client)
	// Isolate this test's data.
	if err := client.Del(ctx, q.key).Err(); err != nil {
		t.Fatalf("clearing queue: %v", err)
	}

	id := uuid.New()
	if err := q.Enqueue(ctx, id); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The worker claims from the right-hand end; verify the id is there.
	got, err := client.RPop(ctx, q.key).Result()
	if err != nil {
		t.Fatalf("RPop: %v", err)
	}
	if got != id.String() {
		t.Errorf("popped %q, want %q", got, id.String())
	}
}

// TestClaimMovesToProcessing is the core reliability property: a claim must not
// destroy the work item, only relocate it to a list this worker owns.
func TestClaimMovesToProcessing(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	q := New(client)
	c := NewConsumer(client, "test-worker")
	if err := client.Del(ctx, q.key, c.processingKey).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}

	id := uuid.New()
	if err := q.Enqueue(ctx, id); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := c.Claim(ctx, time.Second)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != id {
		t.Errorf("claimed %s, want %s", got, id)
	}

	// The id must now be on the processing list — this is what survives a crash.
	inflight, err := client.LRange(ctx, c.processingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange: %v", err)
	}
	if len(inflight) != 1 || inflight[0] != id.String() {
		t.Errorf("processing list = %v, want exactly [%s]", inflight, id)
	}

	if err := c.Ack(ctx, id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	n, err := client.LLen(ctx, c.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if n != 0 {
		t.Errorf("processing list length after Ack = %d, want 0", n)
	}
}

// TestClaimReturnsErrNoWorkWhenEmpty verifies an idle timeout is reported as a
// normal tick rather than an error the loop would back off on.
func TestClaimReturnsErrNoWorkWhenEmpty(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	c := NewConsumer(client, "test-worker-idle")
	if err := client.Del(ctx, defaultQueueKey, c.processingKey).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}

	_, err := c.Claim(ctx, 100*time.Millisecond)
	if !errors.Is(err, ErrNoWork) {
		t.Errorf("Claim on empty queue = %v, want ErrNoWork", err)
	}
}

// TestAckRemovesOnlyOneEntry guards the LREM count of 1: a duplicate claim of
// the same id must not have both entries erased by a single Ack.
func TestAckRemovesOnlyOneEntry(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	c := NewConsumer(client, "test-worker-dupe")
	if err := client.Del(ctx, c.processingKey).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}

	id := uuid.New()
	if err := client.LPush(ctx, c.processingKey, id.String(), id.String()).Err(); err != nil {
		t.Fatalf("seeding processing list: %v", err)
	}

	if err := c.Ack(ctx, id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	n, err := client.LLen(ctx, c.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if n != 1 {
		t.Errorf("processing list length after one Ack = %d, want 1", n)
	}
}

// TestClaimDiscardsMalformedEntry ensures a junk value cannot wedge the worker
// by being re-claimed on every pass.
func TestClaimDiscardsMalformedEntry(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	c := NewConsumer(client, "test-worker-junk")
	if err := client.Del(ctx, defaultQueueKey, c.processingKey).Err(); err != nil {
		t.Fatalf("clearing keys: %v", err)
	}
	if err := client.LPush(ctx, defaultQueueKey, "not-a-uuid").Err(); err != nil {
		t.Fatalf("seeding queue: %v", err)
	}

	if _, err := c.Claim(ctx, time.Second); err == nil {
		t.Fatal("Claim on malformed entry = nil error, want a parse error")
	}
	n, err := client.LLen(ctx, c.processingKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if n != 0 {
		t.Errorf("processing list length = %d, want 0 (malformed entry should be discarded)", n)
	}
}

func TestEnqueueFIFOOrder(t *testing.T) {
	client := requireRedis(t)
	ctx := context.Background()

	q := New(client)
	if err := client.Del(ctx, q.key).Err(); err != nil {
		t.Fatalf("clearing queue: %v", err)
	}

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		if err := q.Enqueue(ctx, id); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// LPUSH + RPOP yields FIFO: first enqueued is first popped.
	for _, want := range ids {
		got, err := client.RPop(ctx, q.key).Result()
		if err != nil {
			t.Fatalf("RPop: %v", err)
		}
		if got != want.String() {
			t.Errorf("popped %q, want %q", got, want.String())
		}
	}
}
