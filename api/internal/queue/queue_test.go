package queue

import (
	"context"
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

	// The worker pops with BRPOP; verify the id is there and matches.
	got, err := client.RPop(ctx, q.key).Result()
	if err != nil {
		t.Fatalf("RPop: %v", err)
	}
	if got != id.String() {
		t.Errorf("popped %q, want %q", got, id.String())
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
