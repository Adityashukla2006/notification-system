// Package queue is the Redis-backed hand-off between the API and the worker.
// It is ephemeral coordination only: the queue holds notification IDs, never
// payloads, because Postgres is the source of truth and the worker loads the
// full row by id. Keeping only IDs here means Redis and Postgres cannot drift,
// and the queue is trivially rebuildable from Postgres if Redis is wiped.
package queue

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// defaultQueueKey is the Redis list that pending notification IDs are pushed to.
const defaultQueueKey = "notifications:queue"

// Queue enqueues notification IDs for delivery.
type Queue struct {
	client *redis.Client
	key    string
}

// New constructs a Queue over the given Redis client.
func New(client *redis.Client) *Queue {
	return &Queue{client: client, key: defaultQueueKey}
}

// Enqueue pushes a notification id onto the work list. The worker pops from the
// other end (FIFO), so LPUSH here pairs with a BRPOP there.
func (q *Queue) Enqueue(ctx context.Context, id uuid.UUID) error {
	if err := q.client.LPush(ctx, q.key, id.String()).Err(); err != nil {
		return fmt.Errorf("enqueueing notification %s: %w", id, err)
	}
	return nil
}
