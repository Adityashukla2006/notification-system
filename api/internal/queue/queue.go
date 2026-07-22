// Package queue is the Redis-backed hand-off between the API and the worker.
// It is ephemeral coordination only: the queue holds notification IDs, never
// payloads, because Postgres is the source of truth and the worker loads the
// full row by id. Keeping only IDs here means Redis and Postgres cannot drift,
// and the queue is trivially rebuildable from Postgres if Redis is wiped.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// defaultQueueKey is the Redis list that pending notification IDs are pushed to.
const defaultQueueKey = "notifications:queue"

// processingKeyPrefix namespaces the per-worker in-flight lists. Each worker
// owns exactly one, keyed by its identity, so a crashed worker's unfinished
// claims remain visible under a key that names the worker responsible.
const processingKeyPrefix = "notifications:processing:"

// ErrNoWork signals that a Claim waited its full timeout without work arriving.
// It is an ordinary idle tick, not a failure: the caller loops and claims again.
var ErrNoWork = errors.New("no work available")

// Queue enqueues notification IDs for delivery.
type Queue struct {
	client *redis.Client
	key    string
}

// New constructs a Queue over the given Redis client.
func New(client *redis.Client) *Queue {
	return &Queue{client: client, key: defaultQueueKey}
}

// Enqueue pushes a notification id onto the work list. The worker claims from
// the other end (FIFO), so LPUSH here pairs with a right-hand BLMOVE there.
func (q *Queue) Enqueue(ctx context.Context, id uuid.UUID) error {
	if err := q.client.LPush(ctx, q.key, id.String()).Err(); err != nil {
		return fmt.Errorf("enqueueing notification %s: %w", id, err)
	}
	return nil
}

// Consumer is the worker's end of the queue. It implements a reliable queue: a
// claim does not destroy the work item, it MOVES it to a list owned by this
// worker, and only an explicit Ack removes it.
//
// This is what makes delivery at-least-once. A plain BRPOP deletes the id the
// instant Redis hands it over, so a worker that dies mid-delivery takes the
// only record of that claim with it. With a processing list, the id outlives
// the worker and can be reclaimed.
type Consumer struct {
	client        *redis.Client
	queueKey      string
	processingKey string
}

// NewConsumer constructs a Consumer that claims work into a processing list
// owned by workerID. The id must be stable for a given worker across restarts,
// otherwise a restarted worker abandons its own in-flight claims under a key
// nobody will look at again.
func NewConsumer(client *redis.Client, workerID string) *Consumer {
	return &Consumer{
		client:        client,
		queueKey:      defaultQueueKey,
		processingKey: processingKeyPrefix + workerID,
	}
}

// Claim blocks for up to timeout waiting for a notification id, atomically
// moving it from the shared queue onto this worker's processing list. It
// returns ErrNoWork if the timeout elapses with the queue empty.
//
// The move is atomic on the Redis side, so there is no window in which the id
// exists on neither list — the failure mode is a duplicate claim, never a lost
// one, which is the correct trade for at-least-once delivery.
func (c *Consumer) Claim(ctx context.Context, timeout time.Duration) (uuid.UUID, error) {
	// RIGHT out of the queue pairs with Enqueue's LPUSH to give FIFO; LEFT into
	// processing keeps the newest claim at the head for inspection.
	raw, err := c.client.BLMove(ctx, c.queueKey, c.processingKey, "RIGHT", "LEFT", timeout).Result()
	if errors.Is(err, redis.Nil) {
		return uuid.Nil, ErrNoWork
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("claiming notification: %w", err)
	}

	id, err := uuid.Parse(raw)
	if err != nil {
		// The value is unusable, so drop it rather than let it block the worker
		// on every future claim.
		if remErr := c.remove(ctx, raw); remErr != nil {
			return uuid.Nil, fmt.Errorf("discarding malformed queue entry %q: %w", raw, remErr)
		}
		return uuid.Nil, fmt.Errorf("parsing claimed notification id %q: %w", raw, err)
	}
	return id, nil
}

// Ack removes a completed notification from this worker's processing list.
// Callers must Ack only after the outcome is durable in Postgres: an id that is
// acked before its terminal status is written is an id nothing will ever retry.
func (c *Consumer) Ack(ctx context.Context, id uuid.UUID) error {
	if err := c.remove(ctx, id.String()); err != nil {
		return fmt.Errorf("acking notification %s: %w", id, err)
	}
	return nil
}

// remove deletes a single occurrence of value from the processing list. The
// count of 1 matters: a duplicate claim of the same id must not have both
// entries erased by one Ack.
func (c *Consumer) remove(ctx context.Context, value string) error {
	return c.client.LRem(ctx, c.processingKey, 1, value).Err()
}
