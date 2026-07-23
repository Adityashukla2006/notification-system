package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// livenessKeyPrefix namespaces the per-worker liveness keys. A key exists only
// while its worker is refreshing it, so its absence is the signal that a
// worker's in-flight claims have been abandoned.
const livenessKeyPrefix = "notifications:worker:"

// Reclaimer returns abandoned claims to the ready queue.
//
// It exists because a reliable claim is only half a guarantee. Moving an id to
// a processing list means a crashed worker cannot LOSE it — but on its own,
// nothing ever picks it up again, so the notification is preserved and stuck.
// The reclaimer is the other half.
//
// The hard part is distinguishing a dead worker from a busy one: a processing
// list looks identical either way. Redis cannot answer that, so each worker
// proves it is alive by refreshing a key with a TTL. If the key has expired,
// the worker stopped refreshing it, and its claims are fair game.
type Reclaimer struct {
	client           *redis.Client
	queueKey         string
	processingPrefix string
	livenessPrefix   string
}

// NewReclaimer constructs a Reclaimer over the given Redis client.
func NewReclaimer(client *redis.Client) *Reclaimer {
	return &Reclaimer{
		client:           client,
		queueKey:         defaultQueueKey,
		processingPrefix: processingKeyPrefix,
		livenessPrefix:   livenessKeyPrefix,
	}
}

// Heartbeat refreshes this worker's liveness key.
//
// ttl must comfortably exceed the heartbeat interval. If it does not, a worker
// that is merely slow — a long GC pause, a stalled provider call — looks dead,
// and its in-flight notification is re-delivered while the original attempt is
// still running. At-least-once makes that safe, not free.
func (r *Reclaimer) Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error {
	key := r.livenessPrefix + workerID
	if err := r.client.Set(ctx, key, time.Now().UnixMilli(), ttl).Err(); err != nil {
		return fmt.Errorf("refreshing liveness for worker %s: %w", workerID, err)
	}
	return nil
}

// ReclaimAbandoned scans for processing lists whose owning worker is no longer
// alive and moves their claims back onto the ready queue. It returns the number
// of notifications reclaimed and the number of dead workers drained.
//
// Safe to run concurrently on every worker: each id is moved with an atomic
// LMOVE, so two reclaimers racing on the same list cannot both move the same
// entry.
func (r *Reclaimer) ReclaimAbandoned(ctx context.Context) (notifications int, workers int, err error) {
	var cursor uint64
	for {
		keys, next, err := r.client.Scan(ctx, cursor, r.processingPrefix+"*", 100).Result()
		if err != nil {
			return notifications, workers, fmt.Errorf("scanning processing lists: %w", err)
		}

		for _, key := range keys {
			workerID := strings.TrimPrefix(key, r.processingPrefix)

			alive, err := r.isAlive(ctx, workerID)
			if err != nil {
				return notifications, workers, err
			}
			if alive {
				continue
			}

			moved, err := r.drain(ctx, key)
			if err != nil {
				return notifications, workers, err
			}
			if moved > 0 {
				notifications += moved
				workers++
			}
		}

		cursor = next
		if cursor == 0 {
			return notifications, workers, nil
		}
	}
}

// isAlive reports whether a worker is still refreshing its liveness key.
func (r *Reclaimer) isAlive(ctx context.Context, workerID string) (bool, error) {
	n, err := r.client.Exists(ctx, r.livenessPrefix+workerID).Result()
	if err != nil {
		return false, fmt.Errorf("checking liveness of worker %s: %w", workerID, err)
	}
	return n > 0, nil
}

// drain moves every entry from a processing list back onto the ready queue,
// oldest claim first, and reports how many moved.
//
// Entries are taken from the RIGHT of the processing list (the oldest claim)
// and pushed to the RIGHT of the ready queue — the end that Claim reads from.
// Reclaimed work has already waited through a worker's death, so it goes to the
// front rather than behind everything enqueued since.
func (r *Reclaimer) drain(ctx context.Context, processingKey string) (int, error) {
	moved := 0
	for {
		_, err := r.client.LMove(ctx, processingKey, r.queueKey, "RIGHT", "RIGHT").Result()
		if errors.Is(err, redis.Nil) {
			// The list is empty; nothing left to reclaim.
			return moved, nil
		}
		if err != nil {
			return moved, fmt.Errorf("reclaiming from %s: %w", processingKey, err)
		}
		moved++
	}
}

// Drain returns this consumer's own outstanding claims to the ready queue,
// reporting how many moved.
//
// A worker calls this at startup, before claiming anything new. Whatever is on
// its own processing list at that moment is by definition left over from a
// previous life, and the worker knows it is not currently delivering any of it
// — so it can recover its own work immediately, without waiting for a liveness
// key to expire.
func (c *Consumer) Drain(ctx context.Context) (int, error) {
	moved := 0
	for {
		_, err := c.client.LMove(ctx, c.processingKey, c.queueKey, "RIGHT", "RIGHT").Result()
		if errors.Is(err, redis.Nil) {
			return moved, nil
		}
		if err != nil {
			return moved, fmt.Errorf("draining own processing list: %w", err)
		}
		moved++
	}
}
