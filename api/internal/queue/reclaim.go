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

// requeueScript moves the OLDEST entry of a processing list back to the FRONT
// of its own tenant's queue (the end claims read from), re-activates that
// tenant in the rotation, and rings the doorbell — all atomically, so an entry
// is never on neither list. It returns the entry, or false when the list is
// empty.
//
// KEYS: 1 processing list, 2 ring, 3 active set, 4 doorbell
// ARGV: 1 tenant list prefix, 2 nil tenant (for legacy bare-id entries),
// 3 doorbell cap
var requeueScript = redis.NewScript(`
local entry = redis.call('RPOP', KEYS[1])
if not entry then return false end
local sep = string.find(entry, ':', 1, true)
local tenant = ARGV[2]
if sep then tenant = string.sub(entry, 1, sep - 1) end
redis.call('RPUSH', ARGV[1] .. tenant, entry)
if redis.call('SADD', KEYS[3], tenant) == 1 then
  redis.call('RPUSH', KEYS[2], tenant)
end
redis.call('RPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], -tonumber(ARGV[3]), -1)
return entry
`)

// requeueAll empties a processing list back onto the tenant queues, oldest
// claim first, and reports how many entries moved.
func requeueAll(ctx context.Context, client *redis.Client, processingKey string) (int, error) {
	moved := 0
	for {
		err := requeueScript.Run(ctx, client,
			[]string{processingKey, ringKey, activeKey, doorbellKey},
			tenantQueuePrefix, nilTenant, doorbellCap).Err()
		if errors.Is(err, redis.Nil) {
			return moved, nil
		}
		if err != nil {
			return moved, fmt.Errorf("requeueing from %s: %w", processingKey, err)
		}
		moved++
	}
}

// nilTenant is the tenant legacy bare-id entries are filed under.
var nilTenant = tenantOf("")

// Reclaimer returns abandoned claims to their tenants' queues.
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
	processingPrefix string
	livenessPrefix   string
}

// NewReclaimer constructs a Reclaimer over the given Redis client.
func NewReclaimer(client *redis.Client) *Reclaimer {
	return &Reclaimer{
		client:           client,
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
// alive and moves their claims back onto their tenants' queues. It returns the
// number of notifications reclaimed and the number of dead workers drained.
//
// Safe to run concurrently on every worker: each entry is moved by an atomic
// script, so two reclaimers racing on the same list cannot both move the same
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

			moved, err := requeueAll(ctx, r.client, key)
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

// Drain returns this consumer's own outstanding claims to their tenants'
// queues, reporting how many moved.
//
// A worker calls this at startup, before claiming anything new. Whatever is on
// its own processing list at that moment is by definition left over from a
// previous life, and the worker knows it is not currently delivering any of it
// — so it can recover its own work immediately, without waiting for a liveness
// key to expire.
func (c *Consumer) Drain(ctx context.Context) (int, error) {
	moved, err := requeueAll(ctx, c.client, c.processingKey)
	if err != nil {
		return moved, fmt.Errorf("draining own processing list: %w", err)
	}
	return moved, nil
}
