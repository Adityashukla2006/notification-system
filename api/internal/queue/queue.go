// Package queue is the Redis-backed hand-off between the API and the worker.
// It is ephemeral coordination only: the queue holds notification IDs, never
// payloads, because Postgres is the source of truth and the worker loads the
// full row by id. Keeping only IDs here means Redis and Postgres cannot drift,
// and the queue is trivially rebuildable from Postgres if Redis is wiped.
//
// Work is queued PER TENANT and claimed round-robin across tenants, so one
// tenant's backlog cannot delay another tenant's notifications. The design,
// and how crash recovery works on it, is in bench/DESIGN-fair-queue.md.
package queue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Redis keys. See bench/DESIGN-fair-queue.md for what each one holds.
const (
	// tenantQueuePrefix + tenant id is one tenant's ready list.
	tenantQueuePrefix = "notifications:q:"
	// ringKey is the rotation order of tenants that may have work.
	ringKey = "notifications:tenants:ring"
	// activeKey is the set of tenants in the ring, so none appears twice.
	activeKey = "notifications:tenants:active"
	// doorbellKey carries wake-up tokens for workers blocked waiting for work.
	doorbellKey = "notifications:doorbell"
	// legacyQueueKey is the single shared list used before per-tenant queues.
	// It is drained by claims but never written, so entries left there by an
	// older deployment are still delivered.
	legacyQueueKey = "notifications:queue"
)

// processingKeyPrefix namespaces the per-worker in-flight lists. Each worker
// owns exactly one, keyed by its identity, so a crashed worker's unfinished
// claims remain visible under a key that names the worker responsible.
const processingKeyPrefix = "notifications:processing:"

// doorbellCap bounds how many unconsumed wake-up tokens accumulate while every
// worker is busy. A stale token costs one empty claim, nothing more.
const doorbellCap = 1024

// ErrNoWork signals that a Claim waited its full timeout without work arriving.
// It is an ordinary idle tick, not a failure: the caller loops and claims again.
var ErrNoWork = errors.New("no work available")

// Item is one queued notification and the tenant it belongs to. The tenant
// travels with the id through every list so that any recovery path can return
// a claim to the right tenant's queue without asking Postgres.
type Item struct {
	Tenant uuid.UUID
	ID     uuid.UUID
	// raw is the exact string read from Redis, kept so Ack removes precisely
	// the entry that was claimed (including legacy bare-id entries).
	raw string
}

// String is the Redis encoding of an item: "<tenant>:<id>".
func (it Item) String() string {
	if it.raw != "" {
		return it.raw
	}
	return it.Tenant.String() + ":" + it.ID.String()
}

// ParseItem decodes an entry. A bare id, written before per-tenant queues
// existed, parses with the nil tenant.
func ParseItem(s string) (Item, error) {
	tenantPart, idPart, found := strings.Cut(s, ":")
	if !found {
		id, err := uuid.Parse(s)
		if err != nil {
			return Item{}, fmt.Errorf("parsing queue entry %q: %w", s, err)
		}
		return Item{ID: id, raw: s}, nil
	}
	tenant, err := uuid.Parse(tenantPart)
	if err != nil {
		return Item{}, fmt.Errorf("parsing tenant in queue entry %q: %w", s, err)
	}
	id, err := uuid.Parse(idPart)
	if err != nil {
		return Item{}, fmt.Errorf("parsing id in queue entry %q: %w", s, err)
	}
	return Item{Tenant: tenant, ID: id, raw: s}, nil
}

// tenantOf returns the tenant part of an entry as Lua scripts see it. Kept in
// Go only for tests and documentation; the scripts do the same split.
func tenantOf(entry string) string {
	if t, _, found := strings.Cut(entry, ":"); found {
		return t
	}
	return uuid.Nil.String()
}

// pushScript puts one entry on its tenant's list and makes sure the tenant is
// in the rotation, atomically, then rings the doorbell.
//
// KEYS: 1 tenant list, 2 ring, 3 active set, 4 doorbell
// ARGV: 1 entry, 2 tenant, 3 "back" (normal enqueue) or "front" (recovered
// work, claimed next), 4 doorbell cap
var pushScript = redis.NewScript(`
if ARGV[3] == 'front' then
  redis.call('RPUSH', KEYS[1], ARGV[1])
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
end
if redis.call('SADD', KEYS[3], ARGV[2]) == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[2])
end
redis.call('RPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], -tonumber(ARGV[4]), -1)
return 1
`)

// push runs pushScript for one entry.
func push(ctx context.Context, client *redis.Client, entry, where string) error {
	tenant := tenantOf(entry)
	keys := []string{tenantQueuePrefix + tenant, ringKey, activeKey, doorbellKey}
	return pushScript.Run(ctx, client, keys, entry, tenant, where, doorbellCap).Err()
}

// Queue enqueues notifications for delivery.
type Queue struct {
	client *redis.Client
}

// New constructs a Queue over the given Redis client.
func New(client *redis.Client) *Queue {
	return &Queue{client: client}
}

// Enqueue puts a notification at the back of its tenant's queue and makes
// sure the tenant is in the claim rotation.
func (q *Queue) Enqueue(ctx context.Context, it Item) error {
	if err := push(ctx, q.client, it.String(), "back"); err != nil {
		return fmt.Errorf("enqueueing notification %s: %w", it.ID, err)
	}
	return nil
}

// claimScript takes the next entry round-robin across tenants and moves it
// onto the caller's processing list, atomically. Each tenant visited is
// rotated to the back of the ring; a tenant whose list is empty is dropped
// from the rotation. It falls back to the legacy single queue.
//
// KEYS: 1 ring, 2 active set, 3 processing list, 4 legacy queue
// ARGV: 1 tenant list prefix
var claimScript = redis.NewScript(`
local n = redis.call('LLEN', KEYS[1])
for i = 1, n do
  local tenant = redis.call('LMOVE', KEYS[1], KEYS[1], 'LEFT', 'RIGHT')
  if not tenant then break end
  local entry = redis.call('LMOVE', ARGV[1] .. tenant, KEYS[3], 'RIGHT', 'LEFT')
  if entry then return entry end
  redis.call('LREM', KEYS[1], -1, tenant)
  redis.call('SREM', KEYS[2], tenant)
end
return redis.call('LMOVE', KEYS[4], KEYS[3], 'RIGHT', 'LEFT')
`)

// Consumer is the worker's end of the queue. It implements a reliable queue: a
// claim does not destroy the work item, it MOVES it to a list owned by this
// worker, and only an explicit Ack removes it.
//
// This is what makes delivery at-least-once. A plain pop deletes the id the
// instant Redis hands it over, so a worker that dies mid-delivery takes the
// only record of that claim with it. With a processing list, the id outlives
// the worker and can be reclaimed.
type Consumer struct {
	client        *redis.Client
	processingKey string
}

// NewConsumer constructs a Consumer that claims work into a processing list
// owned by workerID. The id must be stable for a given worker across restarts,
// otherwise a restarted worker abandons its own in-flight claims under a key
// nobody will look at again.
func NewConsumer(client *redis.Client, workerID string) *Consumer {
	return &Consumer{
		client:        client,
		processingKey: processingKeyPrefix + workerID,
	}
}

// Claim waits up to timeout for a notification, taking tenants in turn, and
// atomically moves it onto this worker's processing list. It returns ErrNoWork
// if the timeout elapses with nothing queued.
//
// The move is atomic on the Redis side, so there is no window in which the
// entry exists on neither list — the failure mode is a duplicate claim, never
// a lost one, which is the correct trade for at-least-once delivery.
func (c *Consumer) Claim(ctx context.Context, timeout time.Duration) (Item, error) {
	deadline := time.Now().Add(timeout)
	for {
		raw, err := claimScript.Run(ctx, c.client,
			[]string{ringKey, activeKey, c.processingKey, legacyQueueKey},
			tenantQueuePrefix).Text()
		if err == nil {
			it, perr := ParseItem(raw)
			if perr != nil {
				// The value is unusable, so drop it rather than let it block
				// the worker on every future claim.
				if remErr := c.remove(ctx, raw); remErr != nil {
					return Item{}, fmt.Errorf("discarding malformed queue entry %q: %w", raw, remErr)
				}
				return Item{}, perr
			}
			return it, nil
		}
		if !errors.Is(err, redis.Nil) {
			return Item{}, fmt.Errorf("claiming notification: %w", err)
		}

		// Nothing queued. Sleep on the doorbell for the rest of the timeout;
		// any enqueue rings it, and the loop claims again.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return Item{}, ErrNoWork
		}
		// BLPOP takes whole seconds (go-redis rounds anything smaller up to 1 s
		// and logs a warning), so round up here to keep the logs quiet.
		wait := time.Duration(math.Ceil(remaining.Seconds())) * time.Second
		err = c.client.BLPop(ctx, wait, doorbellKey).Err()
		if errors.Is(err, redis.Nil) {
			return Item{}, ErrNoWork
		}
		if err != nil {
			return Item{}, fmt.Errorf("waiting for work: %w", err)
		}
	}
}

// Ack removes a completed notification from this worker's processing list.
// Callers must Ack only after the outcome is durable in Postgres: an entry
// that is acked before its terminal status is written is one nothing will ever
// retry.
func (c *Consumer) Ack(ctx context.Context, it Item) error {
	if err := c.remove(ctx, it.String()); err != nil {
		return fmt.Errorf("acking notification %s: %w", it.ID, err)
	}
	return nil
}

// remove deletes a single occurrence of value from the processing list. The
// count of 1 matters: a duplicate claim of the same id must not have both
// entries erased by one Ack.
func (c *Consumer) remove(ctx context.Context, value string) error {
	return c.client.LRem(ctx, c.processingKey, 1, value).Err()
}
