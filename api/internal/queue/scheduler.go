package queue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// defaultScheduledKey is the Redis sorted set holding notifications that are not
// yet eligible for delivery, scored by the unix-millisecond timestamp at which
// they become due.
const defaultScheduledKey = "notifications:scheduled"

// Scheduler defers notifications until a future time. It serves two callers
// that want the same thing: the API, when a client asks for a scheduled send,
// and the worker, when a failed delivery earns a backoff. Both are "do not
// deliver this before time T", so both use one mechanism.
//
// A sorted set is the right structure because the due check is a range query on
// the score — Redis keeps the set ordered, so finding everything due costs the
// size of the answer, not the size of the set.
type Scheduler struct {
	client       *redis.Client
	scheduledKey string
	queueKey     string
}

// NewScheduler constructs a Scheduler over the given Redis client.
func NewScheduler(client *redis.Client) *Scheduler {
	return &Scheduler{
		client:       client,
		scheduledKey: defaultScheduledKey,
		queueKey:     defaultQueueKey,
	}
}

// Schedule makes a notification eligible for delivery no earlier than at.
// Re-scheduling the same id overwrites its due time rather than adding a second
// entry, because a sorted set member is unique — which makes this naturally
// idempotent under retries.
func (s *Scheduler) Schedule(ctx context.Context, id uuid.UUID, at time.Time) error {
	z := redis.Z{Score: float64(at.UnixMilli()), Member: id.String()}
	if err := s.client.ZAdd(ctx, s.scheduledKey, z).Err(); err != nil {
		return fmt.Errorf("scheduling notification %s: %w", id, err)
	}
	return nil
}

// PromoteDue moves up to limit notifications whose due time has passed onto the
// ready queue, returning how many it promoted.
//
// It is safe to run on every worker concurrently. The race is resolved by ZRem's
// return value: for any given id exactly one caller removes it and gets 1, and
// every other caller gets 0 and skips it. That makes a promoter loop per worker
// correct without leader election or a distributed lock.
func (s *Scheduler) PromoteDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	due, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     s.scheduledKey,
		ByScore: true,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.UnixMilli(), 10),
		Count:   limit,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("querying due notifications: %w", err)
	}

	promoted := 0
	for _, raw := range due {
		// Claim this id. Only the caller that removes it may promote it.
		removed, err := s.client.ZRem(ctx, s.scheduledKey, raw).Result()
		if err != nil {
			return promoted, fmt.Errorf("claiming due notification %q: %w", raw, err)
		}
		if removed == 0 {
			// Another worker promoted it first.
			continue
		}

		if err := s.client.LPush(ctx, s.queueKey, raw).Err(); err != nil {
			// The id is now on neither structure. Put it back so it is not
			// lost outright; if this also fails, the Postgres row still holds
			// the authoritative scheduled_at and can be recovered from there.
			z := redis.Z{Score: float64(now.UnixMilli()), Member: raw}
			if readdErr := s.client.ZAdd(ctx, s.scheduledKey, z).Err(); readdErr != nil {
				return promoted, fmt.Errorf("promoting %q failed (%v) and re-scheduling failed: %w", raw, err, readdErr)
			}
			return promoted, fmt.Errorf("promoting due notification %q: %w", raw, err)
		}
		promoted++
	}
	return promoted, nil
}

// Cancel removes a notification from the schedule. It reports whether an entry
// was actually removed.
func (s *Scheduler) Cancel(ctx context.Context, id uuid.UUID) (bool, error) {
	removed, err := s.client.ZRem(ctx, s.scheduledKey, id.String()).Result()
	if err != nil {
		return false, fmt.Errorf("cancelling scheduled notification %s: %w", id, err)
	}
	return removed > 0, nil
}
