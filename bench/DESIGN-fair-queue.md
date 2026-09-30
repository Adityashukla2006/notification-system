# Design: per-tenant fair queueing and token-bucket rate limiting

Status: implemented after bench-v1. Motivation and evidence:
`results/RESULTS.md` §4 test 7. A quiet tenant's p99 went from 98 ms to 34.6 s
when a noisy tenant flooded the single shared FIFO, and the per-minute limit
only halved it.

## Why

Fairness has to be enforced where work is **scheduled**, not only where it
enters. With one FIFO list, every tenant waits behind every message queued
ahead of it, so one tenant's backlog is everyone's latency. And a per-minute
window cannot stop a burst: a tenant may spend a whole minute's budget in a few
seconds, which is exactly what built the backlog in test 7.

## 1. Per-tenant queues with round-robin claiming

### Redis keys

| Key | Type | Meaning |
|---|---|---|
| `notifications:q:<tenant>` | list | one tenant's ready notifications; push LEFT, claim RIGHT (FIFO) |
| `notifications:tenants:ring` | list | rotation order of tenants that may have work |
| `notifications:tenants:active` | set | membership for the ring, so a tenant appears in it at most once |
| `notifications:doorbell` | list | wake-up tokens for idle workers (capped at 1,024) |
| `notifications:processing:<worker>` | list | unchanged: a worker's in-flight claims |
| `notifications:scheduled` | sorted set | unchanged role: retries and scheduled sends, member = entry |
| `notifications:queue` | list | legacy single queue: only drained, never written |

An **entry** is `"<tenant>:<notification id>"`. Carrying the tenant inside the
entry is what lets every recovery path put a claim back on the right tenant's
list without a database lookup. Entries written before this change are a bare
`"<id>"`; they parse as tenant `00000000-…` and are still claimable.

### Enqueue (one Lua script, atomic)

1. `LPUSH notifications:q:<tenant> entry`
2. If `SADD active <tenant>` returned 1 (newly active), `RPUSH ring <tenant>`.
3. `RPUSH doorbell 1`, `LTRIM doorbell -1024 -1`.

### Claim (one Lua script, atomic, non-blocking)

```
repeat LLEN(ring) times:
    tenant = LMOVE ring ring LEFT RIGHT          -- rotate: head goes to the tail
    entry  = LMOVE q:<tenant> processing RIGHT LEFT
    if entry: return entry                        -- this tenant served; next claim starts at the next tenant
    LREM ring -1 tenant; SREM active tenant       -- queue empty: drop it from the rotation
return LMOVE notifications:queue processing RIGHT LEFT   -- legacy entries, if any
```

Each successful claim moves that tenant to the back of the ring, so consecutive
claims visit tenants in turn: a tenant with 10,000 queued items and a tenant with
1 each get every other slot. A tenant's list and its ring membership are only
changed inside these scripts, and Redis runs a script atomically, so an enqueue
can never slip between "queue looked empty" and "tenant removed" and be
stranded outside the rotation.

**Blocking.** A script cannot block, so a worker that finds nothing waits on
`BLPOP doorbell` for the rest of its claim timeout, and claims again when a
token arrives. Every enqueue rings the bell, so an idle worker wakes as soon as
work arrives rather than on a poll interval. A stale token only costs one empty
claim. If the bell and a claim race, the worst case is a wait of one claim
timeout (1 s in the bench config), never a lost item: the item is on its
tenant's list and in the ring.

### Ack

`LREM processing 1 <entry>` using the exact string that was claimed (the
consumer keeps the raw entry in the claimed item), so legacy bare entries are
acked correctly too.

### Crash recovery — unchanged guarantee

The claim is still a single atomic **move** into the worker's own processing
list, so there is never a moment when the entry is on no list. Recovery paths:

| Path | Before | After |
|---|---|---|
| Worker restarts (`Drain`) | own processing list → shared queue | each entry → **its tenant's** list, front (claimed next), via a script that pops and pushes atomically and re-activates the tenant in the ring |
| Worker dies (`ReclaimAbandoned`) | dead worker's list → shared queue | same script, per entry, for workers whose liveness key expired |
| Retry / scheduled send (`PromoteDue`) | ZSET member → shared queue | member (an entry) → its tenant's list via the enqueue script; on failure it is re-added to the ZSET as before |
| Postgres reaper | id → shared queue | `ReapStuck` now also returns the row's `client_id`, so the id goes to its tenant's list |

Reclaimed entries go to the **front** of their tenant's list (the end claims
read from), preserving the old "reclaimed work is claimed first" behaviour.

### Cost

A claim is one script call instead of one `BLMOVE`. Empty tenants are removed
the first time they are visited, so a claim does work proportional to the number
of tenants that just went empty, amortised O(1). Scripts touch keys named inside
the script (`q:<tenant>`), which is fine on a single Redis node but would need
hash tags on Redis Cluster.

## 2. Token-bucket rate limiting per tenant

Replaces the sliding-window counter. Each tenant has a bucket of up to `BURST`
tokens that refills at `RATE` tokens/second; a request takes one token or is
rejected with 429. The whole check is one Lua script using Redis's own clock
(`TIME`), so API instances with skewed clocks still agree.

- `RATE_LIMIT_RATE` (float, tokens/s) and `RATE_LIMIT_BURST` (int) replace
  `RATE_LIMIT_REQUESTS` / `RATE_LIMIT_WINDOW`.
- Defaults keep the old average (100/min ≈ 1.667/s) with a burst of 20.
- `Retry-After` is the time until one token is available, rounded up to 1 s.

Unlike a per-minute window, the bucket bounds the burst: a tenant can never be
more than `BURST` ahead of its steady rate.

## What is deliberately not changed

- Idempotency, the pending→queued hand-off claim, retry policy, dead-lettering,
  liveness TTLs, and the Postgres reaper's selection logic.
- Weighting: all tenants get equal turns. Weighted fair queueing (per-tenant
  priorities) would be a later change.
