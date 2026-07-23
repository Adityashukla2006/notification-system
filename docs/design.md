# Design notes

Why this system is built the way it is. The [README](../README.md) covers what
it does; this covers why.

## The one requirement everything follows from

> Delivery is **at-least-once**. A notification may arrive more than once, but
> it must never be silently lost.

Exactly-once delivery across a network boundary isn't achievable, so the choice
is which failure to accept. Losing a password reset is worse than sending it
twice. That makes duplicates *normal* rather than exceptional, and the whole
design follows: every path is built to survive a repeat, and losing work is the
one outcome engineered against.

## Races are settled by the datastore

There is no leader election and no distributed lock anywhere in the system.
Every race is resolved by something the database or Redis already guarantees:

| Race | Settled by |
|---|---|
| Two requests with the same idempotency key | `UNIQUE (client_id, idempotency_key)` |
| Two workers at the retry ceiling | One `UPDATE` that increments *and* compares |
| Two workers promoting the same scheduled job | `ZREM`'s return value — only one gets `1` |
| Two reapers finding the same stuck row | `UPDATE … FOR UPDATE SKIP LOCKED` |

The idempotency one is worth spelling out. Checking "does this key exist?" then
inserting looks correct and isn't: two concurrent requests both pass the check
and both insert. The constraint is the only race-free option, which is why
idempotency lives in the schema rather than in Go.

The retry ceiling is the same shape. Two workers delivering the same
notification would both read `attempts = 4`, both conclude one remains, and
together grant an extra attempt. Doing the increment and the comparison in one
statement removes the gap.

## Recovery, in three layers

### 1. Claiming without destroying

A worker doesn't pop from the queue. `BLMOVE` moves the id onto a per-worker
processing list, and only an explicit ack removes it.

A destructive pop would delete the only record of the claim, so a worker dying
mid-delivery would take the notification with it. **The ack happens last**,
after the outcome is durable in Postgres — no path can release a claim without a
recorded result.

### 2. Telling dead from busy

A processing list looks identical whether its owner has died or is simply mid-
send, and Redis can't tell you which. So each worker refreshes a key with a TTL.
No key means no refresh means dead, and its claims are fair game.

- **Restart** — the worker drains its own list at startup, immediately.
- **Permanent death** — another worker reclaims once the TTL lapses.

The TTL must comfortably exceed the heartbeat interval. Set too tight, a worker
that is merely slow (a long GC pause, a stalled provider) gets declared dead and
its in-flight work is delivered a second time underneath it.

### 3. The Postgres backstop

The only path that survives losing Redis entirely, because it consults nothing
else. It sweeps for rows stranded in a non-terminal state:

- `pending` — the API died between insert and enqueue
- `queued` — the queue was wiped before any worker claimed it
- `delivering` — a worker vanished and its processing list went with it
- `failed` and due — a retry whose schedule entry was lost

This is what makes "rebuildable from Postgres" true rather than aspirational.

## Retries

`base × 2^attempt`, jittered, capped, then dead-lettered.

**Why jitter.** Without it, a provider outage that fails a thousand
notifications at once retries all thousand in lockstep — hitting the recovering
provider with the same spike that just knocked it over. Equal jitter (half
fixed, half random) is used rather than full jitter so the delay keeps a floor
and can't collapse back to retrying almost immediately.

**Why classify failures.** A bad mailbox fails identically forever. Spending
five backed-off retries on it wastes a worker, delays real work behind it, and —
for email specifically — repeatedly pushing known-bad addresses is what damages
sender reputation. So permanent failures dead-letter at once.

When unsure, a failure stays transient. The asymmetry decides it: misclassifying
transient as permanent silently drops a deliverable message, while the reverse
costs only a few retries.

## Scheduling and retries share one mechanism

A sorted set scored by "deliver no earlier than T". Both a client asking for a
9am send and a worker backing off after a failure are the same statement, so
they use the same structure.

The next-attempt time is written to `scheduled_at`, which already means
"earliest deliverable time". Reusing it means the retry schedule survives a
Redis wipe with no extra column.

## Storage choices

| Choice | Instead of | Why |
|---|---|---|
| Hand-written SQL over pgx | An ORM | The subtle parts *are* the SQL. An ORM hides the atomic `UPDATE` that makes the retry ceiling correct |
| UUIDv7 ids | UUIDv4 | Time-ordered, so inserts append to the index edge instead of scattering — and one column works as both sort key and pagination cursor |
| `TEXT` + `CHECK` | Native `ENUM` | New states via `ALTER CONSTRAINT`, not the near-irreversible `ALTER TYPE` |
| Queue holds ids only | Full payloads | Postgres and Redis can't drift |
| Cursor pagination | `OFFSET` | `OFFSET 500` walks and discards 500 rows, and a row inserted mid-pagination shifts everything so records repeat or vanish |
| Attempt history is separate | One `attempts` column | The counter drives retries; the table serves humans. Different questions |

Attempt history is written outside the delivery transaction, on purpose: it's
observational, and failing a delivery because its audit row couldn't be written
would trade something that matters for something that doesn't.

## Multi-tenancy

Every read query is scoped to the authenticated client in SQL, not filtered
afterwards. A notification belonging to another client returns **404, not 403** —
a 403 would confirm the id exists and let someone probe for real ones.

The dashboard reads the API key server-side only, deliberately without Next's
`NEXT_PUBLIC_` prefix, which would inline it into the browser bundle. The key
carries write access, so this is a security boundary rather than a preference.

## Rate limiting

A sliding window counter, not a fixed window. With a fixed window and a limit of
100/minute, a client can send 100 at 11:59:59 and 100 more at 12:00:00 — double
the intended rate, exactly at the boundary an attacker would aim for. Weighting
the previous bucket smooths that away, and still costs two counters per client
rather than the per-request timestamp log a true sliding window needs.

The limiter **fails open**. Rate limiting protects against load, but
authentication is the security boundary — a Redis outage shouldn't take the API
down with it. The trade is that breaking Redis also escapes rate limiting, so
the failure is logged loudly.

## Operational constraints

Two settings cause subtle misbehaviour rather than obvious failure, so both are
enforced at startup:

- `LIVENESS_TTL` must exceed `HEARTBEAT_EVERY`, or the key lapses between
  refreshes and every worker continuously declares itself dead.
- `DELIVERY_TIMEOUT` must stay below `STUCK_AFTER`, or the reaper requeues
  deliveries that are still legitimately running.

Migrations run as their own step, never at app startup, where N booting
instances would race each other through the same DDL.

## Things found by running it, not by testing it

Worth recording, because each was invisible to a passing test suite:

- **Double dot-stuffing.** `net/smtp`'s writer already escapes leading dots;
  doing it again delivered `..` where the sender wrote `.`. Only visible by
  reading a real received message.
- **`attempts` contradicting its own history.** A dead-lettered notification
  reported zero attempts while its attempt log showed one, because the permanent
  -failure path skipped the only statement that incremented the counter.
- **Silently dropped query parameters.** Go's `url.Values` discards what it
  can't parse, so a mangled cursor read as "no cursor" and returned page one —
  a paginating client would loop forever.
- **`command:` vs `entrypoint:` in Compose.** `command` supplies *arguments to*
  the image's `ENTRYPOINT`, so the migrate and worker services were both running
  the API server with a stray argument.
- **Compose auto-loads `.env`.** A worker reading `${RESEND_API_KEY}` sent live
  email through a real key just from starting the demo stack.
