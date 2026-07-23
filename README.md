# Distributed Notification Delivery System

An asynchronous notification delivery service. An HTTP API accepts requests and
returns immediately; separate worker processes deliver them through pluggable
providers, with retries, exponential backoff, dead-lettering, scheduled sends,
and end-to-end idempotency.

Postgres is the source of truth. Redis is ephemeral coordination — if it is
wiped entirely, the system rebuilds from Postgres and keeps delivering.

---

## Contents

- [Why it is built this way](#why-it-is-built-this-way)
- [Architecture](#architecture)
- [The delivery lifecycle](#the-delivery-lifecycle)
- [Reliability](#reliability)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Design decisions](#design-decisions)
- [Known gaps](#known-gaps)

---

## Why it is built this way

Sending a notification is easy. Sending one **exactly as often as intended, even
when things break**, is not. Almost every decision here follows from a single
requirement:

> Delivery is **at-least-once**. A notification may be delivered more than once,
> but it must never be silently lost.

That choice makes duplicates *normal* rather than exceptional, so the system is
built to survive them — and it makes losing work the one outcome worth
engineering against.

---

## Architecture

```
                 ┌────────────────┐
   client ──────▶│   API server   │  validate → authenticate → rate-limit
                 │  cmd/server    │  → persist → enqueue → 202 Accepted
                 └───────┬────────┘
                         │
              ┌──────────┴───────────┐
              ▼                      ▼
      ┌───────────────┐      ┌──────────────┐
      │   Postgres    │      │    Redis     │
      │ source of     │      │ queue (list) │
      │ truth         │      │ schedule (zset)
      │               │      │ rate limits  │
      └───────┬───────┘      └──────┬───────┘
              │                     │
              │      ┌──────────────┴───────┐
              └─────▶│      Worker(s)       │  claim → load → deliver
                     │    cmd/worker        │  → record → ack
                     └──────────┬───────────┘
                                ▼
                     ┌──────────────────────┐
                     │  Providers           │
                     │  Resend / SMTP / log │
                     └──────────────────────┘
```

**`cmd/server`** — the HTTP API. It validates, authenticates, rate-limits,
persists, enqueues, and returns `202`. It never calls a provider directly, so a
slow or broken email vendor cannot slow down request ingestion.

**`cmd/worker`** — a separate binary. It consumes the queue, delivers, records
every attempt, and schedules retries or dead-letters. Being separate means
delivery capacity scales — and fails — independently of ingestion.

**`cmd/migrate`** — applies schema migrations as a deploy step, never at app
startup where N booting instances would race through the same DDL.

**`cmd/keygen`** — mints API keys. It solves the bootstrap problem: the endpoint
that would create keys must itself be authenticated, so the first key is created
out-of-band with direct database access.

**`web/`** — a read-only Next.js dashboard.

---

## The delivery lifecycle

```
pending ──▶ queued ──▶ delivering ──▶ delivered
                │           │
                │           ├──▶ failed ──▶ (backoff) ──▶ queued ──▶ …
                │           │
                └───────────┴──▶ dead_lettered
```

| Status | Meaning |
|---|---|
| `pending` | Persisted, not yet handed to Redis |
| `queued` | On the ready queue, or waiting on the schedule |
| `delivering` | A worker has it in flight |
| `delivered` | A provider accepted it |
| `failed` | An attempt failed; a retry is scheduled |
| `dead_lettered` | Retries exhausted, or a failure that retrying cannot fix |

---

## Reliability

Three independent recovery layers, each catching what the one before it
structurally cannot.

### 1. Claims survive a crash

A worker does not *pop* work off the queue — that would destroy the only record
of the claim, so a worker dying mid-delivery would take the notification with
it. Instead `BLMOVE` **moves** the id onto a per-worker processing list, and
only an explicit ack removes it.

**The ack always happens last**, after the outcome is durable in Postgres. No
code path can release a claim without a recorded result.

### 2. Abandoned claims are reclaimed

A processing list looks identical whether its owner is dead or merely busy, and
Redis cannot tell you which. So each worker proves it is alive by refreshing a
key with a TTL. A list whose owner has no live key is fair game.

- **Restart** (same worker id): drains its own list at startup — immediate.
- **Permanent death**: another worker reclaims once the TTL lapses (~30s).

### 3. The Postgres reaper

The final backstop, and the only path that survives losing Redis entirely,
because it consults nothing else. It sweeps for rows stranded in a non-terminal
state:

- `pending` — the API died between insert and enqueue
- `queued` — the queue was wiped before any worker claimed it
- `delivering` — a worker vanished and its processing list is gone too
- `failed` and due — a retry whose schedule entry was lost

### Races are resolved by the datastore, never by coordination code

There is no leader election and no distributed lock anywhere:

| Race | Resolved by |
|---|---|
| Two requests, same idempotency key | `UNIQUE (client_id, idempotency_key)` |
| Two workers past the retry ceiling | One `UPDATE` that increments *and* compares |
| Two promoters, same due notification | `ZREM`'s return value — only one gets `1` |
| Two reapers, same stranded row | `UPDATE … FOR UPDATE SKIP LOCKED` |

### Retry backoff

`base × 2^attempt`, **equal jitter**, capped, then dead-lettered.

Jitter matters more than it looks: without it, a provider outage that fails a
thousand notifications at once retries all thousand in lockstep, hitting the
recovering provider with the same spike that just knocked it over.

Failures are also **classified**. A permanent failure — an unusable address, a
`5xx` SMTP reply, a `4xx` from the API — dead-letters immediately rather than
consuming retries it can never pass. When unsure, a failure stays transient:
misclassifying transient as permanent silently drops a deliverable message,
while the reverse costs only a few retries.

---

## Getting started

### Prerequisites

- Go 1.25+
- Postgres 17 (native install, or add a container)
- Docker (for Redis and the local mail catcher)
- Node 22+ (for the dashboard)

### 1. Start Redis and Mailpit

```bash
docker compose up -d --wait
```

Mailpit is a local mail server that accepts everything and delivers nothing
onward, so email can be exercised without sending real mail. Inbox:
<http://localhost:8025>

> Postgres is deliberately **not** in compose: this project assumes a native
> install on 5432. Publishing a container on the same port means the native
> server silently wins every host connection, and the resulting auth failures
> point at entirely the wrong server.

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` — at minimum the `POSTGRES_*` values. Nothing in the project reads
`.env` on its own, so load it before running anything:

```powershell
. .\scripts\env.ps1
```

### 3. Apply migrations

```bash
cd api
go run ./cmd/migrate -status    # what is pending
go run ./cmd/migrate            # apply it
```

Adopting a database whose schema was created by hand:
`go run ./cmd/migrate -baseline 2` records a version without executing anything.

### 4. Mint an API key

```bash
go run ./cmd/keygen -client-name "local" -name "dev"
```

The token is printed **once** and is unrecoverable afterwards — only its hash is
stored.

### 5. Run the API and a worker

```bash
go run ./cmd/server    # :8080
go run ./cmd/worker    # separate terminal
```

### 6. Run the dashboard

```bash
cd web
cp .env.local.example .env.local   # then paste the API key from step 4
npm install
npm run dev                        # http://localhost:3000
```

The dashboard is **read-only**: it lists notifications with status and channel
filters, cursor pagination, and a detail view showing the payload and every
delivery attempt with its error and latency.

> The API key is read server-side only, deliberately **without** a
> `NEXT_PUBLIC_` prefix. That prefix would inline it into the JavaScript bundle,
> and the key carries write access — anyone with it could send notifications as
> you. Every API call is made by the Next server; the browser never sees it.
> `src/lib/api.ts` imports `server-only`, so importing it from a Client
> Component is a build error rather than a leaked credential.

### 7. Send one

```bash
curl -X POST http://localhost:8080/v1/notifications \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{
        "channel": "email",
        "recipient": "someone@example.com",
        "payload": {"subject": "Hello", "body": "It works."}
      }'
```

---

## Configuration

Every setting is an environment variable parsed into one typed struct at boot,
so a misconfigured process fails immediately with a clear error rather than
midway through a request. Full list in [`.env.example`](.env.example).

| Group | Notable variables |
|---|---|
| HTTP | `HTTP_ADDR`, `SHUTDOWN_GRACE`, `LOG_LEVEL` |
| Postgres | `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DBNAME` |
| Redis | `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` |
| Rate limit | `RATE_LIMIT_REQUESTS`, `RATE_LIMIT_WINDOW` |
| Worker | `WORKER_CLAIM_TIMEOUT`, `WORKER_PROMOTE_EVERY`, `WORKER_DELIVERY_TIMEOUT` |
| Retry | `WORKER_RETRY_BASE`, `WORKER_RETRY_MAX` |
| Recovery | `WORKER_HEARTBEAT_EVERY`, `WORKER_LIVENESS_TTL`, `WORKER_REAP_EVERY`, `WORKER_STUCK_AFTER` |
| Email | `RESEND_API_KEY`, `RESEND_FROM`, or `SMTP_*` |

Two constraints are enforced at construction, because violating them causes
subtle misbehavior rather than an obvious failure:

- `WORKER_LIVENESS_TTL` must exceed `WORKER_HEARTBEAT_EVERY` — otherwise the key
  lapses between refreshes and every worker continuously declares itself dead.
- `WORKER_DELIVERY_TIMEOUT` must be below `WORKER_STUCK_AFTER` — otherwise the
  reaper requeues deliveries that are still legitimately running.

### Email providers

Resolved in order: **Resend** (if `RESEND_API_KEY` is set) → **SMTP** (if
`SMTP_HOST` is set) → **logging stub**. The system runs end to end with no mail
credentials at all.

---

## API reference

All `/v1` routes require `Authorization: Bearer <api-key>` and are rate-limited
per client. Health endpoints are public — a throttled liveness probe would get
the process restarted exactly when it is busiest.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness. Touches no dependencies |
| `GET` | `/readyz` | Readiness. Pings Postgres and Redis |
| `GET` | `/v1/me` | The authenticated client id |
| `POST` | `/v1/notifications` | Accept a notification |
| `GET` | `/v1/notifications` | List, newest first |
| `GET` | `/v1/notifications/{id}` | One notification |
| `GET` | `/v1/notifications/{id}/attempts` | Delivery history |

### Accepting a notification

`POST /v1/notifications` — requires an `Idempotency-Key` header.

```jsonc
{
  "channel": "email",              // email | sms | push
  "recipient": "user@example.com",
  "payload": { "subject": "…", "body": "…", "html": false },
  "scheduled_at": "2026-01-01T09:00:00Z",  // optional, deliver no earlier than
  "max_attempts": 5                        // optional
}
```

Returns **`202 Accepted`**, never `200`. The response means *accepted*, never
*delivered* — delivery is asynchronous.

| Code | Meaning |
|---|---|
| `202` | Accepted, or an identical replay of an existing key |
| `400` | Validation failure; the body names the offending field |
| `401` | Missing or invalid API key |
| `409` | Idempotency key reused with a **different** request |
| `429` | Rate limited; see `Retry-After` |

### Idempotency

Retrying with the same `Idempotency-Key` returns the original notification
rather than creating a duplicate. The guarantee is enforced by a `UNIQUE`
constraint in the database, never by a read-then-check in Go — two concurrent
requests would both pass such a check and both insert.

### Listing

`GET /v1/notifications?status=failed&channel=email&limit=25&cursor=…`

Pagination is by **cursor, never `OFFSET`**. `OFFSET 500` makes the database walk
and discard 500 rows, and a row inserted mid-pagination shifts everything so
records are seen twice or skipped. A cursor is a stable position that costs the
same on page 500 as page 1.

```jsonc
{ "data": [ /* … */ ], "next_cursor": "MDE5ZjhlNjIt…" }  // null on the last page
```

Tenant isolation is enforced in the SQL itself. A notification belonging to
another client returns `404`, not `403` — a `403` would confirm the id exists.

---

## Testing

```powershell
.\scripts\test.ps1                      # everything
.\scripts\test.ps1 ./internal/store/    # one package
```

The script exists because the store, queue, and rate-limit tests **skip** when
`TEST_DATABASE_URL` and `TEST_REDIS_ADDR` are unset — a bare `go test ./...`
reports a confident green while ~40 tests against real infrastructure never run.

> **`TEST_DATABASE_URL` must point at a disposable database.** The store tests
> apply every migration down then up, which drops the tables.
> `CREATE DATABASE notifications_test;`

> **Do not run the suite while a worker is running.** They share one Redis, and
> the worker claims the ids the tests enqueue.

Roughly 200 tests across ten packages: table-driven unit tests with fakes, plus
integration tests against real Postgres and Redis for anything whose behavior
depends on the datastore's exact semantics.

---

## Project layout

```
api/
  cmd/
    server/     HTTP API binary
    worker/     delivery loop binary
    migrate/    schema deploy step
    keygen/     API key bootstrap
  internal/
    config/     env → one typed struct, parsed once at boot
    store/      hand-written SQL over pgx; owns every query
    queue/      Redis: ready queue, schedule, reclaimer
    worker/     claim → load → deliver → record → ack
    provider/   pluggable delivery: Resend, SMTP, logging stub
    retry/      backoff schedule
    ratelimit/  sliding-window counter
    http/       router, handlers, auth and rate-limit middleware
    auth/       API key generation and verification
  migrations/   golang-migrate SQL, paired up/down
web/
  src/app/       routes: list, detail, error and loading states
  src/components/ table, filters, pagination, attempt history
  src/lib/       server-only API client, types, formatting
scripts/        env loading, test runner
```

Nothing outside the module imports `internal/`. Both binaries share it.

---

## Design decisions

| Decision | Rejected alternative | Why |
|---|---|---|
| Hand-written SQL over pgx | An ORM | The subtle parts *are* the SQL — an ORM hides the atomic `UPDATE` that makes the retry ceiling correct |
| `UNIQUE` constraint for idempotency | Read-then-insert | Two concurrent requests both pass the check and both insert |
| `202`, at-least-once | `200`, exactly-once | Exactly-once across a network boundary is not achievable; duplicates are made safe instead |
| `BLMOVE` reliable queue | `BRPOP` | A destructive pop loses the id when a worker dies |
| UUIDv7 ids | UUIDv4 | Time-ordered, so inserts append to the index edge instead of scattering — and one column serves as both sort key and cursor |
| `TEXT` + `CHECK` | Native `ENUM` | New states via `ALTER CONSTRAINT`, not the near-irreversible `ALTER TYPE` |
| Reused `scheduled_at` for retries | A new column | It already means "earliest deliverable time", so the schedule survives a Redis wipe with no migration |
| One sorted set for scheduling *and* retries | Two mechanisms | Both are "not before T" |
| Sliding-window rate limit | Fixed window | A fixed window lets a client double its rate across the boundary |
| Attempt history is non-blocking | Same transaction as status | Never fail a delivery because its audit row could not be written |
| Interfaces declared where consumed | Importing concrete types | Each layer lists only what it uses, so it is testable with fakes |
| Migrations as a deploy step | On app startup | N booting instances would race through the same DDL |

---

## Known gaps

- **Worker concurrency** — one delivery at a time per worker; throughput is
  bounded by provider latency.
- **SMS and push** — still logging stubs. Each becomes real by writing one
  `Deliver` method and registering it; the worker loop does not change.
- **CI** — needs a pipeline that *fails* rather than skips when the `TEST_*`
  variables are absent.
- **Test isolation** — the suite and a running worker share one Redis; tests
  should use a dedicated database index.
- **`attempts` semantics** — the counter tracks failures, so a first-try success
  reports `0`.
