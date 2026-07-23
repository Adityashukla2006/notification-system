# Notification Delivery System

Send a notification through an HTTP API; workers deliver it in the background
and keep trying until it succeeds or is given up on.

Postgres holds the truth. Redis handles the queueing. If Redis is wiped, the
system rebuilds from Postgres and carries on.

```
POST /v1/notifications  →  202 Accepted  →  worker delivers  →  email sent
```

## Quick start

Everything in Docker, nothing else needed on the host:

```bash
docker compose --profile app up -d --wait
```

Mint an API key:

```bash
docker compose --profile app run --rm \
  --entrypoint /usr/local/bin/keygen api \
  -client-name local -name dashboard
```

Send one:

```bash
curl -X POST http://localhost:8080/v1/notifications \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"you@example.com",
       "payload":{"subject":"Hello","body":"It works."}}'
```

| | |
|---|---|
| API | http://localhost:8080 |
| Dashboard | http://localhost:3000 (pass `API_KEY=…`) |
| Sent mail | http://localhost:8025 |

Mail goes to a local catcher by default, so nothing real is sent.

## What it does

- **Retries** failed deliveries with exponential backoff and jitter, then
  dead-letters them.
- **Gives up early** on failures that retrying cannot fix, like a bad address.
- **Schedules** sends for later.
- **Never duplicates** a request — send the same `Idempotency-Key` twice and you
  get the original back.
- **Survives crashes.** A worker dying mid-send loses nothing.
- **Records every attempt**, with the provider's error and how long it took.
- **Rate limits** per client, and keeps each client's data private.

## How it works

```
client → API → Postgres (truth)
              ↘ Redis (queue) → worker → email provider
```

The API only validates and stores; it never talks to email providers, so a slow
vendor can't slow down your requests. Workers are a separate binary, so delivery
scales independently.

Four binaries in `api/cmd`: `server`, `worker`, `migrate`, `keygen`.

## Staying reliable

Three layers, each catching what the last one can't:

1. **Claims survive a crash.** A worker moves a job to its own list rather than
   removing it, and only confirms once the result is safely stored.
2. **Dead workers are noticed.** Each worker holds a key that expires; when it
   lapses, another worker picks up its unfinished jobs.
3. **Postgres is the backstop.** Anything stuck is found and requeued — this is
   what survives losing Redis entirely.

Nothing uses locks or leader election. Races are settled by the database and by
Redis return values.

→ [The reasoning behind these choices](docs/design.md)

## API

All `/v1` routes need `Authorization: Bearer <key>`.

| | |
|---|---|
| `POST /v1/notifications` | Accept one. Needs an `Idempotency-Key` header. Returns `202` |
| `GET /v1/notifications` | List, newest first. Filter by `status`, `channel` |
| `GET /v1/notifications/{id}` | One notification |
| `GET /v1/notifications/{id}/attempts` | What was tried, and what failed |
| `GET /healthz` `/readyz` | Probes, no auth |

Request body:

```jsonc
{
  "channel": "email",              // email | sms | push
  "recipient": "user@example.com",
  "payload": { "subject": "…", "body": "…" },
  "scheduled_at": "2026-01-01T09:00:00Z",  // optional
  "max_attempts": 5                        // optional
}
```

`202` means accepted, not delivered — delivery happens in the background.
Listing uses a cursor, returned as `next_cursor` and passed back as `?cursor=`.

## Running it without Docker

Needs Go 1.25+, Postgres, Node 22+, and Redis (from `docker compose up -d`).

```bash
cp .env.example .env       # fill in POSTGRES_*
. .\scripts\env.ps1        # nothing reads .env on its own

cd api
go run ./cmd/migrate       # apply the schema
go run ./cmd/server        # and ./cmd/worker in another terminal

cd ../web && npm install && npm run dev
```

Every setting is an environment variable — see [`.env.example`](.env.example).

## Tests

```powershell
.\scripts\test.ps1
```

The script loads `.env` first, because the store and queue tests skip without a
real Postgres and Redis. `TEST_DATABASE_URL` must point at a **throwaway
database** — those tests drop the tables. Don't run the suite while a worker is
running; they share Redis.

[CI](.github/workflows/ci.yml) runs everything on each push, including the race
detector, and fails rather than skips if the databases are missing.

## Layout

```
api/
  cmd/          server, worker, migrate, keygen
  internal/     store (SQL), queue (Redis), worker, provider, http, …
  migrations/   paired up/down SQL
web/            Next.js dashboard, read-only
docs/           design notes
```

## Not done yet

- Workers deliver one at a time; no concurrency.
- No metrics.
- SMS and push are stubs that log instead of sending.
- `attempts` counts failures, so a first-try success shows `0`.
