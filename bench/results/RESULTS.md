# Benchmark results — Distributed Notification Delivery System

Status: **v2**: the bench-v1 baseline (tag `bench-v1`: hot-row lock fix and
enqueue-race fix) plus the tenant-fairness fix (per-tenant queues with
round-robin claiming, and a per-tenant token bucket). Sections 3–7 are the
bench-v1 baseline; section 8 is the v2 re-measurement and its before/after.

Every number below was produced by a run in this benchmark session and is read
from a file under `bench/results/`. Nothing is estimated or extrapolated. Tables
show `median (min–max)` over 3 repetitions unless they list reps individually.
`bench/results/summary.md` is regenerated from the raw files by `make bench-report`
and holds the same tables.

---

## 1. Environment

From `results/environment.txt` (captured 2026-09-30T07:17:31Z).

| | |
|---|---|
| Host | Intel Core i5-1240P, 12 cores (4P + 8E) / 16 threads, 16 GB RAM, laptop |
| OS | Windows 11 Home Single Language 10.0.26200 |
| Docker | Docker Desktop 29.6.1, WSL2 backend (kernel 6.6.87.2), VM: 16 CPUs, 7.6 GB |
| Go | 1.25.5 (module), built in `golang:1.25-alpine` |
| Postgres | 17.11, stock except `max_connections=300`; `fsync=on`, `synchronous_commit=on`, `shared_buffers=128MB` |
| Redis | 7.4.11, stock (no AOF) |
| Load generator | k6 v1.3.0 (`grafana/k6:1.3.0`), same Docker network as the API |

Container limits (`bench/docker-compose.yml`):

| Container | CPUs | Memory | Notes |
|---|---|---|---|
| api | 2 | 512 MB | pgx pool 40. Per-tenant rate limit lifted except in test 7 (v1: 100,000,000/min window; v2: token bucket 100,000,000/s) |
| worker (each) | 1 | 512 MB | 10 delivery slots per instance unless stated; pool = slots + 5 |
| postgres | 4 | 2 GB | |
| redis | 1 | 512 MB | |
| k6 | 4 | 2 GB | Raised from 2 after k6 itself saturated (see §9) |

Worker settings: claim timeout 1 s, retry base 1 s doubling with jitter, cap 5 min,
`max_attempts` 5, liveness TTL 30 s, reclaim sweep 30 s.

**Provider:** every channel uses the simulated provider — nothing is sent. Call
latency is uniform 25–75 ms (50 ± 25 ms). A **send ledger** (Redis hash
`sim:sends`, id → count) records every successful simulated send, so duplicate
deliveries are counted even when a crash hides them from Postgres.

**Host memory during the runs:** other applications left little free RAM. Free
memory was logged at the start and end of every test (`results/memory.log`):
68 readings, **min 358 MB, max 2,797 MB**. The requested memory floors were
waived by the owner. Low free memory can add latency noise; see §10.

---

## 2. How to reproduce

```bash
make bench-up          # build images, start the isolated "notibench" stack, create 4 tenants
make bench-env         # write results/environment.txt
make bench-pilot       # PILOT_TAG=before|latest   before/after of the hot-row fix
make bench-enqueue     # ENQ_TAG=before|latest     exactly-one-enqueue
make bench-ingest      # INGEST_STEPS="200 400 600 800 1000 1200" INGEST_DURATION=30s
make bench-scaling
make bench-e2e         # E2E_RATES="130 400"
make bench-failures
make bench-crash
make bench-idempotency
make bench-fairness
make bench-burst
make bench-report      # regenerate results/summary.md
make bench-down
```

`REPS` (default 3) and `REP_FROM` select repetitions. Each run resets the
notification tables, queues and ledger first. Correctness is always checked with
SQL against Postgres (`bench/sql/*.sql`); each saved `*.verify.txt`,
`*.latency.txt`, `*.throughput.txt`, `*.failures.txt` contains the query name and
its full output.

Measurement sources:

| Metric | Source |
|---|---|
| Ingestion throughput/latency | k6 JSON summary (`*.k6.json`) |
| End-to-end latency | `notifications.updated_at − created_at` for delivered rows (both Postgres clock) |
| Throughput / drain | Postgres timestamps (`throughput.sql`) |
| Lost | every id from a 202 response (logged by k6 to `*.ids`, loaded into `bench_accepted`) joined to `notifications`; lost = no row, or row not `delivered`/`dead_lettered`. The `*.ids` lists (84 MB) stay on disk and are not committed; the committed `*.verify.txt` holds the result |
| Duplicate deliveries | provider ledger (`bench_sends`) and Postgres attempts |
| Queue depth | Redis sampled every 200 ms by one atomic Lua call (`*.queue.tsv`) |

---

## 3. Bugs found in the application, and the before/after pilot

Two application bugs were found, root-caused, and fixed (commit `bench-v1`).

### 3a. Ingestion serialized on a hot `api_keys` row

**Cause.** Every authenticated request ran
`UPDATE api_keys SET last_used_at = now() WHERE id = $1`. All of a tenant's
requests update the same row, and Postgres holds a row lock until the commit is
flushed, so one tenant's requests went one durable commit at a time. In one
`pg_stat_activity` sample taken at 1,000 req/s offered, 28 of 39 active sessions
were in that statement and 24 were waiting on `Lock:transactionid`/`Lock:tuple`.
(That sample was a live diagnostic query printed during the session and not
saved to a file; the saved evidence for this bug is the 1-vs-4-tenant pilot
below.)

**Fix.** Write only when stale:
`... AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')`.
A row that fails the `WHERE` is neither locked nor written.

**Before/after** (`results/pilot_clean/`, same harness, 1,000 req/s offered
open-loop for 15 s, no warm-up, workers stopped, 3 reps; "before" is the image
built from commit `c3dd9d4`, verified to lack both fixes' SQL in its binary):

| Build | Tenants | Achieved req/s | p99 ms | Dropped iterations (of ~15,000) |
|---|---|---|---|---|
| before | 1 | 141 (128–146) | 8127 (7504–9714) | 12417 (12330–12584) |
| before | 4 | 343 (308–396) | 2914 (2873–3645) | 9237 (8464–9809) |
| after | 1 | 770 (767–832) | 1412 (948–1755) | 2644 (2204–2786) |
| after | 4 | 753 (712–844) | 1338 (1002–2805) | 3218 (1803–3499) |

- Single-tenant throughput: **141 → 770 req/s (5.5×)**.
- Throughput no longer depends on tenant count: 1-tenant/4-tenant ratio
  **0.41 before, 1.02 after**.
- Both builds are still saturated at 1,000 req/s offered (they drop requests
  and p99 is > 900 ms). This table shows the *ceiling* moved; §4 test 1 gives
  the latency-bounded capacity.

### 3b. Concurrent duplicates enqueued the same notification several times

**Cause.** Duplicate requests with one `Idempotency-Key` lose the INSERT race and
read the row back. Until the winner finished, they all read `pending` and each
pushed the id to Redis. Rows stayed unique, but the queue held several copies,
so several workers could deliver one notification concurrently.

**Fix.** A conditional transition, `UPDATE … SET status='queued' WHERE id=$1 AND
status='pending'`: only the caller that moves the row out of `pending` pushes it.
On a failed push the row is returned to `pending` so a client retry can enqueue
it. The transition now happens before the push, which also removes a race where
the API could overwrite a worker's `delivering` status back to `queued`.

**Before/after** (`results/pilot_clean/enqueue-*`; 5,000 keys × 5 concurrent
identical copies, workers stopped so every push stays visible, 3 reps):

| Build | Rows | Queue entries (LLEN) | IDs pushed more than once |
|---|---|---|---|
| before | 5000 | 15254 (15113–15434) | 4425 (4351–4435) |
| after | 5000 | 5000 (5000–5000) | 0 (0–0) |

Before the fix, 87–89% of concurrently duplicated keys were enqueued more than
once. A single key × 1,000 concurrent copies did **not** reproduce it (LLEN 1 on
both builds, earlier runs now in `_discarded/pilot-under-memory-pressure/`); it
takes many keys to hit the few-millisecond window.

Tests added: store tests against real Postgres (`TestClaimHandOffExactlyOnce`,
50 concurrent callers → exactly 1 winner; `TestReleaseHandOff`;
`TestTouchAPIKeyLastUsedSkipsRecentWrite`) and service table tests for every
claim branch.

---

## 4. Results

### Test 1 — Ingestion throughput and latency

Open loop (constant arrival rate), 4 tenants, workers stopped so only the
`POST` path is measured. Each step: **10 s warm-up at the same rate, then 30 s
measured**; only the measured window counts.

**Pass criteria, fixed before the runs:** error rate < 1% AND dropped
iterations < 1% of offered AND p99 ≤ 100 ms.

| Offered req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Error % | Dropped % | Reps passing |
|---|---|---|---|---|---|---|---|
| 200 | 200 (200–200) | 12.8 (12.5–12.9) | 20.4 (19.4–21.2) | 28.8 (24.0–37.0) | 0.0 | 0.0 | 3/3 |
| 400 | 400 (400–400) | 15.2 (14.9–17.0) | 23.7 (21.9–28.3) | 45.7 (44.3–81.6) | 0.0 | 0.0 | 3/3 |
| 600 | 600 (600–600) | 18.8 (17.5–20.2) | 51.0 (32.1–77.8) | 128 (79–139) | 0.0 | 0.0 | 1/3 |
| 800 | 789 (732–799) | 35 (19–219) | 337 (102–1353) | 385 (187–1426) | 0.0 | 1.4 (0.2–8.5) | 0/3 |
| 1000 | 970 (793–999) | 40 (23–1032) | 459 (128–1533) | 508 (200–1580) | 0.0 | 3.0 (0.1–20.7) | 0/3 |
| 1200 | 1105 (832–1179) | 416 (115–1437) | 792 (331–1747) | 973 (393–1805) | 0.0 | 8.0 (1.8–30.7) | 0/3 |

**Max sustained: 400 req/s** (highest passing step per rep: 600, 400, 400).
At 400 req/s: p50 15.2 ms, p95 23.7 ms, p99 45.7 ms, 0 errors.
No request returned an error at any rate; overload shows up as latency and as
requests k6 could not send (dropped iterations), not as 5xx.

Why it saturates (diagnosis, not a saved measurement): a live `docker stats` /
`pg_stat_activity` sample at 600 req/s, during the aborted no-warm-up run, showed
the API container at 165% CPU (of its 2-CPU limit) and Postgres sessions waiting
on `LWLock:WALWrite`. Every accepted request is two durable commits (INSERT, and
the `queued` transition).

#### Reconciling 770 req/s (pilot) with 400 req/s (test 1)

Both are measured; they answer different questions.

| | Pilot "after", 1 tenant | Test 1 "max sustained" |
|---|---|---|
| Question | How much does the API push through when overloaded? | Highest rate it handles *well* |
| Offered load | fixed 1,000 req/s (above capacity) | stepped 200 → 1,200 |
| Latency criterion | none | p99 ≤ 100 ms, drops < 1%, errors < 1% |
| At the reported rate | p99 1,412 ms; 14.7–18.6% of requests dropped | p99 45.7 ms; 0 dropped |
| Warm-up | none (15 s from cold) | 10 s warm-up, 30 s measured |
| Tenants | 1 | 4 (tenant count no longer matters: 770 vs 753 in the pilot) |
| Metric | k6 `http_reqs` rate over the whole run | requests in the measured window ÷ 30 s |

Test 1 also shows the overload ceiling directly: at 1,000 and 1,200 req/s offered
it achieved 970 and 1105 req/s (medians), with p99 508 and 973 ms. The pilot's
770 is lower than that; its runs start cold with no warm-up, and a per-second
trace at 400 req/s (`results/_diag/r400.csv.gz`) showed the first ~2 s of a cold
run at p50 151 ms / p99 660 ms versus ~20 ms afterwards. That trace is evidence
of a cold-start penalty; that it fully explains the 770 vs 970 gap is an
inference, not a measurement.

**How to quote:** "400 req/s sustained at p99 < 50 ms" (test 1) is the
capacity number. "141 → 770 req/s for a single tenant" is the before/after of
the lock fix under identical overload. Do not present 770 as sustained capacity.

Note: an earlier pilot run reported 873 req/s for 1 tenant after the fix. It
ran under heavier memory pressure and was superseded by the clean re-run; it is
kept in `_discarded/pilot-under-memory-pressure/` and not used.

### Test 2 — End-to-end delivery latency

Accepted (`created_at`) → delivered (`updated_at` of the delivered row), 4
worker instances × 10 slots, open loop for 60 s, 4 tenants. Rates chosen from
test 3: 130 req/s ≈ 25% of the 4-instance delivery capacity (531/s); 400 req/s
≈ 75% of it and equal to the max sustained ingestion rate.

| Offered req/s | Delivered | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|
| 130 | 7801 (7801–7801) | 75.3 (75.2–76.2) | 100 (100–101) | 141 (118–143) | 406 (193–513) |
| 400 | 23974 (23925–23985) | 93.7 (92.1–95.4) | 336 (260–404) | 507 (493–553) | 680 (656–694) |

The simulated provider call alone is 25–75 ms, so at 130 req/s the pipeline
adds ~25 ms at the median. Every 202 was delivered (0 lost, 0 duplicate sends).
At 400 req/s, 16–75 requests per run were never sent by k6 (dropped iterations,
0.07–0.31%), which is why delivered < 24,000.

### Test 3 — Worker scaling

Backlog drain: 20,000 notifications queued with workers stopped, then N
instances started. 10 slots and 1 CPU per instance, provider 50 ± 25 ms.
Throughput = delivered ÷ (last terminal write − first attempt start).

| Instances | Slots | Delivered/s | Drain window s | Speed-up | Efficiency |
|---|---|---|---|---|---|
| 1 | 10 | 148 (147–149) | 136 (135–136) | 1.00× | 100% |
| 2 | 20 | 285 (285–287) | 70.1 (69.8–70.3) | 1.93× | 97% |
| 4 | 40 | 531 (525–532) | 37.6 (37.6–38.1) | 3.60× | 90% |
| 8 | 80 | 842 (839–852) | 23.8 (23.5–23.8) | 5.71× | 71% |

Near-linear to 4 instances; efficiency falls to 71% at 8. All 12 runs:
240,000 accepted, 240,000 delivered, 0 missing, 0 duplicate sends. What limits
8 instances was not isolated in this suite (candidates: Postgres commit rate —
each delivery is ≥ 3 writes — and host CPU contention, since 8 workers + Postgres
+ Redis share one laptop).

### Test 4 — Provider failures

Transient failure injected at 10/30/50%, `max_attempts` 5, 200 req/s for 25 s,
4 instances × 10 slots, run until every row is terminal.

| Fail rate | Notifications | Delivered % | Dead-lettered | Observed fail % | Lost |
|---|---|---|---|---|---|
| 10% | 5001 (4986–5001) | 100 (100–100) | 0 (0–0) | 10.3 (9.6–10.3) | 0 |
| 30% | 5001 (4997–5001) | 99.8 (99.7–99.8) | 12 (10–13) | 29.4 (29.2–29.9) | 0 |
| 50% | 5001 (4986–5001) | 97.0 (96.8–97.1) | 152 (145–161) | 50.0 (49.5–50.6) | 0 |

- **Zero lost:** in all 9 runs every accepted id is `delivered` or
  `dead_lettered` (`*.verify.txt`: `accepted_without_row = 0`, `not_terminal = 0`).
- Every dead-lettered row has exactly 5 attempts (`*.failures.txt`).
- The DLQ count matches theory (a notification dead-letters only if all 5
  attempts fail, probability p⁵): expected ≈ 12 at 30% and ≈ 156 at 50%
  of 5,001; observed 10–13 and 145–161.
- 0 duplicate sends in the ledger across all 9 runs.

### Test 5 — Crash recovery (at-least-once)

2 worker instances × 10 slots, 200 req/s for 90 s. At 30 s, `docker kill -s KILL`
worker 1, capture its processing lists, restart it. Death and restart times are
Docker's `State.FinishedAt`/`State.StartedAt` (same VM clock as Postgres).

| Rep | Accepted | Delivered | Lost | In-flight at kill | Of those delivered | Down s | Recovery s from death | Recovery s from restart | Ids sent twice (ledger) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 17990 | 17990 | 0 | 6 | 6 | 7.14 | 7.83 | 0.69 | 0 |
| 2 | 18000 | 18000 | 0 | 7 | 7 | 6.61 | 7.16 | 0.55 | 1 |
| 3 | 18000 | 18000 | 0 | 5 | 5 | 6.66 | 7.09 | 0.42 | 0 |

- **Lost: 0** in all reps (53,990 accepted, 53,990 delivered).
- Every in-flight claim was recovered by the restarted worker's startup
  `Drain` (log lines "recovered claims from a previous run"), within
  **0.42–0.69 s of restart**.
- **Duplicates: 1 in 53,990** (rep 2). Evidence from the ledger and attempt
  history: its first send finished at 08:39:13.277, the process died at
  08:39:13.296 (19 ms later, before `delivered` was written and the claim acked),
  and it was re-sent at 08:39:20.319 after the restart. This is the documented
  at-least-once window, not a new bug.
- Three further evidence reps (`results/crash-evidence/`, older kill-time
  method, not in the table) showed 1, 0 and 1 duplicates; one of them was sent
  twice but has only **one** attempt row in Postgres — the worker died between
  the provider's acceptance and the attempt insert, a duplicate only the
  provider-side ledger can see.
- Downtime is ~6.6–7.1 s rather than the planned 5 s because each `docker`
  call takes 1–3 s on this host; it is reported as measured.

### Test 6 — Idempotency

1,000 virtual users send the same request with the same `Idempotency-Key`,
released at one instant; 20 s later one request reuses the key with a changed
body. Workers running (2 × 10).

| Rep | Rows | 202 responses | Distinct ids returned | Changed body | Provider sends |
|---|---|---|---|---|---|
| 1 | 1 | 1000 | 1 | 409 | 1 |
| 2 | 1 | 1000 | 1 | 409 | 1 |
| 3 | 1 | 1000 | 1 | 409 | 1 |

Rows from `SELECT idempotency_key, count(*) FROM notifications GROUP BY 1`; the
409 is a k6 check (`root_group.checks`) plus a logged `CONFLICT_STATUS 409`.

### Test 7 — Tenant fairness — **measured limitation in v1 (fixed in v2, see §8a)**

A quiet tenant sends 20/s; a noisy tenant sends 200/s (10×) for the same 60 s.
1 worker instance × 10 slots (≈ 148/s capacity, test 3). Latency is the quiet
tenant's end-to-end latency.

| Mode | Quiet p50 ms | Quiet p95 ms | Quiet p99 ms | Quiet max ms | Noisy accepted / sent |
|---|---|---|---|---|---|
| Quiet alone | 69.7 (69.5–70.8) | 92.9 (92.1–95.4) | 98 (96–105) | 106 (104–162) | — |
| + noisy, no limit | 18044 (16202–19276) | 33467 (29553–36220) | 34609 (30725–37458) | 34902 (30965–37782) | 12001 / 12001 |
| + noisy, limit 6,000/min | 7706 (7093–8663) | 16798 (16040–17456) | 17691 (16793–18220) | 17909 (16961–18422) | 6198 / 12001 |

**The quiet tenant is starved.** Its p99 rises from 98 ms to 34.6 s, and the
existing rate limit only halves that (17.7 s).

**Timeline evidence** (`fairness/limited-r1.timeline.{sql,txt}`, also r3):

| Seconds | Noisy accepted | Quiet accepted | Quiet p50 ms | Quiet max ms |
|---|---|---|---|---|
| 0 | 1006 | 101 | 1899 | 3249 |
| 5 | 1000 | 100 | 4577 | 5822 |
| 10 | 1000 | 100 | 7181 | 8536 |
| 15 | 999 | 100 | 9906 | 11263 |
| 20 | 1001 | 100 | 12524 | 13929 |
| 25 | 1000 | 100 | 15276 | 16494 |
| 30 | 192 | 100 | 15590 | 16961 |
| 35 | 0 | 100 | 11285 | 13431 |
| 40 | 0 | 100 | 6934 | 9083 |
| 45 | 0 | 100 | 2616 | 4783 |
| 50 | 0 | 100 | 75 | 445 |
| 55 | 0 | 99 | 70 | 100 |

**Cause.** (1) All tenants share one FIFO list (`notifications:queue`), so a
quiet notification waits behind every noisy one ahead of it. (2) A per-minute
sliding window allows a burst: the noisy tenant spent its 6,000 budget in the
first ~30 s at 200/s. Arrivals were 220/s against ~148/s capacity, so the
backlog grew ~72/s; once the noisy tenant was cut off at ~31 s, the backlog
drained at ~128/s and the quiet tenant's latency returned to baseline at 50 s —
consistent with the table. Without a limit the backlog grows for the whole run.

Fixed in v2 (per-tenant queues with round-robin claiming, plus a per-tenant
token bucket at ingestion). Before/after in §8a.

### Test 8 — Burst

50,000 notifications sent as fast as the API accepts them (k6 closed loop, 200
VUs, 4 tenants) with 4 worker instances × 10 slots running. Queue depth sampled
every 200 ms.

| Rep | Send duration s | Accept rate req/s | Peak ready-queue depth | Drain s (first accepted → last delivered) | Delivered | Lost | Dup sends |
|---|---|---|---|---|---|---|---|
| 1 | 56.3 | 888 | 28534 | 108.67 | 50000 | 0 | 0 |
| 2 | 53.7 | 932 | 27728 | 104.41 | 50000 | 0 | 0 |
| 3 | 55.6 | 899 | 27552 | 106.29 | 50000 | 0 | 0 |

"A few seconds" was not achievable: the API accepts ~900 req/s, so 50,000
takes 53.7–56.3 s to send. The ready queue peaked at 27,552–28,534 and fully
drained 50.78–52.77 s after the last request was accepted
(`drain_after_last_created_s` in `burst/*.throughput.txt`).

---

## 5. Correctness summary (all SQL-verified)

| Guarantee | Evidence | Result |
|---|---|---|
| Zero lost | every accepted id joined to `notifications` (verify.sql) — 45 verified runs across tests 2–8 | 0 lost in all 45 |
| One row per idempotency key | `GROUP BY client_id, idempotency_key HAVING count(*) > 1` | 0 in all 45 |
| Exactly one enqueue per key | LLEN with workers stopped | 5,000 entries for 5,000 keys (after fix) |
| DLQ accounting | failures.sql | every DLQ row has 5 attempts; counts match p⁵ |
| Duplicate deliveries | provider ledger | 0 outside crash tests; 1 in 53,990 in test 5, root-caused |

---

## 6. Harness bugs found and fixed during the session

Each was found from the data, fixed, and the affected test re-run. Flawed
outputs are kept, not deleted.

1. **No warm-up in ingestion.** The first ingestion attempt showed p99 634 ms at
   200 req/s. A per-second trace (`_diag/r400.csv.gz`) put almost all of it in the
   first ~2 s (cold connections and pool). Fixed with a 10 s warm-up scenario
   and measuring only the `{scenario:load}` window. This changed the method
   after the criteria were set; the criteria themselves did not change.
   Old output: `_discarded/ingest-no-warmup-aborted/`.
2. **Broken duplicate counter.** A mangled escape made `tr -d` delete newlines,
   so "distinct ids on the queue" was always 1 and "pushed more than once"
   always 0. LLEN and row counts were unaffected. Caught because LLEN 14,541
   with 5,000 rows contradicted "0 duplicates". Fixed with a `queue_ids` helper
   verified by a self-test. Old output: `_discarded/pilot_clean-enqueue-*-broken-dupcount/`.
3. **Crash-test timing.** (a) Reading 10 processing lists with 10 `docker exec`
   calls delayed the restart to 33 s. (b) The kill time was a `psql` timestamp
   taken *before* `docker kill`, which lags 1–3 s, inflating recovery time.
   Fixed with one exec for the capture and Docker's own
   `State.FinishedAt`/`StartedAt`. Old outputs:
   `_discarded/crash-r1-restart-delayed-33s/`, `_discarded/crash-kill-time-lower-bound/`.
4. **409 not evidenced.** The changed-body request was only visible as "1
   non-2xx". Added a k6 check and a logged status; re-ran test 6. Old output:
   `_discarded/idempotency-no-409-evidence/`.

Other corrections: k6 was CPU-bound at 2 CPUs near 1,000 req/s and was raised to
4 (earlier pilot in `_pilot/k6-2cpu/`); the environment query printed
`shared_buffers` as `163848kB` (value and unit concatenated) and was fixed; the
summarizer could not parse single-column tables and was fixed.

## 7. Archived and discarded outputs

| Folder | What | Why not used |
|---|---|---|
| `_prior_unverified/` (local only, not committed) | Results from a session before this one | Not produced in this session |
| `_pilot/e2e-smoke/`, `_pilot/ingest-pilot/` | Harness smoke test, first ingestion pilot | Exploration before the bug fix |
| `_pilot/k6-2cpu/` | Before/after pilot with k6 at 2 CPUs | Load generator was a bottleneck |
| `_discarded/pilot-under-memory-pressure/` | Second before/after pilot (873 req/s) and first enqueue runs | Superseded by `pilot_clean/` |
| `_discarded/ingest-no-warmup-aborted/` | Ingestion without warm-up | Harness bug 1 |
| `_discarded/ingest-rep1-interrupted-by-memory-kill/` | Ingestion rep 1 | Run killed for low host memory |
| `_discarded/pilot_clean-enqueue-*-broken-dupcount/` | Enqueue runs | Harness bug 2 |
| `_discarded/crash-*` | Crash reps | Harness bug 3 |
| `_discarded/idempotency-no-409-evidence/` | Idempotency reps | Harness bug 4 |
| `crash-evidence/` | 3 extra crash reps | Evidence for the duplicate mechanism only |

## 8. v2: fairness fix — before/after

Design: [`bench/DESIGN-fair-queue.md`](../DESIGN-fair-queue.md). In short:

- **Per-tenant queues, round-robin claims.** Each tenant has its own Redis list;
  a ring of active tenants is rotated by one atomic Lua script per claim, so
  consecutive claims visit tenants in turn. The claim is still one atomic move
  into the worker's own processing list, and every recovery path (restart
  `Drain`, dead-worker reclaim, retry promotion, Postgres reaper) returns an
  entry to its own tenant's list. Entries carry the tenant (`"<tenant>:<id>"`);
  legacy bare-id entries are still drained.
- **Token bucket per tenant** replaces the per-minute sliding window
  (`RATE_LIMIT_RATE` tokens/s, `RATE_LIMIT_BURST`), computed in one Lua script on
  Redis's clock. It bounds bursts, which the per-minute window could not.

Tests added (all run against real Postgres/Redis with `CI=true`): round-robin
order across tenants, a 1-item tenant served within 2 claims behind a
1,000-item backlog, empty tenants leaving and re-joining the rotation, idle
claims waking on enqueue, crash recovery on the tenant path (claim, no ack,
restart `Drain`, re-claimed by another worker), legacy entries, reclaim to the
owning tenant, token-bucket burst/refill/isolation/expiry, and config
validation. All existing tests pass.

All v2 results are in `results/v2/` (summary: `results/v2/summary.md`). Host
free memory during the v2 runs: 30 readings, min 1,048 MB, max 2,374 MB.

### 8a. Tenant fairness (headline)

Same test as §4 test 7: quiet tenant 20/s, noisy tenant 200/s, 60 s, 1 worker
instance × 10 slots, 3 reps. "Limited" is the per-minute window (6,000/min) in
v1 and the token bucket (100/s, burst 100: the same 100/s average) in v2.

| Mode | Quiet p50 ms v1 → v2 | Quiet p95 ms v1 → v2 | **Quiet p99 ms v1 → v2** | Quiet max ms v1 → v2 |
|---|---|---|---|---|
| Quiet alone | 69.7 → 69.9 | 92.9 → 93.1 | 98 → 97.5 | 106 → 116 |
| + noisy, no limit | 18,044 → **87.8** | 33,467 → 118 | **34,609 → 171** | 34,902 → 485 |
| + noisy, limited | 7,706 → **78.7** | 16,798 → 112 | **17,691 → 181** | 17,909 → 364 |

v2 ranges (min–max over 3 reps): no limit p50 86.8–90.9, p99 151–197;
limited p50 75.1–85.6, p99 118–250. v1 ranges are in §4 test 7.

- With a noisy tenant at 10× its volume and **no rate limit**, the quiet
  tenant's p99 went from **34.6 s to 171 ms**. Its p50 is 87.8 ms vs 69.9 ms
  alone.
- The cost moved to the tenant that caused it: in the v2 "no limit" runs the
  noisy tenant's own latency was p50 18.2–20.8 s, p99 31.8–39.7 s
  (`v2/fairness/unlimited-r*.latency.txt`), because its 200/s exceeds the
  ~148/s capacity.
- With fair queueing in place, the token bucket did not lower the quiet
  tenant's latency further (p99 181 vs 171 ms, overlapping ranges); what it does
  is cap the noisy tenant's intake: 6,096 (6,087–6,097) of 12,001 requests
  accepted, the rest rejected with 429.
- Correctness: all 9 v2 fairness runs had 0 lost and 0 duplicate sends.

### 8b. Crash recovery on the new claim path

Same test as §4 test 5.

| Rep | Accepted | Delivered | Lost | In-flight at kill | Of those delivered | Down s | Recovery s from death | Recovery s from restart | Ids sent twice |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 18001 | 18001 | 0 | 9 | 9 | 7.90 | 8.41 | 0.51 | 1 |
| 2 | 17985 | 17985 | 0 | 8 | 8 | 7.25 | 7.80 | 0.55 | 0 |
| 3 | 18001 | 18001 | 0 | 4 | 4 | 7.15 | 7.68 | 0.54 | 0 |

- **0 lost** (53,987 accepted, 53,987 delivered). Every in-flight claim was
  returned by the restarted worker's `Drain` onto its tenant's list (log lines
  match the in-flight counts 9/8/4) and delivered 0.51–0.55 s after restart
  (v1: 0.42–0.69 s).
- 1 duplicate in 53,987, in the same at-least-once window as v1: its first send
  finished at 11:12:11.737, the process died at 11:12:11.763 (26 ms later)
  before `delivered` was written, and it was re-sent at 11:12:20.083. Not a
  regression.

### 8c. Worker scaling (regression check)

| Instances | Delivered/s v1 | Delivered/s v2 | v2 efficiency |
|---|---|---|---|
| 1 | 148 (147–149) | 148 (144–148) | 100% |
| 2 | 285 (285–287) | 280 (278–283) | 95% |
| 4 | 531 (525–532) | 530 (523–532) | 90% |
| 8 | 842 (839–852) | 868 (863–880) | 73% |

No scaling regression from the per-claim Lua script at 1, 4 or 8 instances.
At 2 instances v2 is 5/s (1.8%) lower with non-overlapping ranges; it is
reported as measured, and the difference does not persist at 4 or 8. All 12 v2
runs: 240,000 accepted, 240,000 delivered, 0 lost, 0 duplicate sends.

### 8d. Burst (regression check)

| Rep | Send duration s | Accept rate req/s | Peak ready depth | Drain s (first accepted → last delivered) | Drain after last accepted s | Lost |
|---|---|---|---|---|---|---|
| 1 | 58.8 | 851 | 27233 | 109.75 | 51.10 | 0 |
| 2 | 57.1 | 876 | 27465 | 108.18 | 51.46 | 0 |
| 3 | 69.3 | 721 | 25367 | 117.89 | 48.63 | 0 |

The delivery side is unchanged (drain after the last acceptance 48.63–51.46 s
vs 50.78–52.77 s in v1). The **accept rate was lower** than v1 (721–876 vs
888–932 req/s). 150,000 delivered, 0 lost, 0 duplicate sends.

### 8e. Ingestion (re-measured because the API path changed)

The enqueue is now a Lua script and the limiter is a token-bucket script, so
test 1 was re-run on v2 with identical steps and criteria.

| Offered req/s | v1 p99 ms | v2 p99 ms | v1 reps passing | v2 reps passing | v2 achieved req/s |
|---|---|---|---|---|---|
| 200 | 28.8 | 23.8 | 3/3 | 3/3 | 200 |
| 400 | 45.7 | 30.9 | 3/3 | 3/3 | 400 |
| 600 | 128 | 75.3 | 1/3 | 3/3 | 600 |
| 800 | 385 | 110 | 0/3 | 0/3 | 800 |
| 1000 | 508 | 576 | 0/3 | 0/3 | 973 |
| 1200 | 973 | 1934 | 0/3 | 0/3 | 996 |

- v2 max sustained: **600 req/s** (all 3 reps) at p50 19.9 ms, p95 44.2 ms,
  p99 75.3 ms (ranges 18.6–21.7, 34.6–47.6, 67.8–86.1). v1: 400 req/s.
- Under overload v2 pushes less through: 996 vs 1,105 req/s achieved at 1,200
  offered, consistent with the lower burst accept rate in 8d.
- These are two sets of runs hours apart on a laptop with varying free memory.
  The direction of each difference is measured; its cause (the new scripts vs
  host conditions) was not isolated.

### 8f. Idempotency (regression check, 1 rep)

1 row, 1,000 × 202, 1 distinct id, changed body → 409, 1 provider send.
Unchanged from v1.

### Guarantees after the change

| Guarantee | v2 evidence | Result |
|---|---|---|
| Zero lost | 28 verified v2 runs (fairness 9, crash 3, scaling 12, burst 3, idempotency 1) | 0 lost |
| Duplicates only in the crash window | ledger | 1 in the crash test, root-caused; 0 elsewhere |
| Crash recovery | in-flight claims at kill | 21 of 21 delivered after restart |
| Scaling | §8c | no regression except −1.8% at 2 instances |

## 9. Anomalies

- Ingestion latency near the knee is noisy: 600 req/s passed in 1 of 3 reps and
  800 had p99 from 187 to 1,426 ms across reps.
- The single-key duplicate test did not reproduce the enqueue race that the
  many-key test showed on the same build (§3b).
- Scaling efficiency drops to 71% (v1) / 73% (v2) at 8 instances; the
  limiting resource was not isolated.
- v2 burst accept rate and overload throughput are lower than v1, while v2
  sub-saturation ingestion latency is better (§8d, §8e); cause not isolated.

## 10. Limitations

- **One laptop.** Load generator, API, workers, Postgres and Redis share one
  i5-1240P and one WSL2 disk; commit latency on that disk is in every number.
  These are single-node numbers, not a production cluster.
- **Low and variable free host memory** (358–2,797 MB during the runs) because
  of other applications. It may add latency noise; it did not affect any
  correctness result.
- **Mock provider.** Delivery throughput is pipeline throughput at a fixed
  25–75 ms provider latency; it is not the throughput of a real email/SMS vendor.
- **Local network.** No real network latency between client, API and databases.
- **Short runs** (15–90 s per run, 3 reps). No soak test.
- Redis runs without AOF; the Postgres reaper is the recovery path for a Redis
  loss, and that path was not exercised here.
- Fairness is equal-share round-robin between tenants that have work; there are
  no per-tenant weights or priorities. It was measured with 2 active tenants
  (4 in the other tests).
- The per-tenant Lua scripts reference keys built inside the script, which is
  fine on a single Redis node but would need hash tags on Redis Cluster.
