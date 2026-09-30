# Benchmarks

A reproducible load test of the real system (API, worker, Postgres, Redis) in an
isolated Docker Compose project (`notibench`), driven by k6. Results and their
methodology are in [`results/RESULTS.md`](results/RESULTS.md).

```bash
make bench            # everything: up, env, every test, report  (from the repo root)
make bench-<test>     # pilot enqueue ingest scaling e2e failures crash idempotency fairness burst
make bench-report     # rebuild results/summary.md from the raw outputs
make bench-down       # delete the stack and its volume
```

| Path | What |
|---|---|
| `run.sh` | Stack lifecycle, reset, one function per test. Parameters are env vars (`REPS`, `REP_FROM`, `INGEST_STEPS`, `E2E_RATES`, ...) |
| `k6/` | Load scripts. Every 202's id is logged so each accepted notification can be traced in Postgres |
| `sql/` | Every correctness and delivery measurement: `verify`, `latency`, `throughput`, `failures`, `idempotency` |
| `summarize.py` | Builds `results/summary.md` (median and min–max over reps) from saved files only |
| `docker-compose.yml` | Isolated stack with CPU/memory limits, the simulated provider, k6, and a queue-depth sampler |

Nothing reaches a real provider: `PROVIDER_SIM_ENABLED` is forced on for workers,
and its send ledger (`PROVIDER_SIM_LEDGER`) counts every simulated send so
duplicate deliveries are measurable.
