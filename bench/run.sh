#!/usr/bin/env bash
# Reproducible benchmark suite for the notification system. Use the Makefile
# at the repo root (`make bench`, `make bench-ingest`, ...) or call directly:
#
#   bench/run.sh up          build images, start the isolated stack, create tenants
#   bench/run.sh env         record the test environment
#   bench/run.sh pilot       1 vs 4 tenants at a fixed rate (PILOT_TAG=before|latest)
#   bench/run.sh enqueue     1,000 concurrent same-key requests, workers stopped (ENQ_TAG=before|latest)
#   bench/run.sh <test>      ingest | scaling | e2e | failures | crash |
#                            idempotency | fairness | burst
#   bench/run.sh all         every test, in dependency order
#   bench/run.sh down        stop the stack and delete its volume
#
# Load comes from k6 (grafana/k6 image). Every delivery and correctness number
# comes from SQL run directly against Postgres (bench/sql/*.sql), and every
# raw output is written under bench/results/<test>/.
set -euo pipefail
cd "$(dirname "$0")"

# Git Bash on Windows rewrites arguments that look like POSIX paths
# (/scripts/rate.js) into Windows paths before docker ever sees them.
export MSYS_NO_PATHCONV=1

DC="docker compose"
# RUN_TAG puts every output under results/<tag>/ (e.g. RUN_TAG=v2), so a
# re-measurement never overwrites the committed baseline.
RT="${RUN_TAG:+$RUN_TAG/}"
REPS=${REPS:-3}
mkdir -p results state

log() { printf '\n\033[1m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }

psql_() { $DC exec -T postgres psql -X -U bench -d notifications "$@"; }
redis_() { $DC exec -T redis redis-cli "$@"; }

# ---------------------------------------------------------------- stack

worker_stop() { $DC stop worker >/dev/null 2>&1 || true; $DC rm -f worker >/dev/null 2>&1 || true; }

# worker_up INSTANCES CONCURRENCY — (re)create workers with the current SIM_* env
# and wait until every delivery loop has logged that it started.
worker_up() {
  local n=$1 c=$2
  export WORKER_CONCURRENCY=$c WORKER_PG_CONNS=$((c + 5))
  $DC up -d --no-deps --force-recreate --scale worker="$n" worker >/dev/null 2>&1
  local want=$((n * c)) got=0
  for _ in $(seq 1 240); do
    got=$($DC logs worker 2>/dev/null | grep -c '"msg":"worker started"' || true)
    [ "$got" -ge "$want" ] && return 0
    sleep 0.5
  done
  echo "workers did not start ($got/$want)" >&2
  return 1
}

# api_up RATE [BURST] — recreate the API with a per-tenant token bucket:
# RATE requests/s sustained, BURST at once (default: BURST = RATE).
api_up() {
  export BENCH_RATE_LIMIT_RATE=$1 BENCH_RATE_LIMIT_BURST=${2:-$1}
  $DC up -d --wait --no-deps --force-recreate api >/dev/null 2>&1
}

sim() { # sim LATENCY JITTER FAIL_RATE — the simulated provider for the next worker_up
  export SIM_ENABLED=true SIM_LATENCY=$1 SIM_JITTER=$2 SIM_FAIL_RATE=$3 \
         SIM_PERMANENT_RATE=0 SIM_TIMEOUT_RATE=0 SIM_OUTAGE_AFTER=0s SIM_OUTAGE_FOR=0s \
         WORKER_SMTP_HOST=
}

# reset — empty notifications, attempts, queues, rate-limit counters, the
# harness tables, and the provider ledger. Clients and API keys stay.
reset() {
  mem_check
  $DC --profile tools run --rm -T bench reset >/dev/null 2>&1
  psql_ -q -c 'TRUNCATE bench_accepted, bench_sends' >/dev/null
  redis_ DEL sim:sends >/dev/null
}

# ---------------------------------------------------------------- host memory

# nocr strips the carriage returns that Windows-side tools and redis-cli
# through `docker exec` can emit, so values compare and count correctly.
nocr() { tr -d '\r'; }

# queue_ids prints the notification id of every ready entry, one per line:
# every tenant's list plus the legacy single queue. Entries are
# "<tenant>:<id>" (or a bare legacy id); only the id is printed.
queue_ids() {
  $DC exec -T redis sh -c 'for k in notifications:queue $(redis-cli --raw KEYS "notifications:q:*"); do redis-cli --raw LRANGE "$k" 0 -1; done'     | nocr | sed '/^$/d; s/^.*://'
}

# ready_count is the number of ready entries across all tenant lists.
ready_count() { queue_ids | wc -l | tr -d ' '; }

free_mb() {
  powershell.exe -NoProfile -Command "[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1024)" | nocr
}

# mem_mark LABEL — append host free memory to results/memory.log.
mem_mark() { echo "$(date +%FT%T) free_mb=$(free_mb) ${RUN_TAG:+[$RUN_TAG] }$1" | tee -a results/memory.log >&2; }

# mem_check — stop the suite if host free memory is below MEM_FLOOR_MB,
# printing the largest processes so the cause is visible.
mem_check() {
  local free; free=$(free_mb)
  if [ "$free" -lt "${MEM_FLOOR_MB:-1024}" ]; then
    echo "PAUSED: host free memory ${free} MB < ${MEM_FLOOR_MB:-1024} MB" | tee -a results/memory.log >&2
    powershell.exe -NoProfile -Command "Get-Process | Group-Object Name | ForEach-Object { [pscustomobject]@{Name=\$_.Name; MB=[math]::Round((\$_.Group | Measure-Object WorkingSet64 -Sum).Sum/1MB)} } | Sort-Object MB -Descending | Select-Object -First 10 | Format-Table -AutoSize | Out-String" | nocr | tee -a results/memory.log >&2
    exit 3
  fi
}

# ---------------------------------------------------------------- measure

# k6 DIR LABEL SCRIPT [-e K=V ...] — run a k6 script. Writes the console
# summary, the JSON summary, and the list of accepted ids.
k6() {
  local dir=$1 label=$2 script=$3; shift 3
  mkdir -p "results/${RT}$dir"
  $DC --profile tools run --rm -T -e RUN="$label-$(date +%s)" "$@" k6 run --quiet \
    --summary-trend-stats 'avg,min,med,p(90),p(95),p(99),max' \
    --summary-export "/results/${RT}$dir/$label.k6.json" \
    --log-output "file=/results/${RT}$dir/$label.ids" --log-format raw \
    "/scripts/$script" 2>&1 | tee "results/${RT}$dir/$label.k6.txt" >&2
}

# load_accepted DIR LABEL — copy accepted ids from the k6 log into Postgres.
load_accepted() {
  grep '^A ' "results/${RT}$1/$2.ids" | cut -d' ' -f2 | tr -d '\r' \
    | psql_ -q -c '\copy bench_accepted(id) FROM STDIN'
}

# load_ledger — copy the provider's send ledger (Redis hash) into Postgres.
load_ledger() {
  redis_ --raw HGETALL sim:sends | tr -d '\r' | paste - - \
    | psql_ -q -c '\copy bench_sends(id, sends) FROM STDIN'
}

# wait_terminal [TIMEOUT_S] — until every row is delivered or dead-lettered.
wait_terminal() {
  local timeout=${1:-900} left
  for _ in $(seq 1 $((timeout / 2))); do
    left=$(psql_ -Atc "SELECT count(*) FROM notifications WHERE status NOT IN ('delivered','dead_lettered')")
    [ "$left" = "0" ] && return 0
    sleep 2
  done
  echo "timed out with $left notifications not terminal" >&2
  return 1
}

# sql DIR LABEL NAME — run bench/sql/NAME.sql, saving query and output together.
sql() {
  { echo "-- query: bench/sql/$3.sql"; psql_ < "sql/$3.sql"; } | tee "results/${RT}$1/$2.$3.txt" >&2
}

sampler_start() { $DC --profile tools run -d --name notibench-sampler sampler >/dev/null; }
sampler_stop() { # sampler_stop DIR LABEL
  docker logs notibench-sampler > "results/${RT}$1/$2.queue.tsv" 2>/dev/null || true
  docker rm -f notibench-sampler >/dev/null 2>&1 || true
}

# finish DIR LABEL — the standard post-run evidence: accepted ids and ledger
# into Postgres, then correctness, latency and throughput queries.
finish() {
  load_accepted "$1" "$2"
  load_ledger
  sql "$1" "$2" verify
  sql "$1" "$2" latency
  sql "$1" "$2" throughput
}

# ---------------------------------------------------------------- tests

# 1. Ingestion: open-loop fixed rate per step, workers stopped so only the
#    POST path is measured. The step list is set from a pilot run.
t_ingest() {
  local steps=${INGEST_STEPS:-"500 1000 1500 2000 2500 3000"} dur=${INGEST_DURATION:-30s}
  worker_stop; api_up 100000000
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    for rate in $steps; do
      reset
      log "ingest: $rate req/s for $dur, rep $r"
      k6 ingest "rate${rate}-r$r" rate.js -e RATE="$rate" -e DURATION="$dur" -e WARMUP="${INGEST_WARMUP:-10s}"
    done
  done
}

# Pilot: does ingestion throughput depend on the number of API keys? Same
# offered load with 1 tenant and with 4, workers stopped. PILOT_TAG selects the
# API image (before = the pre-fix build at git HEAD, latest = the fixed build).
t_pilot() {
  local tag=${PILOT_TAG:-latest} rate=${PILOT_RATE:-1000} dur=${PILOT_DURATION:-15s}
  export API_TAG=$tag
  worker_stop; api_up 100000000
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    for t in 1 4; do
      reset
      log "pilot[$tag]: $rate req/s offered, $t tenant(s), rep $r"
      k6 "${OUT_PREFIX:-}pilot-$tag" "t${t}-r$r" rate.js -e RATE="$rate" -e DURATION="$dur" -e TENANTS="$t"
    done
  done
  export API_TAG=latest; api_up 100000000
}

# 6b. Exactly-one-enqueue: DUPS concurrent identical requests with one key and
#     the workers STOPPED, so nothing drains the queue and a duplicate push
#     stays visible. ENQ_TAG selects the API build (before | latest).
t_enqueue() {
  local tag=${ENQ_TAG:-latest} dups=${IDEM_DUPS:-1000} dir="${OUT_PREFIX:-}enqueue-${ENQ_TAG:-latest}"
  export API_TAG=$tag
  worker_stop; api_up 100000000
  [ "${ENQ_SINGLE:-1}" = 1 ] && for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    reset
    local label="dups${dups}-r$r" release=$(( ($(date +%s) + 20) * 1000 ))
    log "enqueue[$tag]: $dups concurrent duplicates, workers stopped, rep $r"
    k6 "$dir" "$label" idempotency.js -e DUPS="$dups" -e RELEASE_AT="$release"
    load_accepted "$dir" "$label"
    {
      echo "-- redis, workers stopped: every push of the id is still on the ready list"
      echo "LLEN notifications:queue = $(ready_count)"
      echo "ZCARD notifications:scheduled = $(redis_ ZCARD notifications:scheduled | nocr)"
      echo "distinct ids on the queue = $(queue_ids | sort -u | wc -l)"
    } | tee "results/${RT}$dir/$label.queue.txt" >&2
    sql "$dir" "$label" idempotency
  done
  # Many keys at once: KEYS keys x COPIES concurrent copies each.
  local keys=${ENQ_KEYS:-5000} copies=${ENQ_COPIES:-5}
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    reset
    local label="keys${keys}x${copies}-r$r"
    log "enqueue[$tag]: $keys keys x $copies concurrent copies, workers stopped, rep $r"
    k6 "$dir" "$label" dupkeys.js -e N="$keys" -e COPIES="$copies"
    load_accepted "$dir" "$label"
    {
      echo "-- query: rows vs queue entries, workers stopped"
      echo "notification rows = $(psql_ -Atc 'SELECT count(*) FROM notifications')"
      echo "distinct ids in 202 responses = $(psql_ -Atc 'SELECT count(DISTINCT id) FROM bench_accepted')"
      echo "LLEN notifications:queue (every push) = $(ready_count)"
      echo "distinct ids on the queue = $(queue_ids | sort -u | wc -l)"
      echo "ids pushed more than once = $(queue_ids | sort | uniq -d | wc -l)"
    } | tee "results/${RT}$dir/$label.queue.txt" >&2
  done
  export API_TAG=latest; api_up 100000000
}

# 3. Worker scaling: drain a preloaded backlog with 1, 2, 4, 8 instances.
t_scaling() {
  local n=${SCALING_N:-20000} c=${SCALING_CONC:-10}
  api_up 100000000; sim 50ms 25ms 0
  for inst in ${SCALING_INSTANCES:-1 2 4 8}; do
    for r in $(seq "${REP_FROM:-1}" "$REPS"); do
      worker_stop; reset
      log "scaling: preload $n, $inst instance(s) x $c, rep $r"
      k6 scaling "i${inst}-r$r" burst.js -e N="$n" -e VUS=100
      worker_up "$inst" "$c"
      wait_terminal
      finish scaling "i${inst}-r$r"
    done
  done
  worker_stop
}

# 2. End-to-end latency at a moderate and a near-peak rate.
t_e2e() {
  local inst=${E2E_INSTANCES:-4} c=${E2E_CONC:-10} dur=${E2E_DURATION:-60s}
  api_up 100000000; sim 50ms 25ms 0
  for rate in ${E2E_RATES:?set E2E_RATES from the scaling result}; do
    for r in $(seq "${REP_FROM:-1}" "$REPS"); do
      worker_stop; reset; worker_up "$inst" "$c"
      log "e2e: $rate/s for $dur, $inst x $c, rep $r"
      k6 e2e "rate${rate}-r$r" rate.js -e RATE="$rate" -e DURATION="$dur"
      wait_terminal
      finish e2e "rate${rate}-r$r"
    done
  done
  worker_stop
}

# 4. Provider failures: transient failure rate 10/30/50%, max_attempts 5.
t_failures() {
  local inst=${FAIL_INSTANCES:-4} c=${FAIL_CONC:-10} rate=${FAIL_RATE_RPS:-200} dur=${FAIL_DURATION:-25s}
  api_up 100000000
  for fr in ${FAIL_RATES:-0.10 0.30 0.50}; do
    for r in $(seq "${REP_FROM:-1}" "$REPS"); do
      sim 50ms 25ms "$fr"; worker_stop; reset; worker_up "$inst" "$c"
      log "failures: fail rate $fr, $rate/s for $dur, rep $r"
      k6 failures "fail${fr}-r$r" rate.js -e RATE="$rate" -e DURATION="$dur"
      wait_terminal
      finish failures "fail${fr}-r$r"
      sql failures "fail${fr}-r$r" failures
    done
  done
  worker_stop
}

# 5. Crash recovery: kill -9 one of two workers mid-load, restart it.
t_crash() {
  local c=${CRASH_CONC:-10} rate=${CRASH_RPS:-200} dur=${CRASH_DURATION:-90s}
  local kill_after=${CRASH_KILL_AFTER:-30} down_for=${CRASH_DOWN_FOR:-5}
  api_up 100000000; sim 50ms 25ms 0
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    local label="kill-r$r" dir=${CRASH_DIR:-crash}
    worker_stop; reset; worker_up 2 "$c"
    mkdir -p "results/${RT}$dir"
    log "crash: $rate/s for $dur, SIGKILL worker-1 at ${kill_after}s, restart after ${down_for}s, rep $r"
    ( k6 "$dir" "$label" rate.js -e RATE="$rate" -e DURATION="$dur" ) &
    local loader=$!
    sleep "$kill_after"
    local host; host=$(docker inspect -f '{{.Config.Hostname}}' notibench-worker-1)
    local t_kill; t_kill=$(psql_ -Atc 'SELECT clock_timestamp()')
    docker kill -s KILL notibench-worker-1 >/dev/null
    local killed_at=$SECONDS
    # What the dead process still held: every slot's processing list, read in
    # ONE exec. A docker exec per slot took ~3 s each here, which delayed the
    # restart by ~30 s and made the harness, not the system, set recovery time.
    local inflight="results/${RT}$dir/$label.inflight"
    $DC exec -T redis sh -c "for k in \$(redis-cli --raw KEYS 'notifications:processing:$host-*'); do redis-cli --raw LRANGE \"\$k\" 0 -1; done" \
      | nocr | sed '/^$/d; s/^.*://' > "$inflight"
    log "killed $host at $t_kill with $(wc -l < "$inflight") claims in flight"
    # Restart down_for seconds after the kill, not after the capture.
    local left=$((down_for - (SECONDS - killed_at)))
    [ "$left" -gt 0 ] && sleep "$left"
    docker start notibench-worker-1 >/dev/null
    # Exact times from Docker itself (same VM clock as Postgres). The psql
    # timestamp above is taken BEFORE `docker kill`, which takes 1-3 s here,
    # so it is only a lower bound on the kill time.
    local t_dead t_up
    t_dead=$(docker inspect -f '{{.State.FinishedAt}}' notibench-worker-1 2>/dev/null)
    t_up=$(docker inspect -f '{{.State.StartedAt}}' notibench-worker-1)
    # FinishedAt is overwritten once the restarted process exits, so it is read
    # right after the restart; StartedAt now refers to the new process.
    local t_restart=$t_up
    wait "$loader"
    wait_terminal
    finish "$dir" "$label"
    {
      echo "-- worker killed: notibench-worker-1 (worker id prefix $host)"
      echo "-- psql time just before docker kill (lower bound): $t_kill"
      echo "-- process died (docker State.FinishedAt): $t_dead"
      echo "-- process restarted (docker State.StartedAt): $t_up"
      t_kill=$t_dead
      echo "-- in-flight claims at kill: $(wc -l < "$inflight")"
      echo "-- query: fate of every in-flight id, and recovery time (kill -> last of them delivered)"
      psql_ -q -c 'CREATE TEMP TABLE inflight(id uuid)' -c '\copy inflight FROM STDIN' -c "
        SELECT count(*) AS inflight_ids,
               count(*) FILTER (WHERE n.status = 'delivered') AS delivered,
               count(*) FILTER (WHERE n.status IS DISTINCT FROM 'delivered') AS not_delivered,
               round(extract(epoch FROM max(n.updated_at) - '$t_kill'::timestamptz)::numeric, 2) AS recovery_s_from_kill,
               round(extract(epoch FROM max(n.updated_at) - '$t_restart'::timestamptz)::numeric, 2) AS recovery_s_from_restart
        FROM inflight i LEFT JOIN notifications n USING (id);" < "$inflight"
      echo "-- worker-1 recovery log lines"
      docker logs notibench-worker-1 2>&1 | grep -E 'recovered claims|reclaimed claims|recovered stranded' || echo "(none)"
    } | tee "results/${RT}$dir/$label.recovery.txt" >&2
  done
  worker_stop
}

# 6. Idempotency: DUPS concurrent identical requests with one key.
t_idempotency() {
  local dups=${IDEM_DUPS:-1000}
  api_up 100000000; sim 50ms 25ms 0
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    worker_stop; reset; worker_up 2 10
    local release=$(( ($(date +%s) + 20) * 1000 ))
    log "idempotency: $dups concurrent duplicates, rep $r"
    k6 idempotency "dups${dups}-r$r" idempotency.js -e DUPS="$dups" -e RELEASE_AT="$release"
    wait_terminal
    load_accepted idempotency "dups${dups}-r$r"
    load_ledger
    sql idempotency "dups${dups}-r$r" idempotency
    sql idempotency "dups${dups}-r$r" verify
  done
  worker_stop
}

# 7. Tenant fairness: quiet tenant alone, then with a 10x noisy tenant with
#    the rate limit lifted, then with a per-tenant limit below the noisy rate.
t_fairness() {
  local c=${FAIR_CONC:-10} quiet=${FAIR_QUIET:-20} noisy=${FAIR_NOISY:-200} dur=${FAIR_DURATION:-60s}
  # Token bucket for the "limited" mode: the same 100/s average as bench-v1's
  # 6,000/min window, but with the burst bounded to 1 s of traffic.
  local limit=${FAIR_LIMIT_RATE:-100} burst=${FAIR_LIMIT_BURST:-100}
  sim 50ms 25ms 0
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    for mode in baseline unlimited limited; do
      local n=$noisy lim=100000000 b=100000000
      [ "$mode" = baseline ] && n=0
      [ "$mode" = limited ] && lim=$limit && b=$burst
      api_up "$lim" "$b"; worker_stop; reset; worker_up 1 "$c"
      log "fairness $mode: quiet $quiet/s, noisy $n/s, token bucket $lim/s burst $b, rep $r"
      k6 fairness "$mode-r$r" fairness.js -e QUIET_RATE="$quiet" -e NOISY_RATE="$n" -e DURATION="$dur"
      wait_terminal
      finish fairness "$mode-r$r"
    done
  done
  worker_stop; api_up 100000000
}

# 8. Burst: N notifications as fast as the API accepts them, workers running.
t_burst() {
  local n=${BURST_N:-50000} inst=${BURST_INSTANCES:-4} c=${BURST_CONC:-10}
  api_up 100000000; sim 50ms 25ms 0
  for r in $(seq "${REP_FROM:-1}" "$REPS"); do
    worker_stop; reset; worker_up "$inst" "$c"
    log "burst: $n as fast as possible, $inst x $c, rep $r"
    sampler_start
    k6 burst "n${n}-r$r" burst.js -e N="$n" -e VUS=200
    wait_terminal
    sleep 1
    sampler_stop burst "n${n}-r$r"
    finish burst "n${n}-r$r"
  done
  worker_stop
}

env_info() {
  {
    echo "captured: $(date -u +%FT%TZ)"
    echo "== host =="
    powershell.exe -NoProfile -Command "Get-CimInstance Win32_Processor | ForEach-Object { 'cpu: ' + \$_.Name + ' | cores ' + \$_.NumberOfCores + ' | threads ' + \$_.NumberOfLogicalProcessors }; 'ram_bytes: ' + (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory; \$o = Get-CimInstance Win32_OperatingSystem; 'os: ' + \$o.Caption + ' ' + \$o.Version" | tr -d '\r'
    echo "== docker =="
    docker version --format 'docker server {{.Server.Version}}'
    docker info --format 'docker vm: {{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}, kernel {{.KernelVersion}}'
    echo "== versions =="
    echo "go (build image): $(grep -m1 '^FROM golang' ../api/Dockerfile)"
    echo "go (module): $(grep -m1 '^go ' ../api/go.mod)"
    $DC exec -T postgres postgres --version
    $DC exec -T redis redis-server --version
    $DC --profile tools run --rm -T k6 version 2>/dev/null | head -1
    echo "== container limits (docker compose config) =="
    $DC --profile tools config --format json | python -c "import json,sys; d=json.load(sys.stdin)['services']; [print(f'  {k}: cpus={v.get(\"deploy\",{}).get(\"resources\",{}).get(\"limits\",{}).get(\"cpus\")} mem={v.get(\"deploy\",{}).get(\"resources\",{}).get(\"limits\",{}).get(\"memory\")}') for k,v in sorted(d.items())]"
    echo "== postgres settings =="
    psql_ -Atc "SELECT name||'='||current_setting(name) FROM pg_settings WHERE name IN ('max_connections','shared_buffers','synchronous_commit','fsync','wal_level','work_mem','max_wal_size') ORDER BY 1"
    echo "== redis settings =="
    for k in appendonly save maxmemory; do redis_ CONFIG GET "$k" | tr -d '\r' | paste - -; done
    echo "== app settings (bench/docker-compose.yml) =="
    echo "  api: POSTGRES_MAX_CONNS=40, rate limit lifted (token bucket 100000000/s) except in the fairness test"
    echo "  worker: WORKER_CONCURRENCY per test (default 10 slots/instance), POSTGRES_MAX_CONNS=slots+5, CLAIM_TIMEOUT=1s"
    echo "  worker: RETRY_BASE=1s RETRY_MAX=5m, max_attempts=5 (column default), LIVENESS_TTL=30s, RECLAIM_EVERY=30s"
    echo "  provider: simulated, latency 50ms +/- 25ms uniform unless a test says otherwise, ledger on"
  } | tee results/environment.txt
}

case "${1:-}" in
  pilot|enqueue|ingest|scaling|e2e|failures|crash|idempotency|fairness|burst)
    mem_mark "start $1 ${REP_FROM:-1}..$REPS"
    trap 'mem_mark "end $1 ${REP_FROM:-1}..$REPS (exit $?)"' EXIT ;;
esac

case "${1:-}" in
  up)
    $DC build && $DC --profile tools build bench && $DC --profile tools pull k6
    $DC up -d --wait postgres redis api
    psql_ -q < sql/harness.sql
    [ -s state/keys.json ] || $DC --profile tools run --rm -T bench setup -tenants 4 -out /state/keys.json ;;
  down) $DC --profile tools down -v; rm -f state/keys.json ;;
  env) env_info ;;
  pilot) t_pilot ;;
  enqueue) t_enqueue ;;
  ingest) t_ingest ;;
  scaling) t_scaling ;;
  e2e) t_e2e ;;
  failures) t_failures ;;
  crash) t_crash ;;
  idempotency) t_idempotency ;;
  fairness) t_fairness ;;
  burst) t_burst ;;
  all) env_info; t_pilot; t_enqueue; t_ingest; t_scaling; t_e2e; t_failures; t_crash; t_idempotency; t_fairness; t_burst ;;
  *) sed -n 2,14p "$0"; exit 2 ;;
esac
