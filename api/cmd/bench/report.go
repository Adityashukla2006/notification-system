package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ---- queue depth sampling ----

type sample struct {
	At        time.Time `json:"at"`
	Ready     int64     `json:"ready"`
	Scheduled int64     `json:"scheduled"`
	InFlight  int64     `json:"in_flight"`
}

// queueSampler polls the real Redis structures: the ready list, the retry and
// schedule sorted set, and every worker's processing list.
type queueSampler struct {
	mu      sync.Mutex
	samples []sample
	cancel  context.CancelFunc
	done    chan struct{}
}

func startSampler(ctx context.Context, e *env, every time.Duration) *queueSampler {
	ctx, cancel := context.WithCancel(ctx)
	s := &queueSampler{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			if smp, err := takeSample(ctx, e); err == nil {
				s.mu.Lock()
				s.samples = append(s.samples, smp)
				s.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return s
}

func takeSample(ctx context.Context, e *env) (sample, error) {
	smp := sample{At: time.Now()}
	pipe := e.rdb.Pipeline()
	ready := pipe.LLen(ctx, "notifications:queue")
	sched := pipe.ZCard(ctx, "notifications:scheduled")
	if _, err := pipe.Exec(ctx); err != nil {
		return smp, err
	}
	smp.Ready, smp.Scheduled = ready.Val(), sched.Val()

	keys, err := e.rdb.Keys(ctx, "notifications:processing:*").Result()
	if err != nil {
		return smp, err
	}
	if len(keys) > 0 {
		pipe = e.rdb.Pipeline()
		lens := make([]interface{ Val() int64 }, len(keys))
		for i, k := range keys {
			lens[i] = pipe.LLen(ctx, k)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return smp, err
		}
		for _, l := range lens {
			smp.InFlight += l.Val()
		}
	}
	return smp, nil
}

func (s *queueSampler) stop() []sample {
	s.cancel()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.samples
}

// ---- delivery report, computed from Postgres ----

type deliveryReport struct {
	Completed              bool           `json:"all_terminal"`
	WaitedS                float64        `json:"waited_s"`
	Notifications          int64          `json:"notifications"`
	StatusCounts           map[string]int `json:"status_counts"`
	Attempts               int64          `json:"attempts_total"`
	AttemptsOK             int64          `json:"attempts_succeeded"`
	AttemptsFailed         int64          `json:"attempts_failed"`
	Retries                int64          `json:"retries"`
	RetriedNotifs          int64          `json:"notifications_retried"`
	DuplicateSends         int64          `json:"notifications_delivered_more_than_once"`
	DLQPermanent           int64          `json:"dead_lettered_permanent"`
	DLQExhausted           int64          `json:"dead_lettered_retries_exhausted"`
	ErrorBreakdown         map[string]int `json:"failed_attempt_errors"`
	SuccessRate            float64        `json:"delivery_success_rate_pct"`
	FirstTryRate           float64        `json:"first_attempt_success_rate_pct"`
	E2E                    percentiles    `json:"e2e_latency_ms_delivered"`
	E2EFirstTry            percentiles    `json:"e2e_latency_ms_first_attempt_successes"`
	QueueWait              percentiles    `json:"queue_wait_ms_first_attempt"`
	ProviderCall           percentiles    `json:"provider_call_ms_all_attempts"`
	FirstCreated           time.Time      `json:"first_created"`
	LastCreated            time.Time      `json:"last_created"`
	FirstAttempt           time.Time      `json:"first_attempt_start"`
	LastTerminal           time.Time      `json:"last_terminal"`
	DrainFromFirstAttemptS float64        `json:"drain_s_from_first_attempt"`
	DrainFromFirstCreateS  float64        `json:"drain_s_from_first_created"`
	DrainAfterLastCreateS  float64        `json:"drain_s_after_last_created"`
	DeliveredPerSec        float64        `json:"delivered_per_sec_avg"`
	PeakDelivered1s        int64          `json:"delivered_peak_1s"`
	PerSecond              []secondCount  `json:"delivered_per_second"`
	PeakReady              int64          `json:"peak_ready_queue_depth"`
	PeakScheduled          int64          `json:"peak_scheduled_depth"`
	PeakInFlight           int64          `json:"peak_in_flight"`
	PeakBacklog            int64          `json:"peak_backlog_ready_plus_scheduled"`
	Samples                int            `json:"queue_samples"`
	Outage                 *outageReport  `json:"outage,omitempty"`
	Tenants                []tenantReport `json:"tenants"`
	QueueSeries            []sample       `json:"queue_series"`
}

type secondCount struct {
	Second time.Time `json:"t"`
	Count  int64     `json:"n"`
}

type outageReport struct {
	FirstOutageFailure    time.Time `json:"first_outage_failure"`
	LastOutageFailure     time.Time `json:"last_outage_failure"`
	OutageFailures        int64     `json:"outage_failed_attempts"`
	FirstSuccessAfterS    float64   `json:"first_success_after_outage_s"`
	PreOutagePeakBacklog  int64     `json:"pre_outage_peak_backlog"`
	PeakBacklogDuring     int64     `json:"peak_backlog_during_or_after"`
	BacklogRecoveredS     float64   `json:"backlog_recovered_s_after_outage"`
	BacklogRecovered      bool      `json:"backlog_recovered"`
	SuccessesDuringOutage int64     `json:"successes_during_outage_window"`
}

type tenantReport struct {
	Tenant    string      `json:"tenant"`
	Count     int64       `json:"notifications"`
	Delivered int64       `json:"delivered"`
	E2E       percentiles `json:"e2e_latency_ms"`
	QueueWait percentiles `json:"queue_wait_ms"`
	LastDone  time.Time   `json:"last_terminal"`
	PerSec    float64     `json:"delivered_per_sec_over_its_active_window"`
}

// waitAndReport waits until every notification is terminal, then builds the
// report from the database and the queue samples.
func waitAndReport(ctx context.Context, e *env, sampler *queueSampler, tenants []tenant, started time.Time, timeout time.Duration) (*deliveryReport, error) {
	deadline := time.Now().Add(timeout)
	completed := false
	lastPrint := time.Now()
	for time.Now().Before(deadline) {
		var total, open int64
		err := e.pg.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE status NOT IN ('delivered','dead_lettered','failed'))
FROM notifications`).Scan(&total, &open)
		if err != nil {
			return nil, fmt.Errorf("polling progress: %w", err)
		}
		if total > 0 && open == 0 {
			completed = true
			break
		}
		if time.Since(lastPrint) > 5*time.Second {
			fmt.Printf("  waiting: %d/%d not yet terminal\n", open, total)
			lastPrint = time.Now()
		}
		time.Sleep(250 * time.Millisecond)
	}
	samples := sampler.stop()
	rep, err := buildReport(ctx, e, samples, tenants)
	if err != nil {
		return nil, err
	}
	rep.Completed = completed
	rep.WaitedS = time.Since(started).Seconds()
	printDelivery(rep)
	return rep, nil
}

// pctSQL returns observed (percentile_disc) p50/p95/p99, max, avg, and n for
// a millisecond expression.
func pctSQL(expr string) string {
	return fmt.Sprintf(`count(%[1]s),
coalesce(percentile_disc(0.50) WITHIN GROUP (ORDER BY %[1]s), 0),
coalesce(percentile_disc(0.95) WITHIN GROUP (ORDER BY %[1]s), 0),
coalesce(percentile_disc(0.99) WITHIN GROUP (ORDER BY %[1]s), 0),
coalesce(max(%[1]s), 0), coalesce(avg(%[1]s), 0)`, expr)
}

func scanPct(dst *percentiles) []any {
	return []any{&dst.N, &dst.P50, &dst.P95, &dst.P99, &dst.Max, &dst.Avg}
}

// firstAndSuccess is a CTE giving each notification its first attempt start
// and first successful attempt finish.
const firstAndSuccess = `
WITH fa AS (
  SELECT notification_id,
         min(started_at) AS first_start,
         min(finished_at) FILTER (WHERE outcome = 'succeeded') AS ok_finish,
         count(*) AS n_attempts,
         count(*) FILTER (WHERE outcome = 'succeeded') AS n_ok
  FROM delivery_attempts GROUP BY notification_id
)`

func epochMS(expr string) string {
	return fmt.Sprintf("(extract(epoch FROM (%s)) * 1000)::float8", expr)
}

func buildReport(ctx context.Context, e *env, samples []sample, tenants []tenant) (*deliveryReport, error) {
	r := &deliveryReport{StatusCounts: map[string]int{}, ErrorBreakdown: map[string]int{}, Samples: len(samples)}
	q := func(sql string, args ...any) error {
		if err := e.pg.QueryRow(ctx, sql).Scan(args...); err != nil {
			return fmt.Errorf("report query failed: %w\n%s", err, sql)
		}
		return nil
	}

	rows, err := e.pg.Query(ctx, `SELECT status, count(*) FROM notifications GROUP BY status`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		r.StatusCounts[s] = n
		r.Notifications += int64(n)
	}
	rows.Close()

	if err := q(firstAndSuccess+`
SELECT coalesce(sum(n_attempts),0), coalesce(sum(n_ok),0),
       coalesce(sum(n_attempts),0) - count(*),
       count(*) FILTER (WHERE n_attempts > 1),
       count(*) FILTER (WHERE n_ok > 1)
FROM fa`, &r.Attempts, &r.AttemptsOK, &r.Retries, &r.RetriedNotifs, &r.DuplicateSends); err != nil {
		return nil, err
	}
	r.AttemptsFailed = r.Attempts - r.AttemptsOK

	if err := q(`
SELECT count(*) FILTER (WHERE last_err LIKE 'permanent:%'),
       count(*) FILTER (WHERE last_err IS NULL OR last_err NOT LIKE 'permanent:%')
FROM (
  SELECT n.id, (SELECT a.error FROM delivery_attempts a WHERE a.notification_id = n.id
                ORDER BY a.started_at DESC LIMIT 1) AS last_err
  FROM notifications n WHERE n.status = 'dead_lettered') d`, &r.DLQPermanent, &r.DLQExhausted); err != nil {
		return nil, err
	}

	rows, err = e.pg.Query(ctx, `
SELECT CASE WHEN error LIKE 'permanent:%' THEN 'permanent'
            WHEN error LIKE 'simulated timeout%' OR error LIKE '%deadline exceeded%' THEN 'timeout'
            WHEN error = 'simulated outage' THEN 'outage'
            WHEN error = 'simulated transient failure' THEN 'transient'
            ELSE left(error, 60) END, count(*)
FROM delivery_attempts WHERE outcome = 'failed' GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		r.ErrorBreakdown[k] = n
	}
	rows.Close()

	if r.Notifications > 0 {
		r.SuccessRate = 100 * float64(r.StatusCounts["delivered"]) / float64(r.Notifications)
	}
	var firstTry int64
	if err := q(firstAndSuccess+`SELECT count(*) FROM fa WHERE n_attempts = 1 AND n_ok = 1`, &firstTry); err != nil {
		return nil, err
	}
	if r.Notifications > 0 {
		r.FirstTryRate = 100 * float64(firstTry) / float64(r.Notifications)
	}

	if err := q(firstAndSuccess+`SELECT `+pctSQL(epochMS("fa.ok_finish - n.created_at"))+`
FROM notifications n JOIN fa ON fa.notification_id = n.id WHERE n.status = 'delivered'`, scanPct(&r.E2E)...); err != nil {
		return nil, err
	}
	if err := q(firstAndSuccess+`SELECT `+pctSQL(epochMS("fa.ok_finish - n.created_at"))+`
FROM notifications n JOIN fa ON fa.notification_id = n.id WHERE n.status = 'delivered' AND fa.n_attempts = 1`, scanPct(&r.E2EFirstTry)...); err != nil {
		return nil, err
	}
	if err := q(firstAndSuccess+`SELECT `+pctSQL(epochMS("fa.first_start - n.created_at"))+`
FROM notifications n JOIN fa ON fa.notification_id = n.id`, scanPct(&r.QueueWait)...); err != nil {
		return nil, err
	}
	if err := q(`SELECT `+pctSQL(epochMS("finished_at - started_at"))+` FROM delivery_attempts`, scanPct(&r.ProviderCall)...); err != nil {
		return nil, err
	}

	if r.Notifications > 0 {
		if err := q(`
SELECT min(created_at), max(created_at),
       (SELECT coalesce(min(started_at), 'epoch') FROM delivery_attempts),
       coalesce(max(updated_at) FILTER (WHERE status IN ('delivered','dead_lettered','failed')), 'epoch')
FROM notifications`, &r.FirstCreated, &r.LastCreated, &r.FirstAttempt, &r.LastTerminal); err != nil {
			return nil, err
		}
		r.DrainFromFirstAttemptS = r.LastTerminal.Sub(r.FirstAttempt).Seconds()
		r.DrainFromFirstCreateS = r.LastTerminal.Sub(r.FirstCreated).Seconds()
		r.DrainAfterLastCreateS = r.LastTerminal.Sub(r.LastCreated).Seconds()
	}

	rows, err = e.pg.Query(ctx, `
SELECT date_trunc('second', finished_at) AS s, count(*)
FROM delivery_attempts WHERE outcome = 'succeeded' GROUP BY 1 ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var sc secondCount
		if err := rows.Scan(&sc.Second, &sc.Count); err != nil {
			return nil, err
		}
		r.PerSecond = append(r.PerSecond, sc)
		r.PeakDelivered1s = max(r.PeakDelivered1s, sc.Count)
	}
	rows.Close()

	var lastOK time.Time
	if r.AttemptsOK > 0 {
		if err := q(`SELECT max(finished_at) FROM delivery_attempts WHERE outcome='succeeded'`, &lastOK); err != nil {
			return nil, err
		}
		if span := lastOK.Sub(r.FirstAttempt).Seconds(); span > 0 {
			r.DeliveredPerSec = float64(r.StatusCounts["delivered"]) / span
		}
	}

	for _, s := range samples {
		r.PeakReady = max(r.PeakReady, s.Ready)
		r.PeakScheduled = max(r.PeakScheduled, s.Scheduled)
		r.PeakInFlight = max(r.PeakInFlight, s.InFlight)
		r.PeakBacklog = max(r.PeakBacklog, s.Ready+s.Scheduled)
	}

	r.QueueSeries = samples
	if err := buildOutage(ctx, e, r, samples); err != nil {
		return nil, err
	}
	if err := buildTenants(ctx, e, r, tenants); err != nil {
		return nil, err
	}
	return r, nil
}

// buildOutage measures recovery from the simulated outage window, using the
// timestamps of the attempts that actually failed with the outage error.
func buildOutage(ctx context.Context, e *env, r *deliveryReport, samples []sample) error {
	o := &outageReport{}
	var first, last *time.Time
	if err := e.pg.QueryRow(ctx, `
SELECT min(started_at), max(finished_at), count(*) FROM delivery_attempts WHERE error = 'simulated outage'`).
		Scan(&first, &last, &o.OutageFailures); err != nil {
		return fmt.Errorf("outage query: %w", err)
	}
	if o.OutageFailures == 0 || first == nil {
		return nil
	}
	o.FirstOutageFailure, o.LastOutageFailure = *first, *last

	var firstOK *time.Time
	if err := e.pg.QueryRow(ctx, `
SELECT min(finished_at) FROM delivery_attempts WHERE outcome='succeeded' AND started_at > $1`, *last).Scan(&firstOK); err != nil {
		return err
	}
	if firstOK != nil {
		o.FirstSuccessAfterS = firstOK.Sub(*last).Seconds()
	}
	if err := e.pg.QueryRow(ctx, `
SELECT count(*) FROM delivery_attempts WHERE outcome='succeeded' AND started_at >= $1 AND finished_at <= $2`, *first, *last).
		Scan(&o.SuccessesDuringOutage); err != nil {
		return err
	}

	for _, s := range samples {
		depth := s.Ready + s.Scheduled
		if s.At.Before(*first) {
			o.PreOutagePeakBacklog = max(o.PreOutagePeakBacklog, depth)
		} else {
			o.PeakBacklogDuring = max(o.PeakBacklogDuring, depth)
		}
	}
	for _, s := range samples {
		if s.At.After(*last) && s.Ready+s.Scheduled <= o.PreOutagePeakBacklog {
			o.BacklogRecovered = true
			o.BacklogRecoveredS = s.At.Sub(*last).Seconds()
			break
		}
	}
	r.Outage = o
	return nil
}

func buildTenants(ctx context.Context, e *env, r *deliveryReport, tenants []tenant) error {
	names := map[string]string{}
	for _, t := range tenants {
		names[t.ClientID] = t.Name
	}
	rows, err := e.pg.Query(ctx, firstAndSuccess+`
SELECT n.client_id::text, count(*), count(*) FILTER (WHERE n.status='delivered'),
  `+pctSQL(epochMS("fa.ok_finish - n.created_at"))+`,
  `+pctSQL(epochMS("fa.first_start - n.created_at"))+`,
  max(n.updated_at), min(n.created_at)
FROM notifications n LEFT JOIN fa ON fa.notification_id = n.id
GROUP BY n.client_id ORDER BY count(*) DESC`)
	if err != nil {
		return fmt.Errorf("tenant query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t tenantReport
		var id string
		var firstCreated time.Time
		dst := []any{&id, &t.Count, &t.Delivered}
		dst = append(dst, scanPct(&t.E2E)...)
		dst = append(dst, scanPct(&t.QueueWait)...)
		dst = append(dst, &t.LastDone, &firstCreated)
		if err := rows.Scan(dst...); err != nil {
			return err
		}
		t.Tenant = names[id]
		if t.Tenant == "" {
			t.Tenant = id
		}
		if span := t.LastDone.Sub(firstCreated).Seconds(); span > 0 {
			t.PerSec = float64(t.Delivered) / span
		}
		r.Tenants = append(r.Tenants, t)
	}
	return rows.Err()
}

func printDelivery(r *deliveryReport) {
	fmt.Printf("\n== delivery (from postgres) ==\n")
	fmt.Printf("  all_terminal=%v notifications=%d status=%v\n", r.Completed, r.Notifications, r.StatusCounts)
	fmt.Printf("  attempts=%d ok=%d failed=%d retries=%d retried_notifs=%d delivered_more_than_once=%d\n",
		r.Attempts, r.AttemptsOK, r.AttemptsFailed, r.Retries, r.RetriedNotifs, r.DuplicateSends)
	fmt.Printf("  dlq: permanent=%d exhausted=%d  errors=%v\n", r.DLQPermanent, r.DLQExhausted, r.ErrorBreakdown)
	fmt.Printf("  success_rate=%.3f%% first_try=%.3f%%\n", r.SuccessRate, r.FirstTryRate)
	fmt.Printf("  e2e ms           p50=%.1f p95=%.1f p99=%.1f max=%.1f (n=%d)\n", r.E2E.P50, r.E2E.P95, r.E2E.P99, r.E2E.Max, r.E2E.N)
	fmt.Printf("  e2e first-try ms p50=%.1f p95=%.1f p99=%.1f\n", r.E2EFirstTry.P50, r.E2EFirstTry.P95, r.E2EFirstTry.P99)
	fmt.Printf("  queue wait ms    p50=%.1f p95=%.1f p99=%.1f max=%.1f\n", r.QueueWait.P50, r.QueueWait.P95, r.QueueWait.P99, r.QueueWait.Max)
	fmt.Printf("  provider call ms p50=%.1f p95=%.1f p99=%.1f\n", r.ProviderCall.P50, r.ProviderCall.P95, r.ProviderCall.P99)
	fmt.Printf("  drain: from_first_attempt=%.2fs from_first_created=%.2fs after_last_created=%.2fs\n",
		r.DrainFromFirstAttemptS, r.DrainFromFirstCreateS, r.DrainAfterLastCreateS)
	fmt.Printf("  throughput: avg=%.1f delivered/s peak_1s=%d\n", r.DeliveredPerSec, r.PeakDelivered1s)
	fmt.Printf("  queue peaks: ready=%d scheduled=%d in_flight=%d backlog=%d (samples=%d)\n",
		r.PeakReady, r.PeakScheduled, r.PeakInFlight, r.PeakBacklog, r.Samples)
	if o := r.Outage; o != nil {
		fmt.Printf("  outage: failures=%d window=%s..%s first_success_after=%.2fs backlog_recovered=%v in %.2fs (pre-peak=%d, peak=%d) successes_during=%d\n",
			o.OutageFailures, o.FirstOutageFailure.Format("15:04:05.000"), o.LastOutageFailure.Format("15:04:05.000"),
			o.FirstSuccessAfterS, o.BacklogRecovered, o.BacklogRecoveredS, o.PreOutagePeakBacklog, o.PeakBacklogDuring, o.SuccessesDuringOutage)
	}
	for _, t := range r.Tenants {
		fmt.Printf("  tenant %-16s n=%-7d delivered=%-7d e2e p50=%.1f p95=%.1f p99=%.1f | queue_wait p50=%.1f p95=%.1f p99=%.1f | %.1f/s\n",
			t.Tenant, t.Count, t.Delivered, t.E2E.P50, t.E2E.P95, t.E2E.P99, t.QueueWait.P50, t.QueueWait.P95, t.QueueWait.P99, t.PerSec)
	}
}

// runWait samples the queue and waits for every notification to be terminal.
// It is used for backlog drains, where the load was sent with workers stopped.
func runWait(args []string) error {
	fs := newFlags("wait")
	keysPath := fs.String("keys", "/results/keys.json", "tenant keys from setup")
	label := fs.String("label", "wait", "result file name")
	out := fs.String("out", "/results", "result directory")
	timeout := fs.Duration("timeout", 30*time.Minute, "give up after this long")
	_ = fs.Parse(args)

	ctx := context.Background()
	e, err := connect(ctx)
	if err != nil {
		return err
	}
	defer e.close()
	tenants, err := loadTenants(*keysPath)
	if err != nil {
		return err
	}
	sampler := startSampler(ctx, e, 200*time.Millisecond)
	started := time.Now()
	rep, err := waitAndReport(ctx, e, sampler, tenants, started, *timeout)
	if err != nil {
		return err
	}
	return writeJSON(fmt.Sprintf("%s/%s.json", *out, *label), loadReport{Label: *label, StartedAt: started, EndedAt: time.Now(), Delivery: rep})
}

// runReport prints the delivery report for whatever is in the database now.
func runReport(args []string) error {
	fs := newFlags("report")
	keysPath := fs.String("keys", "/results/keys.json", "tenant keys from setup")
	_ = fs.Parse(args)
	ctx := context.Background()
	e, err := connect(ctx)
	if err != nil {
		return err
	}
	defer e.close()
	tenants, err := loadTenants(*keysPath)
	if err != nil {
		return err
	}
	rep, err := buildReport(ctx, e, nil, tenants)
	if err != nil {
		return err
	}
	printDelivery(rep)
	return nil
}
