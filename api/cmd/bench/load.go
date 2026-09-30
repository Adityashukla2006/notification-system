package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// streamSpec is one independent source of traffic. Several run at once so a
// scenario can pit a heavy tenant against light ones.
//
// Exactly one of N (closed loop: send N as fast as C in-flight allow) or
// Rate+Duration (open loop: send at a fixed schedule) is used.
type streamSpec struct {
	Name     string        `json:"name"`
	Tenant   int           `json:"tenant"`
	N        int           `json:"n,omitempty"`
	Rate     float64       `json:"rate,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	C        int           `json:"concurrency"`
	// Dup sends every idempotency key this many times, concurrently, with an
	// identical body. 1 means no duplicates.
	Dup int `json:"dup"`
	// Conflict additionally sends each key once more with a different body,
	// which the API must reject with 409.
	Conflict bool `json:"conflict,omitempty"`
	// Channel is email, sms, push, or mix (60/25/15).
	Channel     string `json:"channel"`
	MaxAttempts int    `json:"max_attempts,omitempty"`
}

// parseStream reads "name=heavy,tenant=0,n=20000,c=64,dup=1,channel=mix".
func parseStream(s string) (streamSpec, error) {
	spec := streamSpec{C: 32, Dup: 1, Channel: "mix"}
	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return spec, fmt.Errorf("stream option %q is not key=value", part)
		}
		var err error
		switch k {
		case "name":
			spec.Name = v
		case "tenant":
			spec.Tenant, err = strconv.Atoi(v)
		case "n":
			spec.N, err = strconv.Atoi(v)
		case "rate":
			spec.Rate, err = strconv.ParseFloat(v, 64)
		case "duration":
			spec.Duration, err = time.ParseDuration(v)
		case "c":
			spec.C, err = strconv.Atoi(v)
		case "dup":
			spec.Dup, err = strconv.Atoi(v)
		case "conflict":
			spec.Conflict, err = strconv.ParseBool(v)
		case "channel":
			spec.Channel = v
		case "max_attempts":
			spec.MaxAttempts, err = strconv.Atoi(v)
		default:
			return spec, fmt.Errorf("unknown stream option %q", k)
		}
		if err != nil {
			return spec, fmt.Errorf("stream option %s: %w", k, err)
		}
	}
	if spec.Name == "" {
		spec.Name = fmt.Sprintf("tenant%d", spec.Tenant)
	}
	if (spec.N > 0) == (spec.Rate > 0) {
		return spec, fmt.Errorf("stream %s: set exactly one of n or rate", spec.Name)
	}
	if spec.Rate > 0 && spec.Duration <= 0 {
		return spec, fmt.Errorf("stream %s: rate needs duration", spec.Name)
	}
	spec.Dup = max(spec.Dup, 1)
	spec.C = max(spec.C, 1)
	return spec, nil
}

type streamFlags []string

func (s *streamFlags) String() string     { return strings.Join(*s, " ") }
func (s *streamFlags) Set(v string) error { *s = append(*s, v); return nil }

// streamResult is the client-side view of one stream: what was sent, what came
// back, and how long the API took to accept it.
type streamResult struct {
	Spec            streamSpec     `json:"spec"`
	UniqueKeys      int            `json:"unique_keys"`
	Requests        int64          `json:"requests"`
	StatusCounts    map[string]int `json:"status_counts"`
	TransportErrors int64          `json:"transport_errors"`
	Elapsed         float64        `json:"elapsed_s"`
	RequestsPerSec  float64        `json:"requests_per_sec"`
	AcceptedPerSec  float64        `json:"accepted_202_per_sec"`
	// LatencyMS is the HTTP round trip of 202 responses: the enqueue latency
	// as a client experiences it (auth, rate limit, insert, LPUSH).
	LatencyMS percentiles `json:"enqueue_latency_ms_202"`
	// LateSends counts open-loop sends that started >50ms behind schedule
	// because all C slots were busy — a sign the target rate was not achieved.
	LateSends int64 `json:"late_sends,omitempty"`
	// Dedup accounting, only meaningful when Dup > 1.
	KeysWithOneID     int `json:"keys_resolving_to_one_id,omitempty"`
	KeysWithManyIDs   int `json:"keys_resolving_to_many_ids,omitempty"`
	DuplicateRequests int `json:"duplicate_requests_sent,omitempty"`
	ConflictsRejected int `json:"conflicts_rejected_409,omitempty"`
	ConflictsAccepted int `json:"conflicts_wrongly_accepted,omitempty"`
}

type percentiles struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
	Avg float64 `json:"avg"`
}

// nearestRank computes percentiles by the nearest-rank method, which returns
// only values that were actually observed.
func nearestRank(vals []float64) percentiles {
	if len(vals) == 0 {
		return percentiles{}
	}
	sort.Float64s(vals)
	at := func(p float64) float64 {
		idx := int(float64(len(vals))*p+0.999999) - 1
		return vals[min(max(idx, 0), len(vals)-1)]
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return percentiles{N: len(vals), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: vals[len(vals)-1], Avg: sum / float64(len(vals))}
}

// loadReport is everything one load run produced.
type loadReport struct {
	Label     string          `json:"label"`
	StartedAt time.Time       `json:"started_at"`
	EndedAt   time.Time       `json:"ended_at"`
	Streams   []streamResult  `json:"streams"`
	Delivery  *deliveryReport `json:"delivery,omitempty"`
}

func runLoad(args []string) error {
	fs := newFlags("load")
	var streams streamFlags
	fs.Var(&streams, "stream", "traffic stream spec (repeatable)")
	target := fs.String("target", "http://api:8080", "API base URL")
	keysPath := fs.String("keys", "/results/keys.json", "tenant keys from setup")
	label := fs.String("label", "run", "name for this run's result file")
	out := fs.String("out", "/results", "result directory")
	wait := fs.Bool("wait", false, "after sending, wait for every notification to reach a terminal state")
	waitTimeout := fs.Duration("wait-timeout", 30*time.Minute, "give up waiting after this long")
	_ = fs.Parse(args)

	tenants, err := loadTenants(*keysPath)
	if err != nil {
		return err
	}
	specs := make([]streamSpec, 0, len(streams))
	for _, s := range streams {
		spec, err := parseStream(s)
		if err != nil {
			return err
		}
		if spec.Tenant >= len(tenants) {
			return fmt.Errorf("stream %s: tenant %d does not exist", spec.Name, spec.Tenant)
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return fmt.Errorf("at least one -stream is required")
	}

	ctx := context.Background()
	var e *env
	var sampler *queueSampler
	if *wait {
		if e, err = connect(ctx); err != nil {
			return err
		}
		defer e.close()
		sampler = startSampler(ctx, e, 200*time.Millisecond)
	}

	totalC := 0
	for _, s := range specs {
		totalC += s.C * s.Dup
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        totalC + 16,
			MaxIdleConnsPerHost: totalC + 16,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	rep := loadReport{Label: *label, StartedAt: time.Now()}
	results := make([]streamResult, len(specs))
	var wg sync.WaitGroup
	for i, spec := range specs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runStream(ctx, client, *target, tenants[spec.Tenant], spec, runID+"-"+spec.Name)
		}()
	}
	wg.Wait()
	rep.EndedAt = time.Now()
	rep.Streams = results

	for _, r := range results {
		printStream(r)
	}

	if *wait {
		d, err := waitAndReport(ctx, e, sampler, tenants, rep.StartedAt, *waitTimeout)
		if err != nil {
			return err
		}
		rep.Delivery = d
	}
	return writeJSON(fmt.Sprintf("%s/%s.json", *out, *label), rep)
}

func loadTenants(path string) ([]tenant, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading keys: %w", err)
	}
	var ts []tenant
	if err := json.Unmarshal(b, &ts); err != nil {
		return nil, fmt.Errorf("parsing keys: %w", err)
	}
	return ts, nil
}

// runStream sends one stream's traffic and returns its client-side results.
func runStream(ctx context.Context, client *http.Client, target string, t tenant, spec streamSpec, keyPrefix string) streamResult {
	var (
		mu        sync.Mutex
		latencies []float64
		statuses  = map[string]int{}
		idsByKey  = map[string]map[string]struct{}{}
		requests  atomic.Int64
		transport atomic.Int64
		late      atomic.Int64
		conflict  = map[int]int{}
	)

	send := func(key string, body []byte, isConflict bool) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/notifications", bytes.NewReader(body))
		if err != nil {
			transport.Add(1)
			return
		}
		req.Header.Set("Authorization", "Bearer "+t.Token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)
		requests.Add(1)
		if err != nil {
			transport.Add(1)
			return
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		mu.Lock()
		defer mu.Unlock()
		statuses[strconv.Itoa(resp.StatusCode)]++
		if isConflict {
			conflict[resp.StatusCode]++
			return
		}
		if resp.StatusCode == http.StatusAccepted {
			latencies = append(latencies, ms(elapsed))
			var r struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(respBody, &r) == nil {
				if idsByKey[key] == nil {
					idsByKey[key] = map[string]struct{}{}
				}
				idsByKey[key][r.ID] = struct{}{}
			}
		}
	}

	// sendKey issues every request for one idempotency key: Dup identical
	// copies at once (the race the UNIQUE constraint must win), then the
	// optional conflicting copy.
	sendKey := func(i int) {
		key := fmt.Sprintf("%s-%d", keyPrefix, i)
		rng := rand.New(rand.NewPCG(uint64(i), 7))
		channel, recipient, payload := makeNotification(spec.Channel, rng, i)
		body := requestBody(channel, recipient, payload, spec.MaxAttempts)

		var dw sync.WaitGroup
		for range spec.Dup {
			dw.Add(1)
			go func() { defer dw.Done(); send(key, body, false) }()
		}
		dw.Wait()
		if spec.Conflict {
			payload["subject"] = "CHANGED " + fmt.Sprint(payload["subject"])
			send(key, requestBody(channel, recipient, payload, spec.MaxAttempts), true)
		}
	}

	sem := make(chan struct{}, spec.C)
	var wg sync.WaitGroup
	start := time.Now()
	keys := 0

	if spec.N > 0 {
		for i := range spec.N {
			sem <- struct{}{}
			wg.Add(1)
			go func() { defer wg.Done(); defer func() { <-sem }(); sendKey(i) }()
		}
		keys = spec.N
	} else {
		interval := time.Duration(float64(time.Second) / spec.Rate)
		total := int(spec.Rate * spec.Duration.Seconds())
		for i := range total {
			due := start.Add(time.Duration(i) * interval)
			if d := time.Until(due); d > 0 {
				time.Sleep(d)
			}
			sem <- struct{}{}
			if time.Since(due) > 50*time.Millisecond {
				late.Add(1)
			}
			wg.Add(1)
			go func() { defer wg.Done(); defer func() { <-sem }(); sendKey(i) }()
		}
		keys = total
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := streamResult{
		Spec:            spec,
		UniqueKeys:      keys,
		Requests:        requests.Load(),
		StatusCounts:    statuses,
		TransportErrors: transport.Load(),
		Elapsed:         elapsed.Seconds(),
		RequestsPerSec:  float64(requests.Load()) / elapsed.Seconds(),
		AcceptedPerSec:  float64(statuses["202"]) / elapsed.Seconds(),
		LatencyMS:       nearestRank(latencies),
		LateSends:       late.Load(),
	}
	if spec.Dup > 1 {
		res.DuplicateRequests = keys * (spec.Dup - 1)
		for _, ids := range idsByKey {
			if len(ids) == 1 {
				res.KeysWithOneID++
			} else {
				res.KeysWithManyIDs++
			}
		}
	}
	if spec.Conflict {
		res.ConflictsRejected = conflict[http.StatusConflict]
		res.ConflictsAccepted = conflict[http.StatusAccepted]
	}
	return res
}

func requestBody(channel, recipient string, payload map[string]any, maxAttempts int) []byte {
	req := map[string]any{"channel": channel, "recipient": recipient, "payload": payload}
	if maxAttempts > 0 {
		req["max_attempts"] = maxAttempts
	}
	b, _ := json.Marshal(req)
	return b
}

var (
	firstNames = []string{"Aarav", "Maya", "Liam", "Sofia", "Kenji", "Amara", "Noah", "Priya", "Lucas", "Zara"}
	products   = []string{"wireless headphones", "running shoes", "coffee grinder", "desk lamp", "backpack", "phone case"}
)

// makeNotification builds a realistic transactional notification. Email bodies
// are ~1 KB of order-confirmation text, SMS fits one 160-char segment, and push
// carries a title, body, and deep-link data, matching what a real product sends.
func makeNotification(channel string, rng *rand.Rand, i int) (string, string, map[string]any) {
	if channel == "mix" {
		switch r := rng.IntN(100); {
		case r < 60:
			channel = "email"
		case r < 85:
			channel = "sms"
		default:
			channel = "push"
		}
	}
	name := firstNames[rng.IntN(len(firstNames))]
	product := products[rng.IntN(len(products))]
	order := 100000 + rng.IntN(900000)
	amount := fmt.Sprintf("%d.%02d", 10+rng.IntN(490), rng.IntN(100))

	switch channel {
	case "sms":
		return "sms", fmt.Sprintf("+1555%07d", i%10000000), map[string]any{
			"body": fmt.Sprintf("Hi %s, your order #%d (%s, $%s) has shipped. Track it: https://ex.co/t/%d Reply STOP to opt out.", name, order, product, amount, order),
		}
	case "push":
		return "push", fmt.Sprintf("device-token-%064x", i), map[string]any{
			"title": "Your order is on its way",
			"body":  fmt.Sprintf("%s, your %s ships today.", name, product),
			"data":  map[string]any{"order_id": order, "deep_link": fmt.Sprintf("app://orders/%d", order)},
		}
	default:
		var b strings.Builder
		fmt.Fprintf(&b, "Hi %s,\n\nThanks for your order #%d. We've received your payment of $%s and your %s is being prepared for shipment.\n\n", name, order, amount, product)
		b.WriteString("Order summary\n-------------\n")
		fmt.Fprintf(&b, "Item: %s\nQuantity: %d\nSubtotal: $%s\nShipping: Standard (3-5 business days)\n\n", product, 1+rng.IntN(3), amount)
		b.WriteString("You'll receive another email with a tracking number as soon as your package leaves our warehouse. ")
		b.WriteString("If anything looks wrong, reply to this email or visit our help center and we'll sort it out right away. ")
		b.WriteString("Returns are free within 30 days of delivery; just start one from your order page and print the prepaid label.\n\n")
		b.WriteString("Thanks for shopping with us,\nThe Example Store team\n\n")
		b.WriteString("You are receiving this email because you placed an order at example.com. Manage preferences: https://example.com/prefs")
		return "email", fmt.Sprintf("customer%d@example.com", i), map[string]any{
			"subject": fmt.Sprintf("Order #%d confirmed", order),
			"body":    b.String(),
		}
	}
}

func printStream(r streamResult) {
	fmt.Printf("\n== stream %s (tenant %d) ==\n", r.Spec.Name, r.Spec.Tenant)
	fmt.Printf("  requests=%d  unique_keys=%d  elapsed=%.2fs  req/s=%.1f  202/s=%.1f  transport_errors=%d  late_sends=%d\n",
		r.Requests, r.UniqueKeys, r.Elapsed, r.RequestsPerSec, r.AcceptedPerSec, r.TransportErrors, r.LateSends)
	fmt.Printf("  status codes: %v\n", r.StatusCounts)
	fmt.Printf("  enqueue latency (202, ms): p50=%.2f p95=%.2f p99=%.2f max=%.2f\n", r.LatencyMS.P50, r.LatencyMS.P95, r.LatencyMS.P99, r.LatencyMS.Max)
	if r.Spec.Dup > 1 {
		fmt.Printf("  dedup: duplicate_requests=%d keys->1 id=%d keys->many ids=%d\n", r.DuplicateRequests, r.KeysWithOneID, r.KeysWithManyIDs)
	}
	if r.Spec.Conflict {
		fmt.Printf("  conflicts: rejected_409=%d wrongly_accepted=%d\n", r.ConflictsRejected, r.ConflictsAccepted)
	}
}
