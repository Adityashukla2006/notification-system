// Command worker consumes the Redis queue and delivers notifications via
// pluggable providers, recording outcomes. It is intentionally a separate
// binary from the API so that delivery load can be scaled — and can fail —
// independently of request ingestion.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Adityashukla2006/notification-system/api/internal/config"
	"github.com/Adityashukla2006/notification-system/api/internal/provider"
	"github.com/Adityashukla2006/notification-system/api/internal/queue"
	"github.com/Adityashukla2006/notification-system/api/internal/retry"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
	"github.com/Adityashukla2006/notification-system/api/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

// run wires the process together and blocks until shutdown. It returns an error
// rather than calling os.Exit itself so deferred cleanup runs.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	workerID, err := resolveWorkerID(cfg.Worker.ID)
	if err != nil {
		return err
	}

	pgCfg, err := pgxpool.ParseConfig(cfg.Postgres.DSN())
	if err != nil {
		return err
	}
	pgCfg.MaxConns = cfg.Postgres.MaxConns

	pool, err := pgxpool.NewWithConfig(context.Background(), pgCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	concurrency := max(cfg.Worker.Concurrency, 1)

	// Every delivery loop parks one connection inside a blocking BLMOVE, so the
	// pool must hold one per loop on top of the default headroom. Left at the
	// default (10 per GOMAXPROCS), a container limited to 2 CPUs would starve
	// its own loops of connections past 20 slots.
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: concurrency + 10*runtime.GOMAXPROCS(0),
	})
	defer func() { _ = rdb.Close() }()

	st := store.New(pool)
	registry := providers(logger, cfg, rdb)

	// Cancellation on SIGINT/SIGTERM is the shutdown mechanism: Run observes it
	// between deliveries, so an in-flight send always finishes and records its
	// outcome before the loop returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting delivery loops", "concurrency", concurrency)

	// Each slot is a complete logical worker with its own identity, rather than
	// N goroutines sharing one processing list. That keeps the crash-recovery
	// story unchanged: a slot's claims live under a key only it drains on
	// restart, and its own liveness key tells others when they are abandoned.
	// The per-slot promoter, reaper, and reclaimer loops are redundant work, but
	// each was already built to run safely on every worker at once.
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup
	for slot := range concurrency {
		slotID := workerID
		if concurrency > 1 {
			slotID = fmt.Sprintf("%s-%d", workerID, slot)
		}
		w := newWorker(cfg, st, rdb, registry, logger.With("worker_id", slotID), slotID)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				errs <- fmt.Errorf("worker %s: %w", slotID, err)
				stop()
			}
		}()
	}
	wg.Wait()
	close(errs)

	if err := <-errs; err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// newWorker builds one delivery loop identified by workerID. All loops in a
// process share the Postgres pool, Redis client, and provider registry.
func newWorker(cfg config.Config, st *store.Store, rdb *redis.Client, registry provider.Registry, logger *slog.Logger, workerID string) *worker.Worker {
	return worker.New(
		st,
		queue.NewConsumer(rdb, workerID),
		queue.NewScheduler(rdb),
		queue.NewReclaimer(rdb),
		queue.New(rdb),
		registry,
		logger,
		worker.Config{
			WorkerID:        workerID,
			ReapEvery:       cfg.Worker.ReapEvery,
			StuckAfter:      cfg.Worker.StuckAfter,
			ReapLimit:       cfg.Worker.ReapLimit,
			DeliveryTimeout: cfg.Worker.DeliveryTimeout,
			ClaimTimeout:    cfg.Worker.ClaimTimeout,
			PromoteEvery:    cfg.Worker.PromoteEvery,
			PromoteLimit:    cfg.Worker.PromoteLimit,
			HeartbeatEvery:  cfg.Worker.HeartbeatEvery,
			LivenessTTL:     cfg.Worker.LivenessTTL,
			ReclaimEvery:    cfg.Worker.ReclaimEvery,
			Policy: retry.Policy{
				Base: cfg.Worker.RetryBase,
				Max:  cfg.Worker.RetryMax,
			},
		},
	)
}

// providers builds the channel registry.
//
// Email resolves in order of precedence — Resend, then SMTP, then the logging
// stub — so a deployment sends real mail when credentials exist and still runs
// end to end when they do not. SMS and push remain stubs; each becomes real by
// writing one Deliver method and swapping it in here, with no change to the
// worker loop.
func providers(logger *slog.Logger, cfg config.Config, rdb *redis.Client) provider.Registry {
	registry := provider.Registry{
		string(store.ChannelEmail): provider.NewLog(logger, string(store.ChannelEmail)),
		string(store.ChannelSMS):   provider.NewLog(logger, string(store.ChannelSMS)),
		string(store.ChannelPush):  provider.NewLog(logger, string(store.ChannelPush)),
	}

	switch {
	case cfg.ProviderSim.Enabled:
		// Checked first so a load test can never fall through to a real
		// provider because credentials happened to be in the environment.
		sim := cfg.ProviderSim
		logger.Warn("all channels using the SIMULATED provider; nothing is actually sent",
			"latency", sim.Latency, "jitter", sim.Jitter,
			"fail_rate", sim.FailRate, "permanent_rate", sim.PermanentRate,
			"timeout_rate", sim.TimeoutRate,
			"outage_after", sim.OutageAfter, "outage_for", sim.OutageFor,
			"ledger", sim.Ledger)
		var ledger provider.Ledger
		if sim.Ledger {
			ledger = redisLedger{rdb}
		}
		simulated := provider.NewSimulated(provider.SimulatedConfig{
			Ledger:        ledger,
			Latency:       sim.Latency,
			Jitter:        sim.Jitter,
			FailRate:      sim.FailRate,
			PermanentRate: sim.PermanentRate,
			TimeoutRate:   sim.TimeoutRate,
			OutageAfter:   sim.OutageAfter,
			OutageFor:     sim.OutageFor,
		})
		for channel := range registry {
			registry[channel] = simulated
		}

	case cfg.Resend.Enabled():
		// The key is never logged — only the fact that it is present.
		logger.Info("email channel using resend", "from", cfg.Resend.From)
		registry[string(store.ChannelEmail)] = provider.NewResend(provider.ResendConfig{
			APIKey: cfg.Resend.APIKey,
			From:   cfg.Resend.From,
		})

	case cfg.SMTP.Enabled():
		logger.Info("email channel using smtp",
			"host", cfg.SMTP.Host, "port", cfg.SMTP.Port, "starttls", cfg.SMTP.StartTLS)
		registry[string(store.ChannelEmail)] = provider.NewSMTP(provider.SMTPConfig{
			Host:               cfg.SMTP.Host,
			Port:               cfg.SMTP.Port,
			Username:           cfg.SMTP.Username,
			Password:           cfg.SMTP.Password,
			From:               cfg.SMTP.From,
			StartTLS:           cfg.SMTP.StartTLS,
			InsecureSkipVerify: cfg.SMTP.InsecureSkipVerify,
		})

	default:
		logger.Warn("neither RESEND_API_KEY nor SMTP_HOST is set; email is logged, not sent")
	}

	return registry
}

// resolveWorkerID falls back to the hostname when WORKER_ID is unset, since a
// container's hostname is stable for the life of the instance.
func resolveWorkerID(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return host, nil
}

// newLogger builds a JSON structured logger at the configured level, defaulting
// to info for any unrecognized value.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// simLedgerKey is the Redis hash the simulated provider's ledger writes to:
// notification id -> number of successful simulated sends.
const simLedgerKey = "sim:sends"

// redisLedger is the simulated provider's send ledger, kept in Redis so that it
// survives the worker process being killed.
type redisLedger struct {
	client *redis.Client
}

// RecordSend counts one successful simulated send of id.
func (l redisLedger) RecordSend(ctx context.Context, id uuid.UUID) error {
	if err := l.client.HIncrBy(ctx, simLedgerKey, id.String(), 1).Err(); err != nil {
		return fmt.Errorf("recording simulated send of %s: %w", id, err)
	}
	return nil
}
