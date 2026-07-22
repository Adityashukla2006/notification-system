// Command worker consumes the Redis queue and delivers notifications via
// pluggable providers, recording outcomes. It is intentionally a separate
// binary from the API so that delivery load can be scaled — and can fail —
// independently of request ingestion.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer func() { _ = rdb.Close() }()

	w := worker.New(
		store.New(pool),
		queue.NewConsumer(rdb, workerID),
		queue.NewScheduler(rdb),
		providers(logger),
		logger.With("worker_id", workerID),
		worker.Config{
			ClaimTimeout: cfg.Worker.ClaimTimeout,
			PromoteEvery: cfg.Worker.PromoteEvery,
			PromoteLimit: cfg.Worker.PromoteLimit,
			Policy: retry.Policy{
				Base: cfg.Worker.RetryBase,
				Max:  cfg.Worker.RetryMax,
			},
		},
	)

	// Cancellation on SIGINT/SIGTERM is the shutdown mechanism: Run observes it
	// between deliveries, so an in-flight send always finishes and records its
	// outcome before the loop returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Run(ctx); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// providers builds the channel registry. Every channel is currently served by
// the logging stub; real integrations replace these one at a time without the
// worker loop changing.
func providers(logger *slog.Logger) provider.Registry {
	return provider.Registry{
		string(store.ChannelEmail): provider.NewLog(logger, string(store.ChannelEmail)),
		string(store.ChannelSMS):   provider.NewLog(logger, string(store.ChannelSMS)),
		string(store.ChannelPush):  provider.NewLog(logger, string(store.ChannelPush)),
	}
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
