// Command server runs the notification API. It validates, authenticates,
// rate-limits, persists, and enqueues requests, then returns 202. It never
// calls delivery providers directly — that is the worker's job.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Adityashukla2006/notification-system/api/internal/config"
	apihttp "github.com/Adityashukla2006/notification-system/api/internal/http"
	"github.com/Adityashukla2006/notification-system/api/internal/notification"
	"github.com/Adityashukla2006/notification-system/api/internal/queue"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// run wires the process together and blocks until shutdown. It returns an error
// rather than calling os.Exit itself so that deferred cleanup runs and the
// logic is testable.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	// A pool, not a single connection: the API serves concurrent requests.
	// Ping is deferred to the readiness probe, so a database that is briefly
	// down at boot does not prevent the process from starting and reporting
	// itself unready.
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

	st := store.New(pool)
	q := queue.New(rdb)
	sch := queue.NewScheduler(rdb)
	notifications := notification.New(st, q, sch, logger)

	handler := apihttp.Router(logger, pool, redisPinger{rdb}, st, notifications, st)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Serve in the background so main can wait on either a server error or a
	// shutdown signal.
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections")
	}

	// Graceful shutdown: stop accepting new requests, let in-flight ones
	// finish within the grace period, then let the deferred closes release
	// Postgres and Redis.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// newLogger builds a JSON structured logger at the configured level, defaulting
// to info for any unrecognized value.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}

// redisPinger adapts a *redis.Client to the apihttp.Pinger interface. The
// client's own Ping returns a *redis.StatusCmd, so we unwrap it to an error.
type redisPinger struct {
	client *redis.Client
}

// Ping reports whether Redis is reachable.
func (r redisPinger) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}
