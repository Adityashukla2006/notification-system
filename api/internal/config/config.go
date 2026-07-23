// Package config loads all service configuration from environment variables
// into a single typed struct at boot. Parsing and validation happen exactly
// once, at startup, so a misconfigured process fails fast with a clear error
// instead of failing later, mid-request, in a confusing way.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is the fully-parsed configuration for a single binary. Every field is
// populated from an environment variable; there are no other sources of truth.
type Config struct {
	// HTTP is the address the API server listens on, e.g. ":8080".
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`

	// ShutdownGrace is how long graceful shutdown waits for in-flight
	// requests to finish before forcing the server closed.
	ShutdownGrace time.Duration `env:"SHUTDOWN_GRACE" envDefault:"15s"`

	// LogLevel controls the minimum log level, e.g. "debug", "info", "warn".
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	// Postgres holds the discrete connection parameters. We keep them
	// discrete (rather than a single DSN string) so they are easy to eyeball
	// and edit, and so special characters in the password do not have to be
	// URL-encoded by hand.
	Postgres PostgresConfig `envPrefix:"POSTGRES_"`

	// Redis holds the connection parameters for the ephemeral coordination
	// store (queue, retry scheduler, rate limits).
	Redis RedisConfig `envPrefix:"REDIS_"`

	// Worker holds settings used only by the worker binary. The API parses
	// them too and ignores them; one struct for the whole service is simpler
	// than divergent per-binary configs.
	Worker WorkerConfig `envPrefix:"WORKER_"`

	// RateLimit caps how fast a single client may call the API.
	RateLimit RateLimitConfig `envPrefix:"RATE_LIMIT_"`
}

// RateLimitConfig bounds per-client request rate.
type RateLimitConfig struct {
	// Requests is how many requests one client may make per Window.
	Requests int `env:"REQUESTS" envDefault:"100"`

	// Window is the period the ceiling applies over. The limiter uses a
	// sliding window, so a client cannot double its rate by straddling a
	// window boundary.
	Window time.Duration `env:"WINDOW" envDefault:"1m"`
}

// WorkerConfig holds the delivery worker's settings.
type WorkerConfig struct {
	// ID names this worker's in-flight processing list in Redis. It must be
	// STABLE across restarts: a worker that comes back under a new id orphans
	// the claims it left behind. Defaulting to the hostname is right for a
	// StatefulSet pod and wrong for a randomly-named one, so it is overridable.
	ID string `env:"ID" envDefault:""`

	// ClaimTimeout bounds how long a single blocking claim waits for work. It
	// is also the worst-case delay before the worker notices a shutdown
	// signal, so it trades idle Redis chatter against shutdown latency.
	ClaimTimeout time.Duration `env:"CLAIM_TIMEOUT" envDefault:"5s"`

	// PromoteEvery is how often the worker sweeps the schedule for due
	// notifications. It sets the granularity of scheduled delivery: a
	// notification due at T is delivered somewhere in [T, T+PromoteEvery).
	PromoteEvery time.Duration `env:"PROMOTE_EVERY" envDefault:"1s"`

	// PromoteLimit caps how many notifications a single sweep promotes, so a
	// large backlog coming due at once drains in bounded batches instead of
	// flooding the ready queue in one burst.
	PromoteLimit int64 `env:"PROMOTE_LIMIT" envDefault:"100"`

	// RetryBase is the delay before the first retry; each subsequent attempt
	// doubles it before jitter.
	RetryBase time.Duration `env:"RETRY_BASE" envDefault:"1s"`

	// RetryMax caps the backoff delay. Without a cap, doubling reaches absurd
	// values within a handful of attempts.
	RetryMax time.Duration `env:"RETRY_MAX" envDefault:"5m"`

	// HeartbeatEvery is how often the worker refreshes its liveness key, which
	// is what tells other workers its in-flight claims are not abandoned.
	HeartbeatEvery time.Duration `env:"HEARTBEAT_EVERY" envDefault:"5s"`

	// LivenessTTL is how long that key survives without a refresh, and so how
	// long after a crash the worker's claims become reclaimable. It must
	// comfortably exceed HeartbeatEvery: set too tight, a worker that is merely
	// slow (a long GC pause, a stalled provider call) is declared dead and its
	// in-flight notification is delivered a second time underneath it.
	LivenessTTL time.Duration `env:"LIVENESS_TTL" envDefault:"30s"`

	// ReclaimEvery is how often the worker sweeps for claims left behind by
	// workers that are no longer alive.
	ReclaimEvery time.Duration `env:"RECLAIM_EVERY" envDefault:"30s"`

	// ReapEvery is how often the worker sweeps Postgres for notifications that
	// are stranded in a non-terminal state. This is the only recovery path that
	// survives losing Redis entirely, since it consults the source of truth
	// alone.
	ReapEvery time.Duration `env:"REAP_EVERY" envDefault:"1m"`

	// StuckAfter is how long a notification may sit untouched before the reaper
	// treats it as stranded. It must exceed the longest legitimate delivery, or
	// the reaper requeues work that is merely slow.
	StuckAfter time.Duration `env:"STUCK_AFTER" envDefault:"5m"`

	// ReapLimit caps how many rows one reap sweep recovers, so a large backlog
	// is drained in bounded batches.
	ReapLimit int `env:"REAP_LIMIT" envDefault:"100"`
}

// PostgresConfig holds the discrete parameters used to reach Postgres, the
// system's source of truth.
type PostgresConfig struct {
	Host     string `env:"HOST,required"`
	Port     int    `env:"PORT" envDefault:"5432"`
	User     string `env:"USER,required"`
	Password string `env:"PASSWORD,required"`
	DBName   string `env:"DBNAME,required"`
	SSLMode  string `env:"SSLMODE" envDefault:"disable"`

	// MaxConns caps the connection pool size. The API serves concurrent
	// requests, so it needs a pool, not a single connection.
	MaxConns int32 `env:"MAX_CONNS" envDefault:"10"`
}

// RedisConfig holds the connection parameters for Redis.
type RedisConfig struct {
	Addr     string `env:"ADDR,required"`
	Password string `env:"PASSWORD" envDefault:""`
	DB       int    `env:"DB" envDefault:"0"`
}

// Load parses the process environment into a Config, applying defaults and
// enforcing required fields. It returns an error if any required variable is
// missing or any value fails to parse.
func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config from environment: %w", err)
	}
	return cfg, nil
}

// DSN builds a libpq keyword/value connection string from the discrete
// Postgres parameters. Each value is quoted and escaped so that special
// characters (spaces, quotes, backslashes) in, for example, the password do
// not corrupt the key/value pairs.
func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		quoteDSNValue(p.Host),
		p.Port,
		quoteDSNValue(p.User),
		quoteDSNValue(p.Password),
		quoteDSNValue(p.DBName),
		quoteDSNValue(p.SSLMode),
	)
}

// quoteDSNValue wraps a libpq keyword/value value in single quotes, escaping
// backslashes and single quotes, per the rules pgx's DSN parser expects. This
// makes every value safe regardless of its contents.
func quoteDSNValue(v string) string {
	replaced := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v)
	return "'" + replaced + "'"
}
