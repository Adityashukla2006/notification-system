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

	// SMTP is the mail server the email channel delivers through when Resend is
	// not configured. It is how local development sends mail into Mailpit.
	SMTP SMTPConfig `envPrefix:"SMTP_"`

	// Resend is the hosted email API. When its key is set it takes precedence
	// over SMTP for the email channel.
	Resend ResendConfig `envPrefix:"RESEND_"`

	// ProviderSim replaces every channel's provider with a fault-injecting
	// simulation. It exists for load testing only.
	ProviderSim ProviderSimConfig `envPrefix:"PROVIDER_SIM_"`
}

// ProviderSimConfig configures the simulated provider used for benchmarks.
//
// It is opt-in and overrides Resend and SMTP when enabled, because a load test
// that accidentally reaches a real provider sends real mail to real people at
// thousands per second.
type ProviderSimConfig struct {
	// Enabled swaps every channel's provider for the simulation.
	Enabled bool `env:"ENABLED" envDefault:"false"`

	// Latency is the base duration of one simulated provider call.
	Latency time.Duration `env:"LATENCY" envDefault:"0s"`

	// Jitter widens Latency uniformly to [Latency-Jitter, Latency+Jitter].
	Jitter time.Duration `env:"JITTER" envDefault:"0s"`

	// FailRate is the probability, 0..1, that a call fails transiently and
	// earns a retry.
	FailRate float64 `env:"FAIL_RATE" envDefault:"0"`

	// PermanentRate is the probability, 0..1, that a call fails permanently
	// and is dead-lettered without retry.
	PermanentRate float64 `env:"PERMANENT_RATE" envDefault:"0"`

	// TimeoutRate is the probability, 0..1, that a call hangs until the
	// worker's delivery timeout cancels it.
	TimeoutRate float64 `env:"TIMEOUT_RATE" envDefault:"0"`

	// OutageAfter and OutageFor describe a window, measured from process
	// start, during which every call fails transiently. OutageFor of zero
	// disables it.
	OutageAfter time.Duration `env:"OUTAGE_AFTER" envDefault:"0s"`
	OutageFor   time.Duration `env:"OUTAGE_FOR" envDefault:"0s"`

	// Ledger counts every successful simulated send per notification in the
	// Redis hash sim:sends, so a benchmark can detect duplicate deliveries that
	// Postgres cannot see.
	Ledger bool `env:"LEDGER" envDefault:"false"`
}

// ResendConfig configures the hosted Resend email API.
type ResendConfig struct {
	// APIKey authenticates with Resend. Empty disables the provider.
	//
	// It comes from the environment and must never be committed: a key in
	// source is a key in every clone, every CI log, and every fork.
	APIKey string `env:"API_KEY" envDefault:""`

	// From is the sender address. It must be on a domain verified with Resend,
	// or their onboarding@resend.dev sandbox sender.
	From string `env:"FROM" envDefault:"onboarding@resend.dev"`
}

// Enabled reports whether Resend is configured.
func (r ResendConfig) Enabled() bool {
	return r.APIKey != ""
}

// SMTPConfig describes the mail server used for the email channel.
type SMTPConfig struct {
	// Host is the mail server. Empty disables real email delivery.
	Host string `env:"HOST" envDefault:""`
	Port int    `env:"PORT" envDefault:"587"`

	// Username and Password are optional: a local test server usually needs
	// neither.
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`

	// From is the envelope sender and the From header.
	From string `env:"FROM" envDefault:""`

	// StartTLS should be on for any real server, and off for a local test
	// server that does not offer TLS.
	StartTLS bool `env:"STARTTLS" envDefault:"true"`

	// InsecureSkipVerify disables certificate verification. Development only —
	// it defeats the point of TLS.
	InsecureSkipVerify bool `env:"INSECURE_SKIP_VERIFY" envDefault:"false"`
}

// Enabled reports whether real email delivery is configured.
func (s SMTPConfig) Enabled() bool {
	return s.Host != ""
}

// RateLimitConfig bounds per-client request rate with a token bucket.
type RateLimitConfig struct {
	// Rate is the steady rate, in requests per second, one client may sustain.
	// The default keeps the previous average of 100 per minute.
	Rate float64 `env:"RATE" envDefault:"1.6667"`

	// Burst is the bucket size: how many requests a client may make at once
	// after being idle. It is what bounds a burst, which a per-minute window
	// could not do.
	Burst int `env:"BURST" envDefault:"20"`

	// Instances is how many API instances share the budget. When Redis is
	// unreachable, each instance falls back to an in-memory bucket holding
	// Rate/Instances and Burst/Instances, so together they approximate the
	// global limit. Too low lets an outage multiply the limit; too high makes
	// the fallback stricter than intended.
	Instances int `env:"INSTANCES" envDefault:"1"`
}

// validate rejects settings the limiter cannot work with.
func (r RateLimitConfig) validate() error {
	if !(r.Rate > 0) {
		return fmt.Errorf("RATE_LIMIT_RATE must be greater than 0, got %v", r.Rate)
	}
	if r.Burst < 1 {
		return fmt.Errorf("RATE_LIMIT_BURST must be at least 1, got %d", r.Burst)
	}
	if r.Instances < 1 {
		return fmt.Errorf("RATE_LIMIT_INSTANCES must be at least 1, got %d", r.Instances)
	}
	return nil
}

// WorkerConfig holds the delivery worker's settings.
type WorkerConfig struct {
	// ID names this worker's in-flight processing list in Redis. It must be
	// STABLE across restarts: a worker that comes back under a new id orphans
	// the claims it left behind. Defaulting to the hostname is right for a
	// StatefulSet pod and wrong for a randomly-named one, so it is overridable.
	ID string `env:"ID" envDefault:""`

	// Concurrency is how many delivery loops one worker process runs. Each
	// loop is a full logical worker with its own processing list and liveness
	// key, named "<ID>-<n>" when Concurrency > 1, so crash recovery is unchanged.
	// It is also the hard upper bound on in-flight deliveries per process.
	Concurrency int `env:"CONCURRENCY" envDefault:"1"`

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

	// DeliveryTimeout bounds a single provider call. Without it, a provider
	// that accepts a connection and then stops responding holds the worker
	// forever. It must be shorter than StuckAfter, or the reaper would requeue
	// a delivery that is still legitimately running.
	DeliveryTimeout time.Duration `env:"DELIVERY_TIMEOUT" envDefault:"30s"`
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
	if err := cfg.RateLimit.validate(); err != nil {
		return Config{}, err
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
