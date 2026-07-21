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
