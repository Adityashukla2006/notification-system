package config

import (
	"os"
	"testing"
	"time"
)

// requiredEnv is the minimal set of variables that must be present for Load to
// succeed. Individual tests remove one at a time to assert required-field
// enforcement.
func requiredEnv() map[string]string {
	return map[string]string{
		"POSTGRES_HOST":     "localhost",
		"POSTGRES_USER":     "app",
		"POSTGRES_PASSWORD": "secret",
		"POSTGRES_DBNAME":   "notifications",
		"REDIS_ADDR":        "localhost:6379",
	}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, cfg Config)
	}{
		{
			name: "defaults applied when only required vars set",
			env:  requiredEnv(),
			check: func(t *testing.T, cfg Config) {
				if cfg.HTTPAddr != ":8080" {
					t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
				}
				if cfg.ShutdownGrace != 15*time.Second {
					t.Errorf("ShutdownGrace = %v, want 15s", cfg.ShutdownGrace)
				}
				if cfg.LogLevel != "info" {
					t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
				}
				if cfg.Postgres.Port != 5432 {
					t.Errorf("Postgres.Port = %d, want 5432", cfg.Postgres.Port)
				}
				if cfg.Postgres.SSLMode != "disable" {
					t.Errorf("Postgres.SSLMode = %q, want disable", cfg.Postgres.SSLMode)
				}
				if cfg.Postgres.MaxConns != 10 {
					t.Errorf("Postgres.MaxConns = %d, want 10", cfg.Postgres.MaxConns)
				}
				if cfg.Redis.DB != 0 {
					t.Errorf("Redis.DB = %d, want 0", cfg.Redis.DB)
				}
			},
		},
		{
			name: "overrides parsed",
			env: mergeEnv(requiredEnv(), map[string]string{
				"HTTP_ADDR":          ":9090",
				"SHUTDOWN_GRACE":     "30s",
				"LOG_LEVEL":          "debug",
				"POSTGRES_PORT":      "6543",
				"POSTGRES_SSLMODE":   "require",
				"POSTGRES_MAX_CONNS": "42",
				"REDIS_DB":           "3",
			}),
			check: func(t *testing.T, cfg Config) {
				if cfg.HTTPAddr != ":9090" {
					t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
				}
				if cfg.ShutdownGrace != 30*time.Second {
					t.Errorf("ShutdownGrace = %v, want 30s", cfg.ShutdownGrace)
				}
				if cfg.Postgres.Port != 6543 {
					t.Errorf("Postgres.Port = %d, want 6543", cfg.Postgres.Port)
				}
				if cfg.Postgres.MaxConns != 42 {
					t.Errorf("Postgres.MaxConns = %d, want 42", cfg.Postgres.MaxConns)
				}
				if cfg.Redis.DB != 3 {
					t.Errorf("Redis.DB = %d, want 3", cfg.Redis.DB)
				}
			},
		},
		{
			name:    "missing postgres host fails",
			env:     omitEnv(requiredEnv(), "POSTGRES_HOST"),
			wantErr: true,
		},
		{
			name:    "missing postgres password fails",
			env:     omitEnv(requiredEnv(), "POSTGRES_PASSWORD"),
			wantErr: true,
		},
		{
			name:    "missing redis addr fails",
			env:     omitEnv(requiredEnv(), "REDIS_ADDR"),
			wantErr: true,
		},
		{
			name:    "invalid duration fails",
			env:     mergeEnv(requiredEnv(), map[string]string{"SHUTDOWN_GRACE": "not-a-duration"}),
			wantErr: true,
		},
		{
			name:    "invalid port fails",
			env:     mergeEnv(requiredEnv(), map[string]string{"POSTGRES_PORT": "not-a-number"}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, tt.env)

			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Load() = nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestPostgresDSN(t *testing.T) {
	tests := []struct {
		name string
		pg   PostgresConfig
		want string
	}{
		{
			name: "simple values",
			pg: PostgresConfig{
				Host: "db", Port: 5432, User: "app",
				Password: "secret", DBName: "notifications", SSLMode: "disable",
			},
			want: "host='db' port=5432 user='app' password='secret' dbname='notifications' sslmode='disable'",
		},
		{
			name: "password with space and quote is escaped",
			pg: PostgresConfig{
				Host: "db", Port: 5432, User: "app",
				Password: `p a's`, DBName: "n", SSLMode: "disable",
			},
			want: `host='db' port=5432 user='app' password='p a\'s' dbname='n' sslmode='disable'`,
		},
		{
			name: "password with backslash is escaped",
			pg: PostgresConfig{
				Host: "db", Port: 5432, User: "app",
				Password: `a\b`, DBName: "n", SSLMode: "disable",
			},
			want: `host='db' port=5432 user='app' password='a\\b' dbname='n' sslmode='disable'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pg.DSN(); got != tt.want {
				t.Errorf("DSN() = %q, want %q", got, tt.want)
			}
		})
	}
}

// setEnv makes exactly the given variables present for the duration of the
// test, and ensures every other known variable is genuinely unset. env/v11
// treats an empty variable as present-but-empty (which satisfies required), so
// absent fields must be removed, not blanked. The original environment is
// restored on cleanup.
func setEnv(t *testing.T, env map[string]string) {
	t.Helper()

	for _, k := range allKnownKeys() {
		prev, had := os.LookupEnv(k)
		k := k
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, prev)
			} else {
				_ = os.Unsetenv(k)
			}
		})

		if v, ok := env[k]; ok {
			if err := os.Setenv(k, v); err != nil {
				t.Fatalf("setenv %s: %v", k, err)
			}
		} else if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unsetenv %s: %v", k, err)
		}
	}
}

func allKnownKeys() []string {
	return []string{
		"HTTP_ADDR", "SHUTDOWN_GRACE", "LOG_LEVEL",
		"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD",
		"POSTGRES_DBNAME", "POSTGRES_SSLMODE", "POSTGRES_MAX_CONNS",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB",
	}
}

func mergeEnv(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func omitEnv(base map[string]string, key string) map[string]string {
	out := make(map[string]string, len(base))
	for k, v := range base {
		if k == key {
			continue
		}
		out[k] = v
	}
	return out
}
