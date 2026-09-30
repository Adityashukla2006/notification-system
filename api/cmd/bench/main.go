// Command bench is the load generator and measurement harness for the
// notification system. It drives the real HTTP API, samples the real Redis
// queue, and computes every delivery statistic from the timestamps the system
// itself records in Postgres, so no number it reports is estimated.
//
// Subcommands:
//
//	setup  -tenants N              create N clients with API keys
//	reset                          empty the notification tables and queues
//	load   -stream ... [-wait]     send traffic; optionally wait for delivery
//	wait                           sample the queue until all rows are terminal
//	report                         print delivery statistics from Postgres
//
// It is a benchmarking tool and refuses to run unless BENCH_ALLOW_RESET=1,
// because reset truncates tables.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bench <setup|reset|load|wait|report> [flags]")
		os.Exit(2)
	}
	if os.Getenv("BENCH_ALLOW_RESET") != "1" {
		fmt.Fprintln(os.Stderr, "bench: refusing to run without BENCH_ALLOW_RESET=1 (it truncates tables)")
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "setup":
		err = runSetup(os.Args[2:])
	case "reset":
		err = runReset(os.Args[2:])
	case "load":
		err = runLoad(os.Args[2:])
	case "wait":
		err = runWait(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

// env holds the connections every subcommand needs.
type env struct {
	pg  *pgxpool.Pool
	rdb *redis.Client
}

func connect(ctx context.Context) (*env, error) {
	dsn := os.Getenv("BENCH_DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("BENCH_DATABASE_URL is required")
	}
	pg, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	addr := os.Getenv("BENCH_REDIS_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}
	return &env{pg: pg, rdb: redis.NewClient(&redis.Options{Addr: addr})}, nil
}

func (e *env) close() {
	e.pg.Close()
	_ = e.rdb.Close()
}

// writeJSON saves v to path, creating nothing but the file itself.
func writeJSON(path string, v any) error {
	if path == "" {
		return nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding %s: %w", path, err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// newFlags builds a flag set that exits on -h rather than returning an error.
func newFlags(name string) *flag.FlagSet {
	return flag.NewFlagSet(name, flag.ExitOnError)
}

// ms converts a duration to fractional milliseconds for reporting.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
