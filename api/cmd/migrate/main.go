// Command migrate applies pending schema migrations.
//
// It is a separate binary, run as a deploy step, and deliberately not part of
// server or worker startup. Migrating on boot means N starting instances race
// each other through the same DDL, and a failed migration takes down every
// instance instead of failing one deploy step loudly.
//
// State is tracked in schema_migrations with the same shape golang-migrate
// uses — a single row holding the current version and a dirty flag — so the
// golang-migrate CLI can take over later without a conversion.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Adityashukla2006/notification-system/api/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

// migration is one versioned .up.sql file.
type migration struct {
	Version int64
	Name    string
	Path    string
}

func run() error {
	var (
		dir      = flag.String("dir", "migrations", "directory holding the migration files")
		status   = flag.Bool("status", false, "report the current version and pending migrations, then exit")
		baseline = flag.Int64("baseline", -1, "record this version as already applied WITHOUT running anything (for adopting a database migrated by hand)")
		force    = flag.Int64("force", -1, "clear the dirty flag and set the version, after fixing a failed migration by hand")
	)
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	if err := ensureVersionTable(ctx, pool); err != nil {
		return err
	}

	current, dirty, err := currentVersion(ctx, pool)
	if err != nil {
		return err
	}

	if *force >= 0 {
		if err := setVersion(ctx, pool, *force, false); err != nil {
			return err
		}
		fmt.Printf("forced version to %d and cleared the dirty flag\n", *force)
		return nil
	}

	migrations, err := loadMigrations(*dir)
	if err != nil {
		return err
	}

	if *baseline >= 0 {
		// Adopting a database whose schema already exists: record the version
		// without executing anything, so the next run applies only what is
		// genuinely missing.
		if err := setVersion(ctx, pool, *baseline, false); err != nil {
			return err
		}
		fmt.Printf("baselined at version %d (nothing was executed)\n", *baseline)
		return nil
	}

	if *status {
		return reportStatus(migrations, current, dirty)
	}

	if dirty {
		return fmt.Errorf("database is dirty at version %d: a previous migration failed partway. "+
			"Inspect the schema, finish or undo it by hand, then re-run with -force=<version>", current)
	}

	pending := pendingFrom(migrations, current)
	if len(pending) == 0 {
		fmt.Printf("up to date at version %d\n", current)
		return nil
	}

	for _, m := range pending {
		fmt.Printf("applying %d_%s ... ", m.Version, m.Name)
		if err := apply(ctx, pool, m); err != nil {
			fmt.Println("FAILED")
			return err
		}
		fmt.Println("ok")
	}

	fmt.Printf("migrated to version %d\n", pending[len(pending)-1].Version)
	return nil
}

// ensureVersionTable creates the tracking table if it does not exist.
func ensureVersionTable(ctx context.Context, pool *pgxpool.Pool) error {
	const q = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version BIGINT  NOT NULL PRIMARY KEY,
	dirty   BOOLEAN NOT NULL
)`
	if _, err := pool.Exec(ctx, q); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}
	return nil
}

// currentVersion reads the applied version, returning 0 for a fresh database.
func currentVersion(ctx context.Context, pool *pgxpool.Pool) (int64, bool, error) {
	var version int64
	var dirty bool
	err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading schema version: %w", err)
	}
	return version, dirty, nil
}

// execer is satisfied by both a pool and a transaction, so setVersion works
// standalone (baseline, force) and inside a migration's transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// setVersion replaces the tracked version. The table holds exactly one row.
func setVersion(ctx context.Context, q execer, version int64, dirty bool) error {
	if _, err := q.Exec(ctx, `DELETE FROM schema_migrations`); err != nil {
		return fmt.Errorf("clearing schema version: %w", err)
	}
	if _, err := q.Exec(ctx, `INSERT INTO schema_migrations (version, dirty) VALUES ($1, $2)`, version, dirty); err != nil {
		return fmt.Errorf("recording schema version: %w", err)
	}
	return nil
}

// apply runs one migration and records it, both inside a single transaction.
//
// Postgres supports transactional DDL, so a migration that fails partway leaves
// no half-applied schema — the whole file rolls back together with its version
// bump. The dirty flag still exists for the case a statement cannot run inside
// a transaction (CREATE INDEX CONCURRENTLY, for one).
func apply(ctx context.Context, pool *pgxpool.Pool, m migration) error {
	sql, err := os.ReadFile(m.Path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", m.Path, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("applying %d_%s: %w", m.Version, m.Name, err)
	}
	if err := setVersion(ctx, tx, m.Version, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing %d_%s: %w", m.Version, m.Name, err)
	}
	return nil
}

// loadMigrations reads and orders the .up.sql files.
func loadMigrations(dir string) ([]migration, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return nil, fmt.Errorf("listing migrations: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no .up.sql files found in %s", dir)
	}

	migrations := make([]migration, 0, len(entries))
	for _, path := range entries {
		base := filepath.Base(path)
		versionStr, rest, found := strings.Cut(base, "_")
		if !found {
			return nil, fmt.Errorf("migration %s is not named <version>_<name>.up.sql", base)
		}
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration %s has a non-numeric version: %w", base, err)
		}
		migrations = append(migrations, migration{
			Version: version,
			Name:    strings.TrimSuffix(rest, ".up.sql"),
			Path:    path,
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })

	// Duplicate versions mean two developers numbered migrations the same, and
	// which one ran would depend on filename ordering. Refuse rather than guess.
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version == migrations[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %d", migrations[i].Version)
		}
	}
	return migrations, nil
}

// pendingFrom returns migrations newer than the current version.
func pendingFrom(migrations []migration, current int64) []migration {
	var pending []migration
	for _, m := range migrations {
		if m.Version > current {
			pending = append(pending, m)
		}
	}
	return pending
}

// reportStatus prints the current version and what is outstanding.
func reportStatus(migrations []migration, current int64, dirty bool) error {
	fmt.Printf("current version: %d", current)
	if dirty {
		fmt.Print("  (DIRTY - a migration failed partway)")
	}
	fmt.Println()

	pending := pendingFrom(migrations, current)
	if len(pending) == 0 {
		fmt.Println("pending: none")
		return nil
	}
	fmt.Printf("pending (%d):\n", len(pending))
	for _, m := range pending {
		fmt.Printf("  %d_%s\n", m.Version, m.Name)
	}
	return nil
}
