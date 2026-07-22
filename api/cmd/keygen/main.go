// Command keygen mints an API key for a client. It solves the bootstrap
// problem: the endpoint that would create keys must itself be authenticated, so
// the first key is created out-of-band with direct database access.
//
// It creates a new client (or uses an existing one via -client-id), generates a
// key, stores only the hash, and prints the raw token exactly once — after
// this it is unrecoverable.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Adityashukla2006/notification-system/api/internal/auth"
	"github.com/Adityashukla2006/notification-system/api/internal/config"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "keygen:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		name         = flag.String("name", "", "human label for the key (e.g. \"prod-worker\")")
		clientName   = flag.String("client-name", "", "name for a new client to create (mutually exclusive with -client-id)")
		clientIDFlag = flag.String("client-id", "", "existing client id to attach the key to")
	)
	flag.Parse()

	if (*clientName == "") == (*clientIDFlag == "") {
		return fmt.Errorf("provide exactly one of -client-name (new client) or -client-id (existing client)")
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	st := store.New(pool)

	clientID, err := resolveClient(ctx, st, *clientName, *clientIDFlag)
	if err != nil {
		return err
	}

	gen, err := auth.GenerateKey()
	if err != nil {
		return err
	}

	if _, err := st.CreateAPIKey(ctx, gen.KeyID, clientID, gen.SecretHash, *name); err != nil {
		return err
	}

	fmt.Printf("client id: %s\n", clientID)
	fmt.Printf("key id:    %s\n", gen.KeyID)
	fmt.Printf("\nAPI key (shown once, store it now):\n\n    %s\n\n", gen.Token)
	return nil
}

// resolveClient creates a new client or validates an existing id, returning the
// client id to attach the key to.
func resolveClient(ctx context.Context, st *store.Store, clientName, clientIDFlag string) (uuid.UUID, error) {
	if clientName != "" {
		c, err := st.CreateClient(ctx, clientName)
		if err != nil {
			return uuid.Nil, err
		}
		return c.ID, nil
	}

	id, err := uuid.Parse(clientIDFlag)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid -client-id: %w", err)
	}
	return id, nil
}
