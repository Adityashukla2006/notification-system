package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Adityashukla2006/notification-system/api/internal/auth"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// tenant is one benchmark client and its bearer token.
type tenant struct {
	Name     string `json:"name"`
	ClientID string `json:"client_id"`
	Token    string `json:"token"`
}

// runSetup creates tenants through the same store functions keygen uses, so the
// keys are indistinguishable from production ones.
func runSetup(args []string) error {
	fs := newFlags("setup")
	n := fs.Int("tenants", 3, "number of tenants to create")
	out := fs.String("out", "/results/keys.json", "where to write the tenant keys")
	_ = fs.Parse(args)

	ctx := context.Background()
	e, err := connect(ctx)
	if err != nil {
		return err
	}
	defer e.close()

	st := store.New(e.pg)
	tenants := make([]tenant, 0, *n)
	for i := range *n {
		name := fmt.Sprintf("bench-tenant-%d", i)
		c, err := st.CreateClient(ctx, name)
		if err != nil {
			return fmt.Errorf("creating client: %w", err)
		}
		gen, err := auth.GenerateKey()
		if err != nil {
			return err
		}
		if _, err := st.CreateAPIKey(ctx, gen.KeyID, c.ID, gen.SecretHash, name); err != nil {
			return fmt.Errorf("creating api key: %w", err)
		}
		tenants = append(tenants, tenant{Name: name, ClientID: c.ID.String(), Token: gen.Token})
	}
	fmt.Fprintf(os.Stderr, "created %d tenants -> %s\n", len(tenants), *out)
	return writeJSON(*out, tenants)
}

// runReset empties the notification tables and every queue structure, leaving
// clients and API keys intact, so each scenario starts from the same state.
func runReset(args []string) error {
	fs := newFlags("reset")
	_ = fs.Parse(args)

	ctx := context.Background()
	e, err := connect(ctx)
	if err != nil {
		return err
	}
	defer e.close()

	if _, err := e.pg.Exec(ctx, `TRUNCATE delivery_attempts, notifications`); err != nil {
		return fmt.Errorf("truncating: %w", err)
	}
	// VACUUM ANALYZE so a scenario is not measured against the dead tuples and
	// stale statistics the previous one left behind.
	if _, err := e.pg.Exec(ctx, `VACUUM ANALYZE notifications, delivery_attempts, api_keys`); err != nil {
		return fmt.Errorf("vacuuming: %w", err)
	}

	for _, pattern := range []string{"notifications:queue", "notifications:q:*", "notifications:tenants:*", "notifications:doorbell", "notifications:scheduled", "notifications:processing:*", "ratelimit:*"} {
		iter := e.rdb.Scan(ctx, 0, pattern, 1000).Iterator()
		for iter.Next(ctx) {
			if err := e.rdb.Del(ctx, iter.Val()).Err(); err != nil {
				return fmt.Errorf("deleting %s: %w", iter.Val(), err)
			}
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("scanning %s: %w", pattern, err)
		}
	}
	fmt.Fprintln(os.Stderr, "reset complete")
	return nil
}
