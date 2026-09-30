-- Harness-only tables, loaded after each run from the load generator and the
-- simulated provider's ledger. They are not part of the application schema.
CREATE TABLE IF NOT EXISTS bench_accepted (id uuid NOT NULL);                     -- one row per 202 response
CREATE TABLE IF NOT EXISTS bench_sends (id uuid PRIMARY KEY, sends int NOT NULL); -- ledger: successful simulated sends
