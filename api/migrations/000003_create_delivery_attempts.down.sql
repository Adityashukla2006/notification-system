-- The index belongs to the table and is dropped with it. IF EXISTS so the down
-- migration is safe to apply to a database that never had the table, which is
-- how the test suite rebuilds a clean schema.
DROP TABLE IF EXISTS delivery_attempts;
