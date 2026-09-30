-- End-to-end delivery latency: from the row's created_at (set by Postgres at
-- INSERT, when the request was accepted) to updated_at of the delivered row
-- (the worker's final status write). Both come from the same Postgres clock.
-- percentile_disc returns values that were actually observed.
\pset footer off
\echo '== e2e latency, delivered rows, ms =='
SELECT count(*) AS n,
       round(percentile_disc(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p50,
       round(percentile_disc(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p95,
       round(percentile_disc(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p99,
       round(max(ms)::numeric, 1) AS max
FROM (SELECT extract(epoch FROM updated_at - created_at) * 1000 AS ms
      FROM notifications WHERE status = 'delivered') x;

\echo '== e2e latency by tenant, ms =='
SELECT c.name AS tenant, count(*) AS n,
       round(percentile_disc(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p50,
       round(percentile_disc(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p95,
       round(percentile_disc(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p99,
       round(max(ms)::numeric, 1) AS max
FROM (SELECT client_id, extract(epoch FROM updated_at - created_at) * 1000 AS ms
      FROM notifications WHERE status = 'delivered') x
JOIN clients c ON c.id = x.client_id
GROUP BY c.name ORDER BY c.name;

\echo '== provider call duration (delivery_attempts), ms =='
SELECT count(*) AS attempts,
       round(percentile_disc(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p50,
       round(percentile_disc(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 1) AS p99
FROM (SELECT extract(epoch FROM finished_at - started_at) * 1000 AS ms FROM delivery_attempts) x;
