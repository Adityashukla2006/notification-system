-- Delivery throughput and drain time, from Postgres timestamps only.
--   window_s = first delivery attempt start -> last terminal status write
--   drain_*  = first / last row created     -> last terminal status write
\pset footer off
\echo '== throughput =='
WITH b AS (
  SELECT (SELECT min(started_at) FROM delivery_attempts) AS first_attempt,
         (SELECT min(created_at) FROM notifications)     AS first_created,
         (SELECT max(created_at) FROM notifications)     AS last_created,
         (SELECT max(updated_at) FROM notifications
           WHERE status IN ('delivered', 'dead_lettered')) AS last_terminal,
         (SELECT count(*) FROM notifications WHERE status = 'delivered') AS delivered,
         (SELECT count(*) FROM notifications) AS total
)
SELECT total, delivered,
       round(extract(epoch FROM last_terminal - first_attempt)::numeric, 2) AS window_s,
       round((delivered / nullif(extract(epoch FROM last_terminal - first_attempt), 0))::numeric, 1) AS delivered_per_s,
       round(extract(epoch FROM last_terminal - first_created)::numeric, 2) AS drain_from_first_created_s,
       round(extract(epoch FROM last_terminal - last_created)::numeric, 2)  AS drain_after_last_created_s
FROM b;

\echo '== delivered per 1s bucket =='
SELECT max(c) AS peak_1s, round(avg(c)::numeric, 1) AS avg_1s_over_active_seconds
FROM (SELECT date_trunc('second', updated_at) AS s, count(*) AS c FROM notifications
      WHERE status = 'delivered' GROUP BY 1) x;
