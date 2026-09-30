-- Retry and dead-letter accounting for the provider-failure test.
\pset footer off
\echo '== outcome of every notification =='
SELECT count(*) AS total,
       count(*) FILTER (WHERE status = 'delivered') AS delivered,
       count(*) FILTER (WHERE status = 'dead_lettered') AS dead_lettered,
       count(*) FILTER (WHERE status NOT IN ('delivered', 'dead_lettered')) AS not_terminal,
       round(100.0 * count(*) FILTER (WHERE status = 'delivered') / nullif(count(*), 0), 3) AS pct_delivered,
       round(100.0 * count(*) FILTER (WHERE status = 'dead_lettered') / nullif(count(*), 0), 3) AS pct_dead_lettered
FROM notifications;

\echo '== attempts =='
SELECT count(*) AS attempts,
       count(*) FILTER (WHERE outcome = 'failed') AS failed_attempts,
       round(100.0 * count(*) FILTER (WHERE outcome = 'failed') / nullif(count(*), 0), 2) AS observed_fail_pct
FROM delivery_attempts;

\echo '== delivered notifications by attempts needed =='
SELECT n_attempts, count(*) FROM (
  SELECT n.id, count(a.*) AS n_attempts FROM notifications n
  JOIN delivery_attempts a ON a.notification_id = n.id
  WHERE n.status = 'delivered' GROUP BY n.id) x
GROUP BY n_attempts ORDER BY n_attempts;

\echo '== dead-lettered rows by attempts recorded =='
SELECT attempts, max_attempts, count(*) FROM notifications
WHERE status = 'dead_lettered' GROUP BY 1, 2 ORDER BY 1;
