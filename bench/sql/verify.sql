-- Correctness checks for one run. Accepted ids come from the load generator's
-- 202 responses; sends come from the simulated provider's ledger in Redis.
\pset footer off
\echo '== status counts (all rows) =='
SELECT status, count(*) FROM notifications GROUP BY status ORDER BY status;

\echo '== accepted ids vs notifications (lost = accepted_without_row + not_terminal) =='
SELECT
  (SELECT count(*) FROM bench_accepted)                        AS responses_202,
  (SELECT count(DISTINCT id) FROM bench_accepted)              AS distinct_accepted_ids,
  count(*) FILTER (WHERE n.id IS NULL)                         AS accepted_without_row,
  count(*) FILTER (WHERE n.status = 'delivered')               AS delivered,
  count(*) FILTER (WHERE n.status = 'dead_lettered')           AS dead_lettered,
  count(*) FILTER (WHERE n.id IS NOT NULL
                   AND n.status NOT IN ('delivered', 'dead_lettered')) AS not_terminal
FROM (SELECT DISTINCT id FROM bench_accepted) a
LEFT JOIN notifications n USING (id);

\echo '== rows sharing one (client_id, idempotency_key) (must be 0) =='
SELECT count(*) AS duplicated_keys
FROM (SELECT 1 FROM notifications GROUP BY client_id, idempotency_key HAVING count(*) > 1) d;

\echo '== duplicate deliveries: provider ledger (ground truth) =='
SELECT
  count(*)                                                  AS ids_sent_at_least_once,
  count(*) FILTER (WHERE s.sends > 1)                       AS ids_sent_more_than_once,
  coalesce(sum(s.sends - 1) FILTER (WHERE s.sends > 1), 0)  AS extra_sends,
  count(*) FILTER (WHERE n.status IS DISTINCT FROM 'delivered') AS sent_but_not_marked_delivered
FROM bench_sends s LEFT JOIN notifications n USING (id);

SELECT count(*) AS delivered_without_ledger_send
FROM notifications n LEFT JOIN bench_sends s USING (id)
WHERE n.status = 'delivered' AND s.id IS NULL;

\echo '== every duplicated send, with its full attempt history (evidence for the cause) =='
SELECT s.id, s.sends AS ledger_sends, n.status, a.attempt_number, a.outcome,
       to_char(a.started_at, 'HH24:MI:SS.MS') AS started, to_char(a.finished_at, 'HH24:MI:SS.MS') AS finished
FROM bench_sends s
JOIN notifications n USING (id)
LEFT JOIN delivery_attempts a ON a.notification_id = s.id
WHERE s.sends > 1
ORDER BY s.id, a.started_at;

\echo '== duplicate deliveries visible to Postgres =='
SELECT count(*) AS ids_with_multiple_succeeded_attempts
FROM (SELECT notification_id FROM delivery_attempts WHERE outcome = 'succeeded'
      GROUP BY notification_id HAVING count(*) > 1) x;
