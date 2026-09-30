-- Idempotency: DUPS concurrent identical requests sharing one key.
\pset footer off
\echo '== rows per idempotency key (must be exactly 1) =='
SELECT idempotency_key, count(*) AS rows, min(id::text) AS id, min(status) AS status
FROM notifications GROUP BY idempotency_key;

\echo '== total rows in notifications =='
SELECT count(*) AS total_rows FROM notifications;

\echo '== ids returned in 202 responses =='
SELECT count(*) AS responses_202, count(DISTINCT id) AS distinct_ids FROM bench_accepted;

\echo '== delivery attempts and provider-ledger sends for that row =='
SELECT (SELECT count(*) FROM delivery_attempts) AS attempts,
       (SELECT coalesce(sum(sends), 0) FROM bench_sends) AS ledger_sends;
