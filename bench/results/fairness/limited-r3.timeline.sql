-- Fairness "limited" rep 1, by 5-second bucket of acceptance time.
-- noisy = bench-tenant-0 (200/s offered), quiet = bench-tenant-1 (20/s offered).
SELECT (floor(extract(epoch FROM n.created_at - t0.t) / 5) * 5)::int AS sec,
       count(*) FILTER (WHERE c.name = 'bench-tenant-0') AS noisy_accepted,
       count(*) FILTER (WHERE c.name = 'bench-tenant-1') AS quiet_accepted,
       round(percentile_disc(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM n.updated_at - n.created_at) * 1000)
             FILTER (WHERE c.name = 'bench-tenant-1')::numeric) AS quiet_p50_ms,
       round(max(extract(epoch FROM n.updated_at - n.created_at) * 1000)
             FILTER (WHERE c.name = 'bench-tenant-1')::numeric) AS quiet_max_ms
FROM notifications n JOIN clients c ON c.id = n.client_id,
     (SELECT min(created_at) AS t FROM notifications) t0
GROUP BY 1 ORDER BY 1;
