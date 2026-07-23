-- Supports the list endpoint's access pattern: one client's notifications,
-- newest first, walked by cursor.
--
-- (client_id, id DESC) matches both the filter and the sort, so a page is a
-- range scan that stops after LIMIT rows instead of sorting the client's entire
-- history to return twenty of them.
--
-- id works as the sort key because it is a UUIDv7: the timestamp is the leading
-- component, so id order is creation order. That is what lets the cursor be a
-- single opaque id rather than a (created_at, id) tuple.
--
-- status and channel are deliberately NOT in this index. They are low
-- cardinality — a handful of distinct values — so a composite index on them
-- buys little, and Postgres can filter the scanned range cheaply. Add a
-- partial index later if one specific filter proves hot.
CREATE INDEX idx_notifications_client_id_desc
    ON notifications (client_id, id DESC);
