-- notifications is the source-of-truth table for every accepted request.
-- Redis coordination (queue, retry scheduler) is derived from these rows and
-- is rebuildable from this table if Redis is wiped.
--
-- id is a UUIDv7 generated application-side: time-ordered, so inserts append to
-- the right edge of every index instead of scattering across it, and known
-- before insert so the API can return it in the 202 response.
CREATE TABLE notifications (
    id              UUID        PRIMARY KEY,
    client_id       UUID        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    channel         TEXT        NOT NULL,
    recipient       TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
    attempts        INT         NOT NULL DEFAULT 0,
    -- Retry ceiling is per-notification, not a worker constant: different
    -- channels warrant different limits.
    max_attempts    INT         NOT NULL DEFAULT 5,
    -- Earliest time this notification may be delivered. Defaulting to now()
    -- makes an immediate send the no-op case while leaving room for scheduled
    -- and delayed sends without a future migration on a populated table.
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- TEXT + CHECK rather than a native ENUM: same validation, but new states
    -- can be added by altering the constraint instead of the fragile,
    -- effectively-irreversible ALTER TYPE dance.
    CONSTRAINT notifications_status_check
        CHECK (status IN ('pending', 'queued', 'delivering', 'delivered', 'failed', 'dead_lettered')),
    CONSTRAINT notifications_channel_check
        CHECK (channel IN ('email', 'sms', 'push')),

    -- The idempotency guarantee. A retried request with the same key trips this
    -- constraint instead of creating a duplicate. Named explicitly so the store
    -- layer can identify precisely which constraint fired. The constraint also
    -- creates the index that serves the conflict-time lookup by
    -- (client_id, idempotency_key).
    CONSTRAINT notifications_client_idem_key
        UNIQUE (client_id, idempotency_key)
);
