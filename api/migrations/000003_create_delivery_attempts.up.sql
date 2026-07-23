-- delivery_attempts is the per-attempt history behind every notification.
--
-- notifications.attempts is only a counter: it says a notification failed four
-- times, never WHY, WHEN, or how long each call took. That counter is what the
-- retry logic needs; this table is what a human debugging a stuck notification
-- needs. They answer different questions, so they are separate.
--
-- This table is append-only and observational. Nothing in the delivery path
-- reads it to make a decision — the authoritative state stays on the
-- notifications row — so a lost insert degrades visibility, never correctness.
CREATE TABLE delivery_attempts (
    id              UUID        PRIMARY KEY,
    notification_id UUID        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,

    -- Which attempt this was, 1-based. Deliberately NOT unique per
    -- notification: at-least-once delivery permits the same notification to be
    -- claimed twice, and when that happens two physical attempts genuinely
    -- occurred. A unique constraint would reject the second insert and hide the
    -- duplicate — exactly the event an operator most needs to see.
    attempt_number  INT         NOT NULL,

    outcome         TEXT        NOT NULL,

    -- The provider's error, empty on success. Kept as free text because
    -- provider failures are not a closed set we can model up front.
    error           TEXT        NOT NULL DEFAULT '',

    -- Both ends of the call are stored rather than a duration, so latency can
    -- be derived and attempts can be placed on a timeline.
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- TEXT + CHECK rather than a native ENUM, matching notifications: new
    -- outcomes can be added by altering the constraint instead of the fragile
    -- ALTER TYPE dance.
    CONSTRAINT delivery_attempts_outcome_check
        CHECK (outcome IN ('succeeded', 'failed'))
);

-- The access pattern is "show me this notification's attempts, newest first",
-- which this index serves directly. started_at DESC matches the sort so the
-- read is an index scan rather than a sort over the matched rows.
CREATE INDEX idx_delivery_attempts_notification
    ON delivery_attempts (notification_id, started_at DESC);
