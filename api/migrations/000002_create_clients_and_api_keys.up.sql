-- clients is the tenant table. A client's id is the client_id already
-- referenced by notifications; authentication resolves a presented API key to
-- one of these rows.
CREATE TABLE clients (
    id         UUID        PRIMARY KEY,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- api_keys holds credentials, many-to-one to clients so a client can hold
-- several keys at once (that is how rotation works: mint the new key, move
-- traffic, then revoke the old one).
--
-- Only the SHA-256 of the secret half is stored, never the secret itself: a
-- leaked table cannot be used to authenticate. A fast hash is correct here
-- because the secret is 32 bytes of CSPRNG output with no low-entropy structure
-- to brute-force, unlike a human password.
CREATE TABLE api_keys (
    -- id is the public "key id" embedded in the token; it is the indexed lookup
    -- handle and is safe to store in plaintext.
    id           UUID        PRIMARY KEY,
    client_id    UUID        NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    secret_hash  BYTEA       NOT NULL,
    name         TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Updated when the key authenticates. Nullable = never used yet.
    last_used_at TIMESTAMPTZ,
    -- Set to revoke a key without deleting its history. Checked at auth time.
    revoked_at   TIMESTAMPTZ,
    -- Not enforced yet, but the column costs nothing now and avoids a migration
    -- on a populated table once expiry lands.
    expires_at   TIMESTAMPTZ
);

-- Finding a client's keys (e.g. to list or revoke) is a common operation.
CREATE INDEX idx_api_keys_client_id ON api_keys (client_id);

-- Referential integrity: every notification must belong to a real client.
-- On a populated table you would add this NOT VALID and VALIDATE separately to
-- avoid a long lock; here the table is empty, so the simple form is fine.
ALTER TABLE notifications
    ADD CONSTRAINT fk_notifications_client
    FOREIGN KEY (client_id) REFERENCES clients(id);
