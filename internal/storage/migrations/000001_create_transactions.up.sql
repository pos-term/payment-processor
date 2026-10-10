-- Payments as submitted by terminals (pos.transactions.v1).
-- A row is immutable. The primary key is the last line of defence against duplicates:
-- Redis filters retries first, Postgres is the source of truth.
CREATE TABLE transactions (
    transaction_id uuid        PRIMARY KEY,
    terminal_id    text        NOT NULL CHECK (terminal_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),
    currency       char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- Per-terminal event counter. Not unique: a restarted terminal may reset it.
    seq            bigint      NOT NULL CHECK (seq >= 0),
    -- Terminal clock, UTC.
    created_at     timestamptz NOT NULL,
    received_at    timestamptz NOT NULL DEFAULT now()
);

-- History per terminal, newest first. The REST API orders by transaction_id:
-- UUIDv7 sorts by creation time, and the cursor is built from it.
-- Without the terminal filter the primary key serves the same order.
CREATE INDEX transactions_terminal_id_transaction_id_idx
    ON transactions (terminal_id, transaction_id DESC);
