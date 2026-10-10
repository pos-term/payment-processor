-- Final processing result (pos.transaction-results.v1).
-- No row means the transaction is still pending: pending is derived, never stored.
-- The primary key allows exactly one result per transaction.
CREATE TABLE transaction_results (
    transaction_id uuid        PRIMARY KEY REFERENCES transactions (transaction_id),
    status         text        NOT NULL CHECK (status IN ('success', 'failed')),
    -- UPPER_SNAKE_CASE code, NULL on success. Plain text, not an enum:
    -- the list of codes is not fixed yet and altering a Postgres enum is painful.
    reason         text        CHECK (reason ~ '^[A-Z][A-Z0-9_]*$'),
    processed_at   timestamptz NOT NULL,
    CHECK ((status = 'success') = (reason IS NULL))
);
