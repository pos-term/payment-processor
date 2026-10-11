-- Marks that the result was delivered to pos.transaction-results.v1.
-- NULL means the result is stored but may not be published yet: a replay after a crash publishes it,
-- a retried duplicate of an already published transaction publishes nothing.
ALTER TABLE transaction_results ADD COLUMN published_at timestamptz;

-- Results stored before this migration were handled by an earlier release: treat them as delivered.
UPDATE transaction_results SET published_at = processed_at;
