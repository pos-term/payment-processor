package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Transaction is a payment as submitted by a terminal.
type Transaction struct {
	ID          string
	TerminalID  string
	AmountMinor int64
	Currency    string
	Seq         int64
	CreatedAt   time.Time
}

// Result is the stored outcome of a transaction.
type Result struct {
	TransactionID string
	TerminalID    string
	Status        string
	Reason        *string // nil on success
	ProcessedAt   time.Time
}

// Store is the Postgres-backed source of truth for transactions and their results.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases all connections.
func (s *Store) Close() { s.pool.Close() }

// InsertTransaction stores the payment as pending (a transaction without a result).
// It reports false when the transaction_id is already known: the primary key is the final dedup.
func (s *Store) InsertTransaction(ctx context.Context, tx Transaction) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO transactions (transaction_id, terminal_id, amount_minor, currency, seq, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (transaction_id) DO NOTHING`,
		tx.ID, tx.TerminalID, tx.AmountMinor, tx.Currency, tx.Seq, tx.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("insert transaction: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// SaveResult stores the final result. The first result wins, later calls are no-ops.
func (s *Store) SaveResult(ctx context.Context, id, status string, reason *string, processedAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO transaction_results (transaction_id, status, reason, processed_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (transaction_id) DO NOTHING`,
		id, status, reason, processedAt)
	if err != nil {
		return fmt.Errorf("save result: %w", err)
	}
	return nil
}

// GetResult returns the stored result, or false while the transaction is still pending or unknown.
func (s *Store) GetResult(ctx context.Context, id string) (Result, bool, error) {
	var r Result
	err := s.pool.QueryRow(ctx,
		`SELECT r.transaction_id::text, t.terminal_id, r.status, r.reason, r.processed_at
		 FROM transaction_results r JOIN transactions t USING (transaction_id)
		 WHERE r.transaction_id = $1`, id).
		Scan(&r.TransactionID, &r.TerminalID, &r.Status, &r.Reason, &r.ProcessedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, fmt.Errorf("get result: %w", err)
	}
	r.ProcessedAt = r.ProcessedAt.UTC()
	return r, true, nil
}
