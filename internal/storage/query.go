package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Status values as the REST API reports them. Unlike Kafka it also has Pending,
// which is derived: a transaction row without a result row.
const (
	StatusPending = "pending"
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

// TransactionView is a transaction joined with its result, as the REST API shows it.
type TransactionView struct {
	ID          string
	TerminalID  string
	AmountMinor int64
	Currency    string
	Status      string
	Reason      *string    // nil unless Status is failed
	CreatedAt   time.Time  // terminal time, UTC
	ProcessedAt *time.Time // nil while pending
}

// ListFilter selects a page of history. Zero fields mean "no filter".
type ListFilter struct {
	TerminalID string
	Status     string
	From       time.Time // created_at >= From
	To         time.Time // created_at < To
	// After is the transaction_id of the last item of the previous page.
	After string
	Limit int
}

// Terminal is a terminal that has sent at least one transaction.
type Terminal struct {
	ID                string
	LastTransactionAt time.Time
}

const viewColumns = `t.transaction_id::text, t.terminal_id, t.amount_minor, t.currency::text,
	COALESCE(r.status, 'pending'), r.reason, t.created_at, r.processed_at`

func scanView(row pgx.Row) (TransactionView, error) {
	var v TransactionView
	if err := row.Scan(&v.ID, &v.TerminalID, &v.AmountMinor, &v.Currency, &v.Status, &v.Reason, &v.CreatedAt, &v.ProcessedAt); err != nil {
		return TransactionView{}, err
	}
	v.CreatedAt = v.CreatedAt.UTC()
	if v.ProcessedAt != nil {
		u := v.ProcessedAt.UTC()
		v.ProcessedAt = &u
	}
	return v, nil
}

// GetTransaction returns one transaction with its status, or false if the id is unknown.
func (s *Store) GetTransaction(ctx context.Context, id string) (TransactionView, bool, error) {
	v, err := scanView(s.pool.QueryRow(ctx,
		`SELECT `+viewColumns+`
		 FROM transactions t LEFT JOIN transaction_results r USING (transaction_id)
		 WHERE t.transaction_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return TransactionView{}, false, nil
	}
	if err != nil {
		return TransactionView{}, false, fmt.Errorf("get transaction: %w", err)
	}
	return v, true, nil
}

// ListTransactions returns up to f.Limit transactions, newest first by transaction_id
// (UUIDv7, so creation order). The caller asks for one extra row to learn whether a next page exists.
func (s *Store) ListTransactions(ctx context.Context, f ListFilter) ([]TransactionView, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, arg any) {
		args = append(args, arg)
		where = append(where, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(args))))
	}
	if f.TerminalID != "" {
		add("t.terminal_id = ?", f.TerminalID)
	}
	switch f.Status {
	case "":
	case StatusPending:
		where = append(where, "r.transaction_id IS NULL")
	default:
		add("r.status = ?", f.Status)
	}
	if !f.From.IsZero() {
		add("t.created_at >= ?", f.From)
	}
	if !f.To.IsZero() {
		add("t.created_at < ?", f.To)
	}
	if f.After != "" {
		add("t.transaction_id < ?", f.After)
	}

	q := `SELECT ` + viewColumns + `
		FROM transactions t LEFT JOIN transaction_results r USING (transaction_id)`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit)
	q += fmt.Sprintf(" ORDER BY t.transaction_id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list transactions: %w", err)
	}
	defer rows.Close()

	var out []TransactionView
	for rows.Next() {
		v, err := scanView(rows)
		if err != nil {
			return nil, fmt.Errorf("scan transaction: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list transactions: %w", err)
	}
	return out, nil
}

// ListTerminals returns every terminal that has sent a transaction, most recently active first.
func (s *Store) ListTerminals(ctx context.Context) ([]Terminal, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT terminal_id, max(created_at) AS last_at
		 FROM transactions GROUP BY terminal_id ORDER BY last_at DESC, terminal_id`)
	if err != nil {
		return nil, fmt.Errorf("list terminals: %w", err)
	}
	defer rows.Close()

	var out []Terminal
	for rows.Next() {
		var t Terminal
		if err := rows.Scan(&t.ID, &t.LastTransactionAt); err != nil {
			return nil, fmt.Errorf("scan terminal: %w", err)
		}
		t.LastTransactionAt = t.LastTransactionAt.UTC()
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list terminals: %w", err)
	}
	return out, nil
}
