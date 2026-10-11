package processor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/storage"
)

// Result statuses, as stored. A transaction without a result is pending.
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

// ReasonLimitExceeded is the reason code of a payment above the configured limit.
const ReasonLimitExceeded = "LIMIT_EXCEEDED"

// DeadLetterPublisher writes to the DLQ and returns after the broker acknowledged.
type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, d consumer.DeadLetter) error
}

// Store is the Postgres source of truth.
type Store interface {
	InsertTransaction(ctx context.Context, tx storage.Transaction) (bool, error)
	SaveResult(ctx context.Context, id, status string, reason *string, processedAt time.Time) error
	GetResult(ctx context.Context, id string) (storage.Result, bool, error)
}

// Deduper is the fast duplicate filter in front of Postgres. It only ever saves work:
// a wrong or missing answer must neither lose nor double-process a payment.
type Deduper interface {
	// FirstSeen reports whether this is the first time the id is seen within the filter's TTL.
	FirstSeen(ctx context.Context, transactionID string) (bool, error)
}

// Processor handles one record at a time. Handle returns nil only when the record needs no
// further work, so the consumer may commit its offset. Any other error is transient: retry, do not commit.
type Processor struct {
	store          Store
	dedup          Deduper
	dlq            DeadLetterPublisher
	maxAmountMinor int64
	now            func() time.Time
	log            *slog.Logger
}

// New builds a Processor. Payments above maxAmountMinor fail with LIMIT_EXCEEDED, 0 disables the limit.
func New(store Store, dedup Deduper, dlq DeadLetterPublisher, maxAmountMinor int64, log *slog.Logger) *Processor {
	return &Processor{store: store, dedup: dedup, dlq: dlq, maxAmountMinor: maxAmountMinor, now: time.Now, log: log}
}

// Handle processes a pos.transactions.v1 record.
func (p *Processor) Handle(ctx context.Context, rec consumer.Record) error {
	tx, err := Parse(rec.Value)
	if err != nil {
		var bad *BadMessageError
		if !errors.As(err, &bad) {
			return err
		}
		// A retry cannot fix this message: park it for a manual look, so the partition keeps moving.
		p.log.Warn("bad message, sending to DLQ", "kind", bad.Kind, "detail", bad.Detail,
			"partition", rec.Partition, "offset", rec.Offset)
		return p.dlq.PublishDeadLetter(ctx, consumer.DeadLetter{
			Record: rec, Kind: bad.Kind, Detail: bad.Detail, TransactionID: bad.TransactionID, At: p.now().UTC(),
		})
	}
	log := p.log.With("transaction_id", tx.TransactionID, "terminal_id", tx.TerminalID, "trace_id", rec.Headers["trace_id"])

	// Level 1, Redis
	first, err := p.dedup.FirstSeen(ctx, tx.TransactionID)
	if err != nil {
		log.Warn("dedup filter unavailable, falling back to postgres", "error", err)
		first = true
	}
	if !first {
		// Seen before, but only a stored result proves the earlier attempt finished.
		// Without one the attempt died half way, so this record must finish the job.
		if _, ok, err := p.store.GetResult(ctx, tx.TransactionID); err != nil {
			return err
		} else if ok {
			log.Info("duplicate, already processed")
			return nil
		}
	}

	// Level 2, Postgres: the primary key is the source of truth.
	// pending: the transaction is stored, no result yet. A crash from here on leaves it pending;
	// the replay of this record completes it.
	if _, err := p.store.InsertTransaction(ctx, storage.Transaction{
		ID: tx.TransactionID, TerminalID: tx.TerminalID, AmountMinor: tx.AmountMinor,
		Currency: tx.Currency, Seq: tx.Seq, CreatedAt: tx.CreatedAt,
	}); err != nil {
		return err
	}
	if _, ok, err := p.store.GetResult(ctx, tx.TransactionID); err != nil {
		return err
	} else if ok {
		log.Info("transaction already has a result")
		return nil
	}

	status, reason := p.decide(tx)
	if err := p.store.SaveResult(ctx, tx.TransactionID, status, reason, p.now().UTC()); err != nil {
		return err
	}
	log.Info("transaction processed", "status", status)
	return nil
}

// decide is the business rule of the prototype: one amount limit.
func (p *Processor) decide(tx Transaction) (string, *string) {
	if p.maxAmountMinor > 0 && tx.AmountMinor > p.maxAmountMinor {
		reason := ReasonLimitExceeded
		return StatusFailed, &reason
	}
	return StatusSuccess, nil
}
