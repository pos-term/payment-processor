package processor

import (
	"context"
	"errors"
	"fmt"
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

// Publisher writes to Kafka and returns after the broker acknowledged.
type Publisher interface {
	PublishResult(ctx context.Context, r consumer.Result) error
	PublishDeadLetter(ctx context.Context, d consumer.DeadLetter) error
}

// Store is the Postgres source of truth.
type Store interface {
	InsertTransaction(ctx context.Context, tx storage.Transaction) (bool, error)
	SaveResult(ctx context.Context, id, status string, reason *string, processedAt time.Time) error
	GetResult(ctx context.Context, id string) (storage.Result, bool, error)
	MarkPublished(ctx context.Context, id string) error
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
	pub            Publisher
	maxAmountMinor int64
	now            func() time.Time
	log            *slog.Logger
}

// New builds a Processor. Payments above maxAmountMinor fail with LIMIT_EXCEEDED, 0 disables the limit.
func New(store Store, dedup Deduper, pub Publisher, maxAmountMinor int64, log *slog.Logger) *Processor {
	return &Processor{store: store, dedup: dedup, pub: pub, maxAmountMinor: maxAmountMinor, now: time.Now, log: log}
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
		return p.pub.PublishDeadLetter(ctx, consumer.DeadLetter{
			Record: rec, Kind: bad.Kind, Detail: bad.Detail, TransactionID: bad.TransactionID, At: p.now().UTC(),
		})
	}
	log := p.log.With("transaction_id", tx.TransactionID, "terminal_id", tx.TerminalID, "trace_id", rec.Headers["trace_id"])

	// Level 1, Redis: a cheap "have we seen this id" check. It is only a hint: Redis may be down,
	// flushed, or hold an id whose processing never finished. Postgres below decides.
	first, err := p.dedup.FirstSeen(ctx, tx.TransactionID)
	if err != nil {
		log.Warn("dedup filter unavailable, falling back to postgres", "error", err)
		first = true
	}
	trace := rec.Headers["trace_id"]
	if !first {
		// Seen before. A stored result means the payment is decided: never decide it again, only make
		// sure the terminal got it (deliver skips a result that is already recorded as published).
		// Without a stored result the earlier attempt died half way: finish the job below.
		if res, ok, err := p.store.GetResult(ctx, tx.TransactionID); err != nil {
			return err
		} else if ok {
			return p.deliver(ctx, log, res, trace)
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
	res, ok, err := p.store.GetResult(ctx, tx.TransactionID)
	if err != nil {
		return err
	}
	if !ok {
		status, reason := p.decide(tx)
		if err := p.store.SaveResult(ctx, tx.TransactionID, status, reason, p.now().UTC()); err != nil {
			return err
		}
		// Read it back: when two writers race, the stored result is the one that counts,
		// and a republish after a crash must carry exactly the same data.
		if res, ok, err = p.store.GetResult(ctx, tx.TransactionID); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("result of %s vanished after save", tx.TransactionID)
		}
	}
	return p.deliver(ctx, log, res, trace)
}

// deliver makes sure the terminal gets the stored result, and records that it did.
//
// The order is the whole safety story: publish (acknowledged by the broker), then mark in Postgres,
// then the caller commits the offset. A crash or an error at any point leaves the offset uncommitted,
// so the record comes back and the next attempt starts from the stored result:
//   - before the publish: the result is published now;
//   - between the publish and the mark: the result is published once more (at-least-once, the
//     terminal ignores a transaction_id it has already handled);
//   - after the mark: nothing is published, so a retried duplicate yields exactly one result.
func (p *Processor) deliver(ctx context.Context, log *slog.Logger, res storage.Result, traceID string) error {
	if res.Published {
		log.Info("duplicate, result already published")
		return nil
	}
	if err := p.pub.PublishResult(ctx, consumer.Result{
		TransactionID: res.TransactionID, TerminalID: res.TerminalID, Status: res.Status,
		Reason: res.Reason, ProcessedAt: res.ProcessedAt, TraceID: traceID,
	}); err != nil {
		return err
	}
	if err := p.store.MarkPublished(ctx, res.TransactionID); err != nil {
		return err
	}
	log.Info("transaction processed", "status", res.Status)
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
