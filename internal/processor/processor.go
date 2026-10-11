package processor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/pos-term/payment-processor/internal/consumer"
)

// DeadLetterPublisher writes to the DLQ and returns after the broker acknowledged.
type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, d consumer.DeadLetter) error
}

// Processor handles one record at a time. Handle returns nil only when the record needs no
// further work, so the consumer may commit its offset. Any other error is transient: retry, do not commit.
type Processor struct {
	dlq DeadLetterPublisher
	now func() time.Time
	log *slog.Logger
}

// New builds a Processor.
func New(dlq DeadLetterPublisher, log *slog.Logger) *Processor {
	return &Processor{dlq: dlq, now: time.Now, log: log}
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
	// Placeholder until the storage and dedup steps land.
	p.log.Debug("valid transaction", "transaction_id", tx.TransactionID, "terminal_id", tx.TerminalID)
	return nil
}
