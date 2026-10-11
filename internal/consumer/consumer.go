// Package consumer reads pos.transactions.v1 in a consumer group with manual offset commits.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Default names from infra/docs/kafka.
const (
	DefaultGroup        = "payment-processor"
	DefaultInputTopic   = "pos.transactions.v1"
	DefaultDLQTopic     = "pos.transactions.dlq.v1"
	DefaultResultsTopic = "pos.transaction-results.v1"
)

const (
	eventTypeProcessed = "transaction.processed"
	timeLayout         = "2006-01-02T15:04:05.000Z" // RFC 3339, UTC, milliseconds
	minBackoff         = 200 * time.Millisecond
	maxBackoff         = 5 * time.Second
)

// Config of the Kafka side. Empty names fall back to the defaults.
type Config struct {
	Brokers    []string
	Group      string
	InputTopic string
	DLQTopic   string
	// ResultsTopic receives the final result of every payment.
	ResultsTopic string
}

// Record is one consumed Kafka message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Result is the final outcome of a payment, published to the results topic for the terminal.
type Result struct {
	TransactionID string
	TerminalID    string
	Status        string  // success or failed
	Reason        *string // nil on success
	ProcessedAt   time.Time
	TraceID       string // copied from the input message, empty when it had none
}

// DeadLetter is a message that can never be processed, whatever the number of retries.
type DeadLetter struct {
	Record        Record
	Kind          string // value of the `error` header: invalid_json, validation_failed, ...
	Detail        string
	TransactionID string // best effort, empty when unreadable: the DLQ key
	At            time.Time
}

// Handler processes one record. A nil return means the offset may be committed;
// any other error is transient: the record is retried and its offset is not committed.
type Handler interface {
	Handle(ctx context.Context, rec Record) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, rec Record) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, rec Record) error { return f(ctx, rec) }

// Consumer reads, hands records to the Handler and commits. It also produces results and dead letters.
type Consumer struct {
	cfg    Config
	client *kgo.Client
	log    *slog.Logger
}

// New creates the Kafka client. It does not connect until Run.
func New(cfg Config, log *slog.Logger) (*Consumer, error) {
	if cfg.Group == "" {
		cfg.Group = DefaultGroup
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = DefaultInputTopic
	}
	if cfg.ResultsTopic == "" {
		cfg.ResultsTopic = DefaultResultsTopic
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = DefaultDLQTopic
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.InputTopic),
		// A new group starts from the beginning: events sent before the first start must not be skipped.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// No rebalance while a polled batch is being processed, offsets are committed first.
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Consumer{cfg: cfg, client: client, log: log}, nil
}

// Close leaves the group and releases connections.
func (c *Consumer) Close() { c.client.Close() }

// Run consumes until ctx is cancelled. Offsets are committed only for records the handler finished.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	c.log.Info("consumer started", "group", c.cfg.Group, "topic", c.cfg.InputTopic)
	// Leaving the group waits for rebalances to be allowed: never exit with them blocked.
	defer c.client.AllowRebalance()
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Error("fetch failed", "topic", topic, "partition", partition, "error", err)
		})

		done := map[int32]*kgo.Record{} // last finished record per partition
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			if err := c.handle(ctx, h, r); err != nil {
				return // only on shutdown
			}
			done[r.Partition] = r
		})
		c.commit(done)
		c.client.AllowRebalance()
	}
}

// handle retries transient failures until the record is done or ctx ends. The offset is never skipped.
func (c *Consumer) handle(ctx context.Context, h Handler, r *kgo.Record) error {
	rec := Record{
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
		Headers: map[string]string{},
	}
	for _, hd := range r.Headers {
		rec.Headers[hd.Key] = string(hd.Value)
	}
	backoff := minBackoff
	for {
		err := h.Handle(ctx, rec)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.Error("processing failed, will retry", "partition", r.Partition, "offset", r.Offset, "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (c *Consumer) commit(done map[int32]*kgo.Record) {
	if len(done) == 0 {
		return
	}
	recs := make([]*kgo.Record, 0, len(done))
	for _, r := range done {
		recs = append(recs, r)
	}
	// Not the run context: finished work is committed even while shutting down.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		// Harmless once records are deduplicated: they are replayed.
		c.log.Warn("offset commit failed", "error", err)
	}
}

// PublishDeadLetter writes the original bytes to the DLQ topic and returns after the broker acknowledged.
// The key is the transaction_id when it could be read, the headers say what was wrong and where it came from.
func (c *Consumer) PublishDeadLetter(ctx context.Context, d DeadLetter) error {
	headers := []kgo.RecordHeader{
		{Key: "error", Value: []byte(d.Kind)},
		{Key: "error_detail", Value: []byte(d.Detail)},
		{Key: "original_topic", Value: []byte(d.Record.Topic)},
		{Key: "original_partition", Value: []byte(strconv.Itoa(int(d.Record.Partition)))},
		{Key: "original_offset", Value: []byte(strconv.FormatInt(d.Record.Offset, 10))},
		{Key: "failed_at", Value: []byte(d.At.UTC().Format(timeLayout))},
	}
	if trace := d.Record.Headers["trace_id"]; trace != "" {
		headers = append(headers, kgo.RecordHeader{Key: "trace_id", Value: []byte(trace)})
	}
	var key []byte
	if d.TransactionID != "" {
		key = []byte(d.TransactionID)
	}
	return c.produce(ctx, &kgo.Record{Topic: c.cfg.DLQTopic, Key: key, Value: d.Record.Value, Headers: headers})
}

// resultMessage is the pos.transaction-results.v1 value, see infra/schemas.
type resultMessage struct {
	SchemaVersion int     `json:"schema_version"`
	TransactionID string  `json:"transaction_id"`
	TerminalID    string  `json:"terminal_id"`
	Status        string  `json:"status"`
	Reason        *string `json:"reason"` // null on success
	ProcessedAt   string  `json:"processed_at"`
}

// PublishResult writes the result keyed by terminal_id, so a terminal finds its own results and their
// order is kept. It returns after the broker acknowledged: only then may the input offset be committed.
func (c *Consumer) PublishResult(ctx context.Context, r Result) error {
	value, err := json.Marshal(resultMessage{
		SchemaVersion: 1, TransactionID: r.TransactionID, TerminalID: r.TerminalID,
		Status: r.Status, Reason: r.Reason, ProcessedAt: r.ProcessedAt.UTC().Format(timeLayout),
	})
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	headers := []kgo.RecordHeader{{Key: "event_type", Value: []byte(eventTypeProcessed)}}
	if r.TraceID != "" {
		headers = append(headers, kgo.RecordHeader{Key: "trace_id", Value: []byte(r.TraceID)})
	}
	return c.produce(ctx, &kgo.Record{Topic: c.cfg.ResultsTopic, Key: []byte(r.TerminalID), Value: value, Headers: headers})
}

func (c *Consumer) produce(ctx context.Context, r *kgo.Record) error {
	if err := c.client.ProduceSync(ctx, r).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", r.Topic, err)
	}
	return nil
}
