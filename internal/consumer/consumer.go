// Package consumer reads pos.transactions.v1 in a consumer group with manual offset commits.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Default names from infra/docs/kafka.
const (
	DefaultGroup      = "payment-processor"
	DefaultInputTopic = "pos.transactions.v1"
)

const (
	minBackoff = 200 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// Config of the Kafka side. Empty names fall back to the defaults.
type Config struct {
	Brokers    []string
	Group      string
	InputTopic string
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

// Handler processes one record. A nil return means the offset may be committed;
// any other error is transient: the record is retried and its offset is not committed.
type Handler interface {
	Handle(ctx context.Context, rec Record) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, rec Record) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, rec Record) error { return f(ctx, rec) }

// Consumer reads, hands records to the Handler and commits.
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
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.InputTopic),
		// A new group starts from the beginning: events sent before the first start must not be skipped.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// No rebalance while a polled batch is being processed, offsets are committed first.
		kgo.BlockRebalanceOnPoll(),
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
