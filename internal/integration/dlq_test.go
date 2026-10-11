// Package integration runs the pipeline against a real Kafka. The tests are skipped unless
// TEST_KAFKA_BROKERS is set; the infra compose stack provides the broker (localhost:9094).
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/processor"
	"github.com/pos-term/payment-processor/internal/storage"
)

const validTx = `{"schema_version":1,"transaction_id":"0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10","terminal_id":"term-001",` +
	`"amount_minor":15050,"currency":"RUB","created_at":"2026-09-19T10:15:30.123Z","seq":42}`

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// An invalid message lands in the DLQ with its payload and reason, and the partition keeps moving.
func TestBadMessagesReachDLQAndPartitionKeepsMoving(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set; start the infra compose stack to run this test")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := consumer.Config{
		Brokers: strings.Split(brokers, ","), Group: "it-" + suffix,
		InputTopic: "it.in." + suffix, DLQTopic: "it.dlq." + suffix, ResultsTopic: "it.out." + suffix,
	}
	ctx := context.Background()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for topic, parts := range map[string]int32{cfg.InputTopic: 3, cfg.DLQTopic: 1, cfg.ResultsTopic: 1} {
		if _, err := adm.CreateTopic(ctx, parts, 1, nil, topic); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _, _ = adm.DeleteTopics(ctx, cfg.InputTopic, cfg.DLQTopic, cfg.ResultsTopic) }()

	// One terminal, one partition: the order is fixed. The valid message comes last, behind three bad ones.
	trace := []kgo.RecordHeader{{Key: "trace_id", Value: []byte("trace-abc")}}
	for _, v := range []string{
		"{broken",
		strings.Replace(validTx, `"schema_version":1`, `"schema_version":9`, 1),
		strings.Replace(validTx, `"amount_minor":15050`, `"amount_minor":0`, 1),
		validTx,
	} {
		r := &kgo.Record{Topic: cfg.InputTopic, Key: []byte("term-001"), Value: []byte(v), Headers: trace}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cons, err := consumer.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	proc := processor.New(newMemStore(), noDedup{}, cons, 0, log)
	var mu sync.Mutex
	handled := 0
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cons.Run(runCtx, consumer.HandlerFunc(func(c context.Context, rec consumer.Record) error {
			err := proc.Handle(c, rec)
			if err == nil {
				mu.Lock()
				handled++
				mu.Unlock()
			}
			return err
		}))
	}()
	defer func() { cancel(); <-done; cons.Close() }()

	// All four records are finished, including the valid one behind the bad ones.
	deadline := time.Now().Add(60 * time.Second)
	for {
		mu.Lock()
		n := handled
		mu.Unlock()
		if n == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 4 records were handled: the partition is stuck", n)
		}
		time.Sleep(50 * time.Millisecond)
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.DLQTopic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var dlq []*kgo.Record
	for end := time.Now().Add(30 * time.Second); len(dlq) < 3 && time.Now().Before(end); {
		c, cancel := context.WithTimeout(ctx, time.Second)
		reader.PollFetches(c).EachRecord(func(r *kgo.Record) { dlq = append(dlq, r) })
		cancel()
	}
	if len(dlq) != 3 {
		t.Fatalf("dead letters = %d, want 3 (the valid message must not be there)", len(dlq))
	}

	wantErr := []string{"invalid_json", "unsupported_schema_version", "validation_failed"}
	for i, r := range dlq {
		if got := header(r, "error"); got != wantErr[i] {
			t.Errorf("dlq[%d] error = %q, want %q", i, got, wantErr[i])
		}
		if header(r, "original_topic") != cfg.InputTopic || header(r, "original_offset") == "" ||
			header(r, "original_partition") == "" || header(r, "failed_at") == "" || header(r, "error_detail") == "" {
			t.Errorf("dlq[%d] headers incomplete: %v", i, r.Headers)
		}
		if header(r, "trace_id") != "trace-abc" {
			t.Errorf("dlq[%d] trace_id = %q, want trace-abc", i, header(r, "trace_id"))
		}
	}
	if string(dlq[0].Value) != "{broken" || dlq[0].Key != nil {
		t.Errorf("dlq[0] must keep the payload and have no key: key=%q value=%q", dlq[0].Key, dlq[0].Value)
	}
	if string(dlq[1].Key) != "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10" {
		t.Errorf("dlq[1] key = %q, want the transaction_id", dlq[1].Key)
	}
}

// memStore stands in for Postgres: this test is about the DLQ, not about storage.
type memStore struct {
	mu      sync.Mutex
	txs     map[string]storage.Transaction
	results map[string]storage.Result
}

func newMemStore() *memStore {
	return &memStore{txs: map[string]storage.Transaction{}, results: map[string]storage.Result{}}
}

func (s *memStore) InsertTransaction(_ context.Context, tx storage.Transaction) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, known := s.txs[tx.ID]
	s.txs[tx.ID] = tx
	return !known, nil
}

func (s *memStore) SaveResult(_ context.Context, id, status string, reason *string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[id] = storage.Result{TransactionID: id, TerminalID: s.txs[id].TerminalID, Status: status, Reason: reason, ProcessedAt: at}
	return nil
}

func (s *memStore) MarkPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.results[id]; ok {
		r.Published = true
		s.results[id] = r
	}
	return nil
}

func (s *memStore) GetResult(_ context.Context, id string) (storage.Result, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[id]
	return r, ok, nil
}

// noDedup says every id is new: Postgres alone decides.
type noDedup struct{}

func (noDedup) FirstSeen(context.Context, string) (bool, error) { return true, nil }
