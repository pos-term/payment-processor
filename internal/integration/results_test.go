package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/processor"
	"github.com/pos-term/payment-processor/internal/storage"
)

// The terminal gets the final result in pos.transaction-results.v1: keyed by terminal_id, with the
// trace_id of the request, in the shape the JSON Schema in infra describes.
func TestResultsAreDeliveredToTheTerminal(t *testing.T) {
	brokers, adminDSN := os.Getenv("TEST_KAFKA_BROKERS"), os.Getenv("TEST_POSTGRES_DSN")
	if brokers == "" || adminDSN == "" {
		t.Skip("TEST_KAFKA_BROKERS and TEST_POSTGRES_DSN are required; start the infra compose stack")
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	dbName := "it_" + suffix
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP DATABASE "+dbName+" WITH (FORCE)") }()
	u, _ := url.Parse(adminDSN)
	u.Path = "/" + dbName
	if err := storage.Migrate(u.String(), log); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := consumer.Config{
		Brokers: strings.Split(brokers, ","), Group: "it-" + suffix,
		InputTopic: "it.in." + suffix, DLQTopic: "it.dlq." + suffix, ResultsTopic: "it.out." + suffix,
	}
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

	const okID, bigID = "0192f0c4-7a3e-7b21-9d4e-00000000b001", "0192f0c4-7a3e-7b21-9d4e-00000000b002"
	send := func(id string, amount int, terminal, trace string) {
		v := strings.Replace(txJSON(id, amount), `"term-001"`, `"`+terminal+`"`, 1)
		r := &kgo.Record{Topic: cfg.InputTopic, Key: []byte(terminal), Value: []byte(v),
			Headers: []kgo.RecordHeader{{Key: "trace_id", Value: []byte(trace)}}}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	send(okID, 5000, "term-001", "trace-ok")
	send(bigID, 50000, "term-002", "trace-big") // the limit below is 10000
	send("not-a-uuid", 1, "term-001", "trace-bad")

	cons, err := consumer.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = cons.Run(runCtx, processor.New(store, noDedup{}, cons, 10000, log)) }()
	defer func() { cancel(); <-done; cons.Close() }()

	reader, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.ResultsTopic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var recs []*kgo.Record
	for end := time.Now().Add(60 * time.Second); len(recs) < 2 && time.Now().Before(end); {
		c, cancel := context.WithTimeout(ctx, time.Second)
		reader.PollFetches(c).EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		cancel()
	}
	time.Sleep(time.Second) // a third, unwanted message would show up by now
	c, cancelPoll := context.WithTimeout(ctx, time.Second)
	reader.PollFetches(c).EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	cancelPoll()
	if len(recs) != 2 {
		t.Fatalf("results = %d, want 2: the invalid message must not produce one", len(recs))
	}

	byID := map[string]*kgo.Record{}
	for _, r := range recs {
		var m struct {
			TransactionID string `json:"transaction_id"`
		}
		_ = json.Unmarshal(r.Value, &m)
		byID[m.TransactionID] = r
	}

	type result struct {
		SchemaVersion int     `json:"schema_version"`
		TransactionID string  `json:"transaction_id"`
		TerminalID    string  `json:"terminal_id"`
		Status        string  `json:"status"`
		Reason        *string `json:"reason"`
		ProcessedAt   string  `json:"processed_at"`
	}
	check := func(id, terminal, trace, status, reason string) {
		r := byID[id]
		if r == nil {
			t.Fatalf("no result for %s", id)
		}
		if string(r.Key) != terminal {
			t.Errorf("%s: key = %q, want %q", id, r.Key, terminal)
		}
		if header(r, "trace_id") != trace || header(r, "event_type") != "transaction.processed" {
			t.Errorf("%s: headers = %v, want trace_id=%s and event_type=transaction.processed", id, r.Headers, trace)
		}
		var m result
		dec := json.NewDecoder(strings.NewReader(string(r.Value)))
		dec.DisallowUnknownFields() // the schema forbids extra fields
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("%s: value does not match the schema: %v", id, err)
		}
		if m.SchemaVersion != 1 || m.TransactionID != id || m.TerminalID != terminal || m.Status != status {
			t.Errorf("%s: unexpected value %+v", id, m)
		}
		if (reason == "") != (m.Reason == nil) || (m.Reason != nil && *m.Reason != reason) {
			t.Errorf("%s: reason = %v, want %q", id, m.Reason, reason)
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", m.ProcessedAt); err != nil {
			t.Errorf("%s: processed_at = %q is not RFC 3339 UTC with milliseconds", id, m.ProcessedAt)
		}
		stored, ok, _ := store.GetResult(ctx, id)
		if !ok || stored.ProcessedAt.UTC().Format("2006-01-02T15:04:05.000Z") != m.ProcessedAt {
			t.Errorf("%s: published processed_at %q differs from the stored one", id, m.ProcessedAt)
		}
	}
	check(okID, "term-001", "trace-ok", "success", "")
	check(bigID, "term-002", "trace-big", "failed", "LIMIT_EXCEEDED")
}
