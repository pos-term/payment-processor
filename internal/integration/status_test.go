package integration

import (
	"context"
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

func txJSON(id string, amount int) string {
	return fmt.Sprintf(`{"schema_version":1,"transaction_id":"%s","terminal_id":"term-001","amount_minor":%d,`+
		`"currency":"RUB","created_at":"2026-09-19T10:15:30.123Z","seq":1}`, id, amount)
}

// Transactions read from Kafka end up in Postgres with the right final status.
// Needs TEST_KAFKA_BROKERS and TEST_POSTGRES_DSN (a server where databases can be created).
func TestTransactionsAreStoredWithStatus(t *testing.T) {
	brokers, adminDSN := os.Getenv("TEST_KAFKA_BROKERS"), os.Getenv("TEST_POSTGRES_DSN")
	if brokers == "" || adminDSN == "" {
		t.Skip("TEST_KAFKA_BROKERS and TEST_POSTGRES_DSN are required; start the infra compose stack")
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// Own database for the test.
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
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
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

	const (
		okID, bigID = "0192f0c4-7a3e-7b21-9d4e-000000000001", "0192f0c4-7a3e-7b21-9d4e-000000000002"
	)
	for _, v := range []string{txJSON(okID, 5000), txJSON(bigID, 50000)} { // the limit below is 10000
		r := &kgo.Record{Topic: cfg.InputTopic, Key: []byte("term-001"), Value: []byte(v)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	cons, err := consumer.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = cons.Run(runCtx, processor.New(store, noDedup{}, cons, 10000, log)) }()
	defer func() { cancel(); <-done; cons.Close() }()

	deadline := time.Now().Add(60 * time.Second)
	var ok, big storage.Result
	for {
		var found1, found2 bool
		ok, found1, _ = store.GetResult(ctx, okID)
		big, found2, _ = store.GetResult(ctx, bigID)
		if found1 && found2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("results not stored in time: %v %v", found1, found2)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok.Status != "success" || ok.Reason != nil || ok.TerminalID != "term-001" {
		t.Errorf("payment under the limit: %+v", ok)
	}
	if big.Status != "failed" || big.Reason == nil || *big.Reason != "LIMIT_EXCEEDED" {
		t.Errorf("payment over the limit: %+v", big)
	}
}
