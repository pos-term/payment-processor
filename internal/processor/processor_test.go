package processor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/storage"
)

var errBoom = errors.New("boom")

type memStore struct {
	mu      sync.Mutex
	txs     map[string]storage.Transaction
	results map[string]storage.Result
	failOn  string // method name that fails once: "insert" or "save"
	saves   int
}

func newMemStore() *memStore {
	return &memStore{txs: map[string]storage.Transaction{}, results: map[string]storage.Result{}}
}

func (s *memStore) fail(method string) bool {
	if s.failOn == method {
		s.failOn = ""
		return true
	}
	return false
}

func (s *memStore) InsertTransaction(_ context.Context, tx storage.Transaction) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail("insert") {
		return false, errBoom
	}
	if _, ok := s.txs[tx.ID]; ok {
		return false, nil
	}
	s.txs[tx.ID] = tx
	return true, nil
}

func (s *memStore) SaveResult(_ context.Context, id, status string, reason *string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail("save") {
		return errBoom
	}
	s.saves++
	if _, ok := s.results[id]; !ok {
		s.results[id] = storage.Result{TransactionID: id, TerminalID: s.txs[id].TerminalID, Status: status, Reason: reason, ProcessedAt: at}
	}
	return nil
}

func (s *memStore) GetResult(_ context.Context, id string) (storage.Result, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[id]
	return r, ok, nil
}

type memDLQ struct {
	letters []consumer.DeadLetter
	err     error
}

func (m *memDLQ) PublishDeadLetter(_ context.Context, d consumer.DeadLetter) error {
	if m.err != nil {
		return m.err
	}
	m.letters = append(m.letters, d)
	return nil
}

type memDedup struct {
	mu   sync.Mutex
	seen map[string]bool
	err  error
}

func newDedup() *memDedup { return &memDedup{seen: map[string]bool{}} }

func (d *memDedup) FirstSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return false, d.err
	}
	first := !d.seen[id]
	d.seen[id] = true
	return first, nil
}

func (d *memDedup) flush() { d.mu.Lock(); d.seen = map[string]bool{}; d.mu.Unlock() }

func newProc(store *memStore, dlq *memDLQ, maxAmount int64) *Processor {
	return newProcWith(store, newDedup(), dlq, maxAmount)
}

func newProcWith(store *memStore, dedup *memDedup, dlq *memDLQ, maxAmount int64) *Processor {
	return New(store, dedup, dlq, maxAmount, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func rec(value string) consumer.Record {
	return consumer.Record{Topic: "pos.transactions.v1", Partition: 1, Offset: 10, Key: []byte("term-001"),
		Value: []byte(value), Headers: map[string]string{"trace_id": "trace-1"}}
}

func TestBadMessagesGoToDLQAndAreDone(t *testing.T) {
	dlq, store := &memDLQ{}, newMemStore()
	p := newProc(store, dlq, 0)
	for _, v := range []string{
		`{"schema_version":1,"amount_minor":0}`,
		`not json`,
		`{"schema_version":7}`,
	} {
		if err := p.Handle(context.Background(), rec(v)); err != nil {
			t.Fatalf("Handle(%q) = %v, want nil so the offset is committed", v, err)
		}
	}

	if len(dlq.letters) != 3 {
		t.Fatalf("dead letters = %d, want 3", len(dlq.letters))
	}
	want := []string{ErrValidationFailed, ErrInvalidJSON, ErrUnsupportedSchemaVersion}
	for i, w := range want {
		if dlq.letters[i].Kind != w {
			t.Errorf("dead letter %d kind = %q, want %q", i, dlq.letters[i].Kind, w)
		}
	}
	if string(dlq.letters[1].Record.Value) != "not json" {
		t.Error("the original payload must be kept unchanged")
	}
	if len(store.txs) != 0 || len(store.results) != 0 {
		t.Error("a bad message must not touch the store")
	}
}

func TestValidMessageIsNotDeadLettered(t *testing.T) {
	dlq := &memDLQ{}
	if err := newProc(newMemStore(), dlq, 0).Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if len(dlq.letters) != 0 {
		t.Errorf("a valid transaction went to the DLQ: %+v", dlq.letters)
	}
}

// If the DLQ itself is unavailable the offset must stay uncommitted, otherwise the message is lost.
func TestDLQFailureIsReturnedSoTheOffsetIsNotCommitted(t *testing.T) {
	boom := errors.New("kafka is down")
	err := newProc(newMemStore(), &memDLQ{err: boom}, 0).Handle(context.Background(), rec(`not json`))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the publish error", err)
	}
}

const txID = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"

func TestPaymentIsStoredAsSuccess(t *testing.T) {
	store := newMemStore()
	if err := newProc(store, &memDLQ{}, 0).Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}

	tx := store.txs[txID]
	if tx.TerminalID != "term-001" || tx.AmountMinor != 15050 || tx.Currency != "RUB" || tx.Seq != 42 {
		t.Errorf("transaction stored as sent by the terminal: %+v", tx)
	}
	r := store.results[txID]
	if r.Status != StatusSuccess || r.Reason != nil || r.ProcessedAt.IsZero() {
		t.Errorf("unexpected result: %+v", r)
	}
}

func TestPaymentAboveLimitFailsWithReason(t *testing.T) {
	store, dlq := newMemStore(), &memDLQ{}
	if err := newProc(store, dlq, 10000).Handle(context.Background(), rec(validTx)); err != nil { // 15050 > 10000
		t.Fatal(err)
	}

	r := store.results[txID]
	if r.Status != StatusFailed || r.Reason == nil || *r.Reason != ReasonLimitExceeded {
		t.Errorf("unexpected result: %+v", r)
	}
	if len(dlq.letters) != 0 {
		t.Error("a failed payment is a business result, not a dead letter")
	}
}

func TestPaymentAtLimitSucceeds(t *testing.T) {
	store := newMemStore()
	if err := newProc(store, &memDLQ{}, 15050).Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if store.results[txID].Status != StatusSuccess {
		t.Errorf("amount equal to the limit must pass: %+v", store.results[txID])
	}
}

// A crash between the pending insert and the result leaves the transaction pending;
// the replay of the same record completes it.
func TestReplayCompletesPendingTransaction(t *testing.T) {
	store := newMemStore()
	p := newProc(store, &memDLQ{}, 0)

	store.failOn = "save"
	if err := p.Handle(context.Background(), rec(validTx)); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the store error so the offset stays uncommitted", err)
	}
	if _, pending := store.txs[txID]; !pending || len(store.results) != 0 {
		t.Fatalf("expected a pending transaction: %d txs, %d results", len(store.txs), len(store.results))
	}

	if err := p.Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if len(store.txs) != 1 || len(store.results) != 1 || store.results[txID].Status != StatusSuccess {
		t.Errorf("replay must complete the same transaction: %d txs, %d results", len(store.txs), len(store.results))
	}
}

func TestStoreErrorIsReturnedSoTheOffsetIsNotCommitted(t *testing.T) {
	store := newMemStore()
	store.failOn = "insert"
	if err := newProc(store, &memDLQ{}, 0).Handle(context.Background(), rec(validTx)); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the store error", err)
	}
}

func TestSameRecordTwiceKeepsTheFirstResult(t *testing.T) {
	store := newMemStore()
	p := newProc(store, &memDLQ{}, 0)
	for i := 0; i < 2; i++ {
		if err := p.Handle(context.Background(), rec(validTx)); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.txs) != 1 || len(store.results) != 1 {
		t.Errorf("%d txs, %d results, want 1 each", len(store.txs), len(store.results))
	}
}

func handleTwice(t *testing.T, p *Processor, between func()) {
	t.Helper()
	if err := p.Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if between != nil {
		between()
	}
	if err := p.Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateIsProcessedOnce(t *testing.T) {
	store := newMemStore()
	handleTwice(t, newProc(store, &memDLQ{}, 0), nil)

	if len(store.txs) != 1 || len(store.results) != 1 || store.saves != 1 {
		t.Errorf("%d txs, %d results, %d decisions; want 1 each", len(store.txs), len(store.results), store.saves)
	}
}

// Flushing Redis must not lead to double processing: Postgres still knows the transaction.
func TestRedisFlushDoesNotCauseDoubleProcessing(t *testing.T) {
	store, dedup := newMemStore(), newDedup()
	handleTwice(t, newProcWith(store, dedup, &memDLQ{}, 0), dedup.flush)

	if len(store.txs) != 1 || len(store.results) != 1 || store.saves != 1 {
		t.Errorf("%d txs, %d results, %d decisions; want 1 each", len(store.txs), len(store.results), store.saves)
	}
}

func TestRedisDownFallsBackToPostgres(t *testing.T) {
	store, dedup := newMemStore(), newDedup()
	dedup.err = errBoom
	handleTwice(t, newProcWith(store, dedup, &memDLQ{}, 0), nil)

	if len(store.txs) != 1 || store.saves != 1 {
		t.Errorf("%d txs, %d decisions; want 1 each", len(store.txs), store.saves)
	}
}

// Redis was marked, then the service died before Postgres saw the payment. Redis must not swallow it.
func TestRedisMarkWithoutPostgresRowStillProcesses(t *testing.T) {
	store, dedup := newMemStore(), newDedup()
	dedup.seen[txID] = true
	if err := newProcWith(store, dedup, &memDLQ{}, 0).Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if len(store.txs) != 1 || store.saves != 1 {
		t.Errorf("%d txs, %d decisions; want the payment processed once", len(store.txs), store.saves)
	}
}

func TestDifferentTransactionsAreNotDeduplicated(t *testing.T) {
	store := newMemStore()
	p := newProc(store, &memDLQ{}, 0)
	other := `{"schema_version":1,"transaction_id":"0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e11","terminal_id":"term-001",` +
		`"amount_minor":100,"currency":"RUB","created_at":"2026-09-19T10:15:30.123Z","seq":43}`
	for _, v := range []string{validTx, other} {
		if err := p.Handle(context.Background(), rec(v)); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.txs) != 2 || store.saves != 2 {
		t.Errorf("%d txs, %d decisions; want 2 each", len(store.txs), store.saves)
	}
}
