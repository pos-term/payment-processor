package storage

import (
	"context"
	"testing"
	"time"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	dsn := testDB(t)
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func sampleTx(id string) Transaction {
	return Transaction{
		ID: id, TerminalID: "term-001", AmountMinor: 15050, Currency: "RUB", Seq: 42,
		CreatedAt: time.Date(2026, 9, 19, 10, 15, 30, 123_000_000, time.UTC),
	}
}

func TestInsertTransactionIsIdempotent(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if inserted, err := s.InsertTransaction(ctx, sampleTx(txID1)); err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}
	if inserted, err := s.InsertTransaction(ctx, sampleTx(txID1)); err != nil || inserted {
		t.Fatalf("second insert: inserted=%v err=%v, want false and no error", inserted, err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM transactions").Scan(&n); err != nil || n != 1 {
		t.Errorf("rows = %d (err %v), want 1", n, err)
	}
}

func TestPendingThenSuccess(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.InsertTransaction(ctx, sampleTx(txID1)); err != nil {
		t.Fatal(err)
	}

	// Pending: stored, no result yet.
	if _, ok, err := s.GetResult(ctx, txID1); err != nil || ok {
		t.Fatalf("pending transaction: ok=%v err=%v, want no result", ok, err)
	}

	at := time.Date(2026, 9, 19, 10, 15, 31, 5_000_000, time.UTC)
	if err := s.SaveResult(ctx, txID1, "success", nil, at); err != nil {
		t.Fatal(err)
	}
	r, ok, err := s.GetResult(ctx, txID1)
	if err != nil || !ok {
		t.Fatalf("GetResult: ok=%v err=%v", ok, err)
	}
	if r.Status != "success" || r.Reason != nil || r.TerminalID != "term-001" || !r.ProcessedAt.Equal(at) {
		t.Errorf("unexpected result: %+v", r)
	}
}

func TestFailedKeepsReasonAndFirstResultWins(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.InsertTransaction(ctx, sampleTx(txID1)); err != nil {
		t.Fatal(err)
	}
	reason := "LIMIT_EXCEEDED"
	at := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.SaveResult(ctx, txID1, "failed", &reason, at); err != nil {
		t.Fatal(err)
	}
	// A second writer must not change the outcome.
	if err := s.SaveResult(ctx, txID1, "success", nil, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.GetResult(ctx, txID1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "failed" || r.Reason == nil || *r.Reason != "LIMIT_EXCEEDED" {
		t.Errorf("unexpected result: %+v", r)
	}
}

func TestMarkPublished(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.InsertTransaction(ctx, sampleTx(txID1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveResult(ctx, txID1, "success", nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	if r, _, _ := s.GetResult(ctx, txID1); r.Published {
		t.Fatal("a fresh result must not be published")
	}
	if err := s.MarkPublished(ctx, txID1); err != nil {
		t.Fatal(err)
	}
	if r, _, _ := s.GetResult(ctx, txID1); !r.Published {
		t.Fatal("the result must be published after MarkPublished")
	}
	// Idempotent, and an unknown id is not an error: the replay of a record may mark twice.
	if err := s.MarkPublished(ctx, txID1); err != nil {
		t.Errorf("second MarkPublished: %v", err)
	}
	if err := s.MarkPublished(ctx, txID2); err != nil {
		t.Errorf("MarkPublished of an unknown id: %v", err)
	}
}
