package storage

import (
	"context"
	"testing"
	"time"
)

const txID3 = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e12"

// seed stores three transactions: txID1 success, txID2 failed, txID3 pending.
// txID1 and txID2 belong to term-001, txID3 to term-002.
func seed(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	for i, tx := range []Transaction{
		{ID: txID1, TerminalID: "term-001", AmountMinor: 100, Currency: "RUB", CreatedAt: base},
		{ID: txID2, TerminalID: "term-001", AmountMinor: 20000000, Currency: "RUB", CreatedAt: base.Add(time.Hour)},
		{ID: txID3, TerminalID: "term-002", AmountMinor: 300, Currency: "EUR", CreatedAt: base.Add(2 * time.Hour)},
	} {
		tx.Seq = int64(i)
		if _, err := s.InsertTransaction(ctx, tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SaveResult(ctx, txID1, StatusSuccess, nil, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	reason := "LIMIT_EXCEEDED"
	if err := s.SaveResult(ctx, txID2, StatusFailed, &reason, base.Add(time.Hour+time.Second)); err != nil {
		t.Fatal(err)
	}
}

func ids(vs []TransactionView) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.ID
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGetTransaction(t *testing.T) {
	s := openStore(t)
	seed(t, s)
	ctx := context.Background()

	v, ok, err := s.GetTransaction(ctx, txID3)
	if err != nil || !ok {
		t.Fatalf("pending: ok=%v err=%v", ok, err)
	}
	if v.Status != StatusPending || v.Reason != nil || v.ProcessedAt != nil || v.Currency != "EUR" || v.AmountMinor != 300 {
		t.Errorf("pending view = %+v", v)
	}

	v, _, _ = s.GetTransaction(ctx, txID2)
	if v.Status != StatusFailed || v.Reason == nil || *v.Reason != "LIMIT_EXCEEDED" || v.ProcessedAt == nil {
		t.Errorf("failed view = %+v", v)
	}
	if v.CreatedAt.Location() != time.UTC || v.ProcessedAt.Location() != time.UTC {
		t.Errorf("times must be UTC: %v, %v", v.CreatedAt.Location(), v.ProcessedAt.Location())
	}

	if _, ok, err := s.GetTransaction(ctx, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5eff"); err != nil || ok {
		t.Errorf("unknown id: ok=%v err=%v, want not found", ok, err)
	}
}

func TestListTransactions(t *testing.T) {
	s := openStore(t)
	seed(t, s)
	ctx := context.Background()
	from := time.Date(2026, 9, 19, 10, 30, 0, 0, time.UTC)
	to := time.Date(2026, 9, 19, 11, 30, 0, 0, time.UTC)

	for name, tc := range map[string]struct {
		filter ListFilter
		want   []string
	}{
		"newest first":     {ListFilter{Limit: 10}, []string{txID3, txID2, txID1}},
		"limit":            {ListFilter{Limit: 2}, []string{txID3, txID2}},
		"after":            {ListFilter{Limit: 10, After: txID3}, []string{txID2, txID1}},
		"after the last":   {ListFilter{Limit: 10, After: txID1}, nil},
		"terminal":         {ListFilter{Limit: 10, TerminalID: "term-001"}, []string{txID2, txID1}},
		"unknown terminal": {ListFilter{Limit: 10, TerminalID: "nope"}, nil},
		"pending":          {ListFilter{Limit: 10, Status: StatusPending}, []string{txID3}},
		"success":          {ListFilter{Limit: 10, Status: StatusSuccess}, []string{txID1}},
		"failed":           {ListFilter{Limit: 10, Status: StatusFailed}, []string{txID2}},
		"from":             {ListFilter{Limit: 10, From: from}, []string{txID3, txID2}},
		"to is exclusive":  {ListFilter{Limit: 10, To: to}, []string{txID2, txID1}},
		"from and to":      {ListFilter{Limit: 10, From: from, To: to}, []string{txID2}},
		"combined":         {ListFilter{Limit: 10, TerminalID: "term-001", Status: StatusFailed, After: txID3}, []string{txID2}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := s.ListTransactions(ctx, tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			if !equal(ids(got), tc.want) {
				t.Errorf("ids = %v, want %v", ids(got), tc.want)
			}
		})
	}
}

func TestListTransactionsPagesWalkEverythingOnce(t *testing.T) {
	s := openStore(t)
	seed(t, s)
	ctx := context.Background()

	var seen []string
	after := ""
	for range 5 {
		page, err := s.ListTransactions(ctx, ListFilter{Limit: 1, After: after})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		seen = append(seen, page[0].ID)
		after = page[0].ID
	}
	if !equal(seen, []string{txID3, txID2, txID1}) {
		t.Errorf("walked %v", seen)
	}
}

func TestListTerminals(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if ts, err := s.ListTerminals(ctx); err != nil || len(ts) != 0 {
		t.Fatalf("empty database: %v, %v", ts, err)
	}

	seed(t, s)
	ts, err := s.ListTerminals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 || ts[0].ID != "term-002" || ts[1].ID != "term-001" {
		t.Fatalf("terminals = %+v, want term-002 then term-001", ts)
	}
	// term-001's latest transaction is txID2, created an hour after the first.
	if want := time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC); !ts[1].LastTransactionAt.Equal(want) {
		t.Errorf("last_transaction_at = %v, want %v", ts[1].LastTransactionAt, want)
	}
}
