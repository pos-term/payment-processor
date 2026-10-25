package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pos-term/payment-processor/internal/storage"
)

const (
	testToken = "secret"
	txID1     = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"
	txID2     = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e11"
	txID3     = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e12"
)

type fakeStore struct {
	byID      map[string]storage.TransactionView
	list      []storage.TransactionView
	terminals []storage.Terminal
	err       error

	gotFilter storage.ListFilter
}

func (f *fakeStore) GetTransaction(_ context.Context, id string) (storage.TransactionView, bool, error) {
	v, ok := f.byID[id]
	return v, ok, f.err
}

func (f *fakeStore) ListTransactions(_ context.Context, flt storage.ListFilter) ([]storage.TransactionView, error) {
	f.gotFilter = flt
	if len(f.list) > flt.Limit {
		return f.list[:flt.Limit], f.err
	}
	return f.list, f.err
}

func (f *fakeStore) ListTerminals(context.Context) ([]storage.Terminal, error) {
	return f.terminals, f.err
}

func view(id, status string) storage.TransactionView {
	v := storage.TransactionView{
		ID: id, TerminalID: "term-001", AmountMinor: 15050, Currency: "RUB", Status: status,
		CreatedAt: time.Date(2026, 9, 19, 10, 15, 30, 123_000_000, time.UTC),
	}
	switch status {
	case storage.StatusSuccess:
		at := time.Date(2026, 9, 19, 10, 15, 31, 5_000_000, time.UTC)
		v.ProcessedAt = &at
	case storage.StatusFailed:
		at := time.Date(2026, 9, 19, 10, 15, 31, 5_000_000, time.UTC)
		reason := "LIMIT_EXCEEDED"
		v.ProcessedAt, v.Reason = &at, &reason
	}
	return v
}

func get(t *testing.T, store Store, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	rec := httptest.NewRecorder()
	New(":0", testToken, store, discardLogger()).Handler.ServeHTTP(rec, req)
	return rec
}

func authed(t *testing.T, store Store, path string) *httptest.ResponseRecorder {
	t.Helper()
	return get(t, store, path, "Bearer "+testToken)
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("body %q is not JSON: %v", rec.Body.String(), err)
	}
}

func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q", ct)
	}
	var p problem
	decode(t, rec, &p)
	if p.Code != code || p.Status != status {
		t.Errorf("problem = %+v, want code %q status %d", p, code, status)
	}
}

func TestHealthz(t *testing.T) {
	rec := get(t, &fakeStore{}, "/healthz", "") // no token
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"status":"ok"}` {
		t.Errorf("body = %q", got)
	}
}

func TestHealthzRejectsOtherMethods(t *testing.T) {
	srv := New(":0", testToken, &fakeStore{}, discardLogger())
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestAPIRequiresBearerToken(t *testing.T) {
	store := &fakeStore{byID: map[string]storage.TransactionView{txID1: view(txID1, storage.StatusSuccess)}}
	paths := []string{"/v1/transactions", "/v1/transactions/" + txID1, "/v1/terminals"}
	headers := map[string]string{
		"missing":      "",
		"wrong token":  "Bearer nope",
		"empty token":  "Bearer ",
		"other scheme": "Basic " + testToken,
		"no scheme":    testToken,
	}
	for _, path := range paths {
		for name, h := range headers {
			rec := get(t, store, path, h)
			wantProblem(t, rec, http.StatusUnauthorized, "unauthorized")
			if rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Errorf("%s %s: no WWW-Authenticate header", name, path)
			}
		}
		if rec := authed(t, store, path); rec.Code != http.StatusOK {
			t.Errorf("%s with the right token: status = %d, want 200", path, rec.Code)
		}
	}
}

func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	if rec := get(t, &fakeStore{}, "/v1/terminals", "bearer "+testToken); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestUnauthorizedBeforeValidation(t *testing.T) {
	// A bad parameter must not leak validation details to a caller without a token.
	rec := get(t, &fakeStore{}, "/v1/transactions?limit=abc", "")
	wantProblem(t, rec, http.StatusUnauthorized, "unauthorized")
}

func TestGetTransaction(t *testing.T) {
	store := &fakeStore{byID: map[string]storage.TransactionView{
		txID1: view(txID1, storage.StatusPending),
		txID2: view(txID2, storage.StatusFailed),
	}}

	t.Run("pending has null reason and processed_at", func(t *testing.T) {
		rec := authed(t, store, "/v1/transactions/"+txID1)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		want := `{"transaction_id":"` + txID1 + `","terminal_id":"term-001","amount_minor":15050,"currency":"RUB",` +
			`"status":"pending","reason":null,"created_at":"2026-09-19T10:15:30.123Z","processed_at":null}`
		if got := strings.TrimSpace(rec.Body.String()); got != want {
			t.Errorf("body =\n%s\nwant\n%s", got, want)
		}
	})

	t.Run("failed carries reason", func(t *testing.T) {
		var tx transactionJSON
		decode(t, authed(t, store, "/v1/transactions/"+txID2), &tx)
		if tx.Status != "failed" || tx.Reason == nil || *tx.Reason != "LIMIT_EXCEEDED" ||
			tx.ProcessedAt == nil || *tx.ProcessedAt != "2026-09-19T10:15:31.005Z" {
			t.Errorf("tx = %+v", tx)
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		wantProblem(t, authed(t, store, "/v1/transactions/"+txID3), http.StatusNotFound, "not_found")
	})

	t.Run("not a UUIDv7", func(t *testing.T) {
		for _, id := range []string{"abc", "stream", "0192f0c4-7a3e-4b21-9d4e-2c1f8a6b5e10"} {
			wantProblem(t, authed(t, store, "/v1/transactions/"+id), http.StatusBadRequest, "invalid_parameter")
		}
	})

	t.Run("store failure is a 500 without details", func(t *testing.T) {
		rec := authed(t, &fakeStore{err: errors.New("connection refused")}, "/v1/transactions/"+txID1)
		wantProblem(t, rec, http.StatusInternalServerError, "internal_error")
		if strings.Contains(rec.Body.String(), "connection refused") {
			t.Errorf("internal error leaked to the client: %s", rec.Body.String())
		}
	})
}

type page struct {
	Items      []transactionJSON `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

func TestListTransactionsPagination(t *testing.T) {
	store := &fakeStore{list: []storage.TransactionView{
		view(txID3, storage.StatusSuccess), view(txID2, storage.StatusFailed), view(txID1, storage.StatusPending),
	}}

	var p page
	decode(t, authed(t, store, "/v1/transactions?limit=2"), &p)
	if len(p.Items) != 2 || p.Items[0].TransactionID != txID3 || p.Items[1].TransactionID != txID2 {
		t.Fatalf("items = %+v", p.Items)
	}
	if p.NextCursor == nil {
		t.Fatal("next_cursor = null, want a cursor: one more row exists")
	}
	if store.gotFilter.Limit != 3 {
		t.Errorf("store limit = %d, want 3 (page size + 1)", store.gotFilter.Limit)
	}

	// The cursor is handed back to the store as the "after" id.
	authed(t, store, "/v1/transactions?limit=2&cursor="+*p.NextCursor)
	if store.gotFilter.After != txID2 {
		t.Errorf("After = %q, want %q", store.gotFilter.After, txID2)
	}
}

func TestListTransactionsLastPage(t *testing.T) {
	store := &fakeStore{list: []storage.TransactionView{view(txID1, storage.StatusSuccess)}}
	var p page
	decode(t, authed(t, store, "/v1/transactions"), &p)
	if len(p.Items) != 1 || p.NextCursor != nil {
		t.Errorf("page = %+v, want one item and null next_cursor", p)
	}
	if store.gotFilter.Limit != defaultLimit+1 {
		t.Errorf("store limit = %d, want %d", store.gotFilter.Limit, defaultLimit+1)
	}
}

func TestListTransactionsEmptyIsArrayNotNull(t *testing.T) {
	rec := authed(t, &fakeStore{}, "/v1/transactions")
	if got := strings.TrimSpace(rec.Body.String()); got != `{"items":[],"next_cursor":null}` {
		t.Errorf("body = %s", got)
	}
}

func TestListTransactionsFilters(t *testing.T) {
	store := &fakeStore{}
	authed(t, store, "/v1/transactions?terminal_id=term-007&status=pending&from=2026-09-19T10:00:00Z&to=2026-09-19T12:00:00%2B02:00&limit=10")
	f := store.gotFilter
	if f.TerminalID != "term-007" || f.Status != "pending" || f.Limit != 11 {
		t.Errorf("filter = %+v", f)
	}
	if !f.From.Equal(time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)) || !f.To.Equal(time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("from = %v, to = %v (offsets must be normalised to UTC)", f.From, f.To)
	}
}

func TestListTransactionsRejectsBadParameters(t *testing.T) {
	for _, q := range []string{
		"limit=0", "limit=201", "limit=abc", "limit=-1",
		"status=done", "status=PENDING",
		"terminal_id=bad%20id", "terminal_id=" + strings.Repeat("a", 65),
		"from=yesterday", "to=2026-09-19",
		"cursor=!!!", "cursor=YWJj", // not base64url / not a transaction id
	} {
		wantProblem(t, authed(t, &fakeStore{}, "/v1/transactions?"+q), http.StatusBadRequest, "invalid_parameter")
	}
}

func TestListTerminals(t *testing.T) {
	store := &fakeStore{terminals: []storage.Terminal{
		{ID: "term-002", LastTransactionAt: time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC)},
		{ID: "term-001", LastTransactionAt: time.Date(2026, 9, 19, 10, 15, 30, 123_000_000, time.UTC)},
	}}
	got := strings.TrimSpace(authed(t, store, "/v1/terminals").Body.String())
	want := `{"items":[{"terminal_id":"term-002","last_transaction_at":"2026-09-19T11:00:00.000Z"},` +
		`{"terminal_id":"term-001","last_transaction_at":"2026-09-19T10:15:30.123Z"}]}`
	if got != want {
		t.Errorf("body =\n%s\nwant\n%s", got, want)
	}
	if got := strings.TrimSpace(authed(t, &fakeStore{}, "/v1/terminals").Body.String()); got != `{"items":[]}` {
		t.Errorf("empty body = %s", got)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	id, ok := decodeCursor(encodeCursor(txID1))
	if !ok || id != txID1 {
		t.Errorf("decodeCursor(encodeCursor(id)) = %q, %v", id, ok)
	}
}
