package httpserver

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/pos-term/payment-processor/internal/storage"
)

// Store is what the API reads. storage.Store implements it.
type Store interface {
	GetTransaction(ctx context.Context, id string) (storage.TransactionView, bool, error)
	ListTransactions(ctx context.Context, f storage.ListFilter) ([]storage.TransactionView, error)
	ListTerminals(ctx context.Context) ([]storage.Terminal, error)
}

const (
	defaultLimit = 50
	maxLimit     = 200

	// Same shape as the contract: milliseconds, UTC.
	timeLayout = "2006-01-02T15:04:05.000Z"
)

// Patterns copied from the schemas in the infra repo.
var (
	transactionIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	terminalIDRe    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

type api struct {
	store Store
	log   *slog.Logger
}

// healthz reports liveness only: the process is up and serving. It needs no token.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

type transactionJSON struct {
	TransactionID string  `json:"transaction_id"`
	TerminalID    string  `json:"terminal_id"`
	AmountMinor   int64   `json:"amount_minor"`
	Currency      string  `json:"currency"`
	Status        string  `json:"status"`
	Reason        *string `json:"reason"`
	CreatedAt     string  `json:"created_at"`
	ProcessedAt   *string `json:"processed_at"`
}

func toJSON(v storage.TransactionView) transactionJSON {
	out := transactionJSON{
		TransactionID: v.ID,
		TerminalID:    v.TerminalID,
		AmountMinor:   v.AmountMinor,
		Currency:      v.Currency,
		Status:        v.Status,
		Reason:        v.Reason,
		CreatedAt:     v.CreatedAt.UTC().Format(timeLayout),
	}
	if v.ProcessedAt != nil {
		s := v.ProcessedAt.UTC().Format(timeLayout)
		out.ProcessedAt = &s
	}
	return out
}

func (a *api) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.log.Error("api request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	writeProblem(w, http.StatusInternalServerError, codeInternal, "")
}

func (a *api) getTransaction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("transaction_id")
	if !transactionIDRe.MatchString(id) {
		writeProblem(w, http.StatusBadRequest, codeInvalidParameter, "transaction_id must be a UUIDv7")
		return
	}
	v, ok, err := a.store.GetTransaction(r.Context(), id)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	if !ok {
		writeProblem(w, http.StatusNotFound, codeNotFound, "transaction not found")
		return
	}
	writeJSON(w, toJSON(v))
}

func (a *api) listTransactions(w http.ResponseWriter, r *http.Request) {
	f, detail := parseListFilter(r)
	if detail != "" {
		writeProblem(w, http.StatusBadRequest, codeInvalidParameter, detail)
		return
	}

	// One extra row tells whether another page follows without a second query.
	limit := f.Limit
	f.Limit = limit + 1
	rows, err := a.store.ListTransactions(r.Context(), f)
	if err != nil {
		a.internalError(w, r, err)
		return
	}

	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		c := encodeCursor(rows[limit-1].ID)
		next = &c
	}
	items := make([]transactionJSON, 0, len(rows))
	for _, v := range rows {
		items = append(items, toJSON(v))
	}
	writeJSON(w, struct {
		Items      []transactionJSON `json:"items"`
		NextCursor *string           `json:"next_cursor"`
	}{items, next})
}

// parseListFilter validates the query string. A non-empty detail is the 400 message.
func parseListFilter(r *http.Request) (f storage.ListFilter, detail string) {
	q := r.URL.Query()
	f.Limit = defaultLimit

	if v := q.Get("terminal_id"); v != "" {
		if !terminalIDRe.MatchString(v) {
			return f, "terminal_id must match " + terminalIDRe.String()
		}
		f.TerminalID = v
	}
	switch v := q.Get("status"); v {
	case "", storage.StatusPending, storage.StatusSuccess, storage.StatusFailed:
		f.Status = v
	default:
		return f, "status must be one of pending, success, failed"
	}
	for name, dst := range map[string]*time.Time{"from": &f.From, "to": &f.To} {
		if v := q.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return f, name + " must be an RFC 3339 date-time"
			}
			*dst = t.UTC()
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			return f, "limit must be an integer from 1 to " + strconv.Itoa(maxLimit)
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		id, ok := decodeCursor(v)
		if !ok {
			return f, "cursor is not valid"
		}
		f.After = id
	}
	return f, ""
}

// The cursor is the transaction_id of the last item of the page: the order is by that id,
// so the next page is everything below it. It is opaque to clients.
func encodeCursor(id string) string { return base64.RawURLEncoding.EncodeToString([]byte(id)) }

func decodeCursor(c string) (string, bool) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !transactionIDRe.Match(b) {
		return "", false
	}
	return string(b), true
}

func (a *api) listTerminals(w http.ResponseWriter, r *http.Request) {
	ts, err := a.store.ListTerminals(r.Context())
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	type terminalJSON struct {
		TerminalID        string `json:"terminal_id"`
		LastTransactionAt string `json:"last_transaction_at"`
	}
	items := make([]terminalJSON, 0, len(ts))
	for _, t := range ts {
		items = append(items, terminalJSON{t.ID, t.LastTransactionAt.UTC().Format(timeLayout)})
	}
	writeJSON(w, struct {
		Items []terminalJSON `json:"items"`
	}{items})
}
