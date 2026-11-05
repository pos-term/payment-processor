package httpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pos-term/payment-processor/internal/events"
)

type scriptedSource struct {
	mu      sync.Mutex
	batches []events.Batch
	err     error
	cursors []string
}

func (s *scriptedSource) Read(_ context.Context, cursor string) (events.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors = append(s.cursors, cursor)
	if s.err != nil {
		return events.Batch{}, s.err
	}
	if len(s.batches) == 0 {
		return events.Batch{Cursor: cursor}, nil
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return b, nil
}

func openStream(t *testing.T, source EventSource, path, after string, options ...Option) (*bufio.Reader, *http.Response) {
	t.Helper()
	opts := append([]Option{WithEventSource(source)}, options...)
	server := httptest.NewServer(New(":0", testToken, &fakeStore{}, discardLogger(), opts...).Handler)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Last-Event-ID", after)
	response, err := server.Client().Do(req)
	if err != nil {
		cancel()
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); response.Body.Close(); server.Close() })
	if response.StatusCode != 200 {
		t.Fatalf("status=%d", response.StatusCode)
	}
	if response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("headers=%v", response.Header)
	}
	return bufio.NewReader(response.Body), response
}

func frame(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var out strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		out.WriteString(line)
		if line == "\n" {
			return out.String()
		}
	}
}

func TestSSEStreamsStoredTransactionAndReplayCursor(t *testing.T) {
	v := view(txID1, "pending")
	source := &scriptedSource{batches: []events.Batch{{Cursor: "new", Events: []events.Event{{ID: "new", Kind: "transaction.updated", Transaction: v}}}}}
	reader, _ := openStream(t, source, "/v1/transactions/stream", "old")
	if got := frame(t, reader); got != ": connected\n\n" {
		t.Fatal(got)
	}
	got := frame(t, reader)
	if !strings.HasPrefix(got, "id: new\nevent: transaction.updated\ndata: ") {
		t.Fatal(got)
	}
	var tx transactionJSON
	data := strings.TrimSuffix(strings.SplitN(got, "data: ", 2)[1], "\n\n")
	if err := json.Unmarshal([]byte(data), &tx); err != nil {
		t.Fatal(err)
	}
	if tx.TransactionID != txID1 || tx.Status != "pending" || tx.ProcessedAt != nil || tx.CreatedAt != "2026-09-19T10:15:30.123Z" {
		t.Fatalf("transaction=%+v", tx)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.cursors[0] != "old" {
		t.Fatalf("cursor=%q", source.cursors[0])
	}
}

func TestSSEFiltersAndResync(t *testing.T) {
	other := view(txID3, "failed")
	other.TerminalID = "term-002"
	source := &scriptedSource{batches: []events.Batch{{Cursor: "3", Events: []events.Event{
		{ID: "other", Kind: "transaction.updated", Transaction: other},
		{ID: "1", Kind: "transaction.updated", Transaction: view(txID1, "pending")},
		{ID: "2", Kind: "transaction.updated", Transaction: view(txID2, "failed")},
		{ID: "3", Kind: "resync"},
	}}}}
	reader, _ := openStream(t, source, "/v1/transactions/stream?terminal_id=term-001&status=failed", "")
	frame(t, reader)
	if got := frame(t, reader); !strings.Contains(got, `"transaction_id":"`+txID2+`"`) {
		t.Fatal(got)
	}
	if got := frame(t, reader); got != "id: 3\nevent: resync\ndata: {}\n\n" {
		t.Fatal(got)
	}
}

func TestSSEExpiredCursorAndKeepAlive(t *testing.T) {
	source := &scriptedSource{batches: []events.Batch{{Cursor: "tail", Resync: true}}}
	reader, _ := openStream(t, source, "/v1/transactions/stream", "expired", func(a *api) { a.keepAlive = 10 * time.Millisecond })
	frame(t, reader)
	if got := frame(t, reader); got != "id: tail\nevent: resync\ndata: {}\n\n" {
		t.Fatal(got)
	}
	if got := frame(t, reader); got != ": keep-alive\n\n" {
		t.Fatal(got)
	}
}

func TestSSEOutageAndRecoveryResync(t *testing.T) {
	source := &scriptedSource{err: errors.New("redis down")}
	reader, _ := openStream(t, source, "/v1/transactions/stream", "")
	frame(t, reader)
	if got := frame(t, reader); !strings.Contains(got, "event: resync") {
		t.Fatal(got)
	}
	source.mu.Lock()
	source.err = nil
	source.batches = []events.Batch{{Cursor: "tail"}, {Cursor: "tail"}}
	source.mu.Unlock()
	if got := frame(t, reader); got != "id: tail\nevent: resync\ndata: {}\n\n" {
		t.Fatal(got)
	}
}

func TestSSEAuthAndValidation(t *testing.T) {
	wantProblem(t, get(t, &fakeStore{}, "/v1/transactions/stream?status=wrong", ""), 401, "unauthorized")
	for _, query := range []string{"status=", "status=wrong", "status=pending&status=failed", "terminal_id=", "terminal_id=bad%20id", "terminal_id=a&terminal_id=b"} {
		wantProblem(t, authed(t, &fakeStore{}, "/v1/transactions/stream?"+query), 400, "invalid_parameter")
	}
}

type disconnectedWriter struct{ header http.Header }

func (w *disconnectedWriter) Header() http.Header     { return w.header }
func (*disconnectedWriter) WriteHeader(int)           {}
func (*disconnectedWriter) Write([]byte) (int, error) { return 0, errors.New("client disconnected") }
func (*disconnectedWriter) Flush()                    {}

func TestSSEStopsOnWriteFailure(t *testing.T) {
	a := &api{events: &scriptedSource{}, log: discardLogger()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.streamTransactions(&disconnectedWriter{header: http.Header{}}, httptest.NewRequest(http.MethodGet, "/v1/transactions/stream", nil))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream kept polling after write failure")
	}
}
