package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pos-term/payment-processor/internal/events"
	"github.com/pos-term/payment-processor/internal/httpserver"
)

func TestSSEKafkaToTwoInstancesAndReplay(t *testing.T) {
	e := setupInfra(t, os.Getenv("TEST_REDIS_ADDR"))
	key := fmt.Sprintf("sse_integration:%d", time.Now().UnixNano())
	t.Cleanup(func() { e.redis.Del(context.Background(), key, key+":epoch") })
	writer := events.New(e.redis, e.store, key)
	e.start(t, e.store, writer)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server1 := httptest.NewServer(httpserver.New(":0", "test", e.store, log, httpserver.WithEventSource(events.New(e.redis, e.store, key))).Handler)
	server2 := httptest.NewServer(httpserver.New(":0", "test", e.store, log, httpserver.WithEventSource(events.New(e.redis, e.store, key))).Handler)
	t.Cleanup(server1.Close)
	t.Cleanup(server2.Close)
	connect := func(server *httptest.Server, after string) *bufio.Reader {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/transactions/stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("Last-Event-ID", after)
		response, err := server.Client().Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(func() { cancel(); response.Body.Close() })
		if response.StatusCode != 200 {
			t.Fatalf("SSE status=%d", response.StatusCode)
		}
		return bufio.NewReader(response.Body)
	}
	type update struct{ id, status string }
	next := func(reader *bufio.Reader) update {
		t.Helper()
		var id, kind, data string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			line = strings.TrimSuffix(line, "\n")
			if strings.HasPrefix(line, "id: ") {
				id = strings.TrimPrefix(line, "id: ")
			}
			if strings.HasPrefix(line, "event: ") {
				kind = strings.TrimPrefix(line, "event: ")
			}
			if strings.HasPrefix(line, "data: ") {
				data = strings.TrimPrefix(line, "data: ")
			}
			if line == "" {
				if kind == "transaction.updated" {
					var v struct {
						ID          string  `json:"transaction_id"`
						Status      string  `json:"status"`
						ProcessedAt *string `json:"processed_at"`
					}
					if err := json.Unmarshal([]byte(data), &v); err != nil {
						t.Fatal(err)
					}
					if v.ID != idA || (v.Status == "pending") != (v.ProcessedAt == nil) {
						t.Fatalf("snapshot=%s", data)
					}
					return update{id, v.Status}
				}
				id, kind, data = "", "", ""
			}
		}
	}
	first, second := connect(server1, ""), connect(server2, "")
	e.send(t, idA)
	var after string
	var finalID string
	for i, reader := range []*bufio.Reader{first, second} {
		pending, final := next(reader), next(reader)
		if pending.status != "pending" || final.status != "success" || pending.id == final.id {
			t.Fatalf("instance %d: %+v %+v", i, pending, final)
		}
		if i == 0 {
			after = pending.id
			finalID = final.id
		} else if pending.id != after || final.id != finalID {
			t.Fatal("instances did not receive the same journal events")
		}
	}
	replay := next(connect(server2, after))
	if replay.id != finalID || replay.status != "success" {
		t.Fatalf("replay=%+v", replay)
	}
	if counts := e.resultCounts(t, 1); counts[idA] != 1 {
		t.Fatalf("Kafka results=%v", counts)
	}
}
