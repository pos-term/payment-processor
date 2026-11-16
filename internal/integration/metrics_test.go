package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/httpserver"
	"github.com/pos-term/payment-processor/internal/metrics"
	"github.com/pos-term/payment-processor/internal/processor"
)

func TestPaymentMetrics(t *testing.T) {
	e := setupInfra(t, os.Getenv("TEST_REDIS_ADDR"))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	counters := metrics.New()
	cons, err := consumer.New(e.cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	cons.WithMetrics(counters)
	proc := processor.New(e.store, processor.NewRedisDeduper(e.redis, time.Hour), cons, 1000, log)
	ctx, cancel := context.WithCancel(context.Background())
	i := &instance{cancel: cancel, done: make(chan struct{}), cons: cons}
	go func() {
		defer close(i.done)
		failFirst := true
		_ = cons.Run(ctx, consumer.HandlerFunc(func(ctx context.Context, rec consumer.Record) error {
			if failFirst {
				failFirst = false
				return errors.New("transient processing error")
			}
			return proc.Handle(ctx, rec)
		}))
	}()
	t.Cleanup(i.stop)

	handler := httpserver.New(":0", "test", e.store, log, httpserver.WithMetrics(counters.Handler())).Handler
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthenticated scrape: status=%d headers=%v", rec.Code, rec.Header())
	}
	scrape := func() []string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		req.Header.Set("Authorization", "Bearer test")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain;") {
			t.Fatalf("scrape: status=%d headers=%v body=%s", rec.Code, rec.Header(), rec.Body)
		}
		var samples []string
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if strings.HasPrefix(line, "payment_processor_") {
				samples = append(samples, line)
			}
		}
		slices.Sort(samples)
		return samples
	}
	expected := func(value string) []string {
		return []string{
			"payment_processor_dlq_published_total " + value,
			"payment_processor_processing_retries_total " + value,
			`payment_processor_results_published_total{status="failed"} ` + value,
			`payment_processor_results_published_total{status="success"} ` + value,
		}
	}
	if got := scrape(); !slices.Equal(got, expected("0")) {
		t.Fatalf("initial samples=%v, want %v", got, expected("0"))
	}

	// Keep all records in order on one partition, with the DLQ record last.
	// The duplicate has a stored, already published result and must not add a sample.
	sendCtx, stopSend := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopSend()
	for _, value := range []string{txJSON(idA, 1000), txJSON(idA, 1000), txJSON(idB, 2000), "invalid json"} {
		record := &kgo.Record{Topic: e.cfg.InputTopic, Key: []byte("term-001"), Value: []byte(value)}
		if err := e.cl.ProduceSync(sendCtx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	want := expected("1")
	deadline := time.Now().Add(20 * time.Second)
	for {
		got := scrape()
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("samples=%v, want %v", got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
