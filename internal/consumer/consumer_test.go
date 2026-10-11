package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The tests need a Kafka broker: TEST_KAFKA_BROKERS=localhost:9094 with the infra compose stack running.
// Each test creates its own topic and consumer group.

func setup(t *testing.T) (Config, *kgo.Client) {
	t.Helper()
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set; start the infra compose stack to run this test")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := Config{Brokers: strings.Split(brokers, ","), Group: "it-" + suffix, InputTopic: "it.in." + suffix}

	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	adm := kadm.NewClient(cl)
	// Same layout as infra/kafka/create-topics.sh: 3 partitions.
	if _, err := adm.CreateTopic(context.Background(), 3, 1, nil, cfg.InputTopic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(context.Background(), cfg.InputTopic) })
	return cfg, cl
}

func send(t *testing.T, cl *kgo.Client, cfg Config, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		r := &kgo.Record{
			Topic: cfg.InputTopic, Key: []byte(fmt.Sprintf("term-%03d", i%5)), Value: []byte(fmt.Sprintf("msg-%d", i)),
			Headers: []kgo.RecordHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		}
		if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

type run struct {
	cancel context.CancelFunc
	done   chan struct{}
	cons   *Consumer
}

// stop ends the run and leaves the group; unfinished records stay uncommitted.
func (r *run) stop() { r.cancel(); <-r.done; r.cons.Close() }

func start(t *testing.T, cfg Config, h Handler) *run {
	t.Helper()
	c, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel, done: make(chan struct{}), cons: c}
	go func() { defer close(r.done); _ = c.Run(ctx, h) }()
	return r
}

type collector struct {
	mu   sync.Mutex
	seen map[string]int
}

func newCollector() *collector { return &collector{seen: map[string]int{}} }

func (c *collector) add(v string) { c.mu.Lock(); c.seen[v]++; c.mu.Unlock() }

func (c *collector) distinct() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.seen) }

func (c *collector) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.seen {
		n += v
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestDeliversRecordsWithKeyAndHeaders(t *testing.T) {
	cfg, cl := setup(t)
	send(t, cl, cfg, 10)

	got := newCollector()
	var badHeader sync.Once
	r := start(t, cfg, HandlerFunc(func(_ context.Context, rec Record) error {
		if rec.Headers["trace_id"] != "trace-1" || !strings.HasPrefix(string(rec.Key), "term-") {
			badHeader.Do(func() { t.Errorf("record lost key or headers: %+v", rec) })
		}
		got.add(string(rec.Value))
		return nil
	}))
	defer r.stop()
	waitFor(t, "10 records", func() bool { return got.distinct() == 10 })
}

// No event is lost on restart: committed records are not read again, new ones are.
func TestRestartContinuesAfterCommittedOffset(t *testing.T) {
	cfg, cl := setup(t)
	send(t, cl, cfg, 10)

	first := newCollector()
	r := start(t, cfg, HandlerFunc(func(_ context.Context, rec Record) error { first.add(string(rec.Value)); return nil }))
	waitFor(t, "first 10", func() bool { return first.distinct() == 10 })
	r.stop() // the last batch is committed on the way out

	send(t, cl, cfg, 5) // msg-0..4 again: same text, but new records
	second := newCollector()
	r2 := start(t, cfg, HandlerFunc(func(_ context.Context, rec Record) error { second.add(string(rec.Value)); return nil }))
	defer r2.stop()
	waitFor(t, "5 new records", func() bool { return second.total() >= 5 })
	time.Sleep(time.Second)
	if n := second.total(); n != 5 {
		t.Errorf("second run read %d records, want exactly the 5 new ones", n)
	}
}

// A transient handler error retries the same record, it is neither skipped nor committed.
func TestHandlerErrorIsRetriedNotSkipped(t *testing.T) {
	cfg, cl := setup(t)
	send(t, cl, cfg, 3)

	got := newCollector()
	var mu sync.Mutex
	failures := 0
	r := start(t, cfg, HandlerFunc(func(_ context.Context, rec Record) error {
		mu.Lock()
		defer mu.Unlock()
		if string(rec.Value) == "msg-1" && failures < 2 {
			failures++
			return errors.New("postgres is down")
		}
		got.add(string(rec.Value))
		return nil
	}))
	defer r.stop()
	waitFor(t, "all records after retries", func() bool { return got.distinct() == 3 })
	mu.Lock()
	defer mu.Unlock()
	if failures != 2 {
		t.Errorf("failures = %d, want 2", failures)
	}
}

// Killing the service mid-processing: the record being handled is not committed and a new instance reads it.
func TestUnfinishedRecordIsRedeliveredAfterRestart(t *testing.T) {
	cfg, cl := setup(t)
	send(t, cl, cfg, 6)

	started := make(chan struct{})
	var once sync.Once
	var stuck *run
	stuck = start(t, cfg, HandlerFunc(func(ctx context.Context, rec Record) error {
		once.Do(func() { close(started) })
		<-ctx.Done() // the process dies while the first record is in flight
		return ctx.Err()
	}))
	<-started
	stuck.stop()

	got := newCollector()
	r := start(t, cfg, HandlerFunc(func(_ context.Context, rec Record) error { got.add(string(rec.Value)); return nil }))
	defer r.stop()
	waitFor(t, "all 6 records, including the interrupted one", func() bool { return got.distinct() == 6 })
}
