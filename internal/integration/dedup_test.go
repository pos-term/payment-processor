package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pos-term/payment-processor/internal/consumer"
	"github.com/pos-term/payment-processor/internal/processor"
	"github.com/pos-term/payment-processor/internal/storage"
)

// countingStore counts how many times a payment was decided, i.e. actually processed.
// The primary key alone would hide double processing: the second insert is silently ignored.
type countingStore struct {
	*storage.Store
	saves atomic.Int64
}

func (s *countingStore) SaveResult(ctx context.Context, id, status string, reason *string, at time.Time) error {
	s.saves.Add(1)
	return s.Store.SaveResult(ctx, id, status, reason, at)
}

type dedupEnv struct {
	store *countingStore
	pg    *pgx.Conn
	cl    *kgo.Client
	cfg   consumer.Config
	redis *redis.Client
}

// setupDedup wires Kafka, Postgres and Redis (at redisAddr) around a running processor.
func setupDedup(t *testing.T, redisAddr string) *dedupEnv {
	t.Helper()
	e := setupInfra(t, redisAddr)
	e.start(t, e.store)
	return e
}

// setupInfra creates the database, topics and clients; no processor runs yet.
func setupInfra(t *testing.T, redisAddr string) *dedupEnv {
	t.Helper()
	brokers, adminDSN := os.Getenv("TEST_KAFKA_BROKERS"), os.Getenv("TEST_POSTGRES_DSN")
	if brokers == "" || adminDSN == "" || os.Getenv("TEST_REDIS_ADDR") == "" {
		t.Skip("TEST_KAFKA_BROKERS, TEST_POSTGRES_DSN and TEST_REDIS_ADDR are required; start the infra compose stack")
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close(ctx) })
	dbName := "it_" + suffix
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP DATABASE "+dbName+" WITH (FORCE)") })
	u, _ := url.Parse(adminDSN)
	u.Path = "/" + dbName
	if err := storage.Migrate(u.String(), log); err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	pg, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close(ctx) })

	e := &dedupEnv{
		store: &countingStore{Store: st}, pg: pg,
		redis: redis.NewClient(&redis.Options{Addr: redisAddr, DialTimeout: 200 * time.Millisecond, MaxRetries: -1}),
		cfg: consumer.Config{
			Brokers: strings.Split(brokers, ","), Group: "it-" + suffix,
			InputTopic: "it.in." + suffix, DLQTopic: "it.dlq." + suffix, ResultsTopic: "it.out." + suffix,
		},
	}
	t.Cleanup(func() { e.redis.Close() })
	if e.cl, err = kgo.NewClient(kgo.SeedBrokers(e.cfg.Brokers...)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.cl.Close)
	adm := kadm.NewClient(e.cl)
	for topic, parts := range map[string]int32{e.cfg.InputTopic: 3, e.cfg.DLQTopic: 1, e.cfg.ResultsTopic: 1} {
		if _, err := adm.CreateTopic(ctx, parts, 1, nil, topic); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = adm.DeleteTopics(ctx, e.cfg.InputTopic, e.cfg.DLQTopic, e.cfg.ResultsTopic) })

	return e
}

// instance is one running processor, a member of the consumer group.
type instance struct {
	cancel context.CancelFunc
	done   chan struct{}
	cons   *consumer.Consumer
}

// stop ends the instance and leaves the group; records it did not finish stay uncommitted.
func (i *instance) stop() { i.cancel(); <-i.done; i.cons.Close() }

// start runs a processor on top of st and stops it when the test ends.
func (e *dedupEnv) start(t *testing.T, st processor.Store) *instance {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cons, err := consumer.New(e.cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	proc := processor.New(st, processor.NewRedisDeduper(e.redis, time.Hour), cons, 0, log)
	ctx, cancel := context.WithCancel(context.Background())
	i := &instance{cancel: cancel, done: make(chan struct{}), cons: cons}
	go func() { defer close(i.done); _ = cons.Run(ctx, proc) }()
	t.Cleanup(i.stop)
	return i
}

// resultCounts reads the results topic and counts messages per transaction_id. It waits for atLeast
// messages, then one more second so that an unwanted extra message has time to show up.
func (e *dedupEnv) resultCounts(t *testing.T, atLeast int) map[string]int {
	t.Helper()
	reader, err := kgo.NewClient(kgo.SeedBrokers(e.cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{e.cfg.ResultsTopic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	counts, total := map[string]int{}, 0
	poll := func() {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		reader.PollFetches(c).EachRecord(func(r *kgo.Record) {
			var m struct {
				TransactionID string `json:"transaction_id"`
			}
			_ = json.Unmarshal(r.Value, &m)
			counts[m.TransactionID]++
			total++
		})
	}
	for end := time.Now().Add(60 * time.Second); total < atLeast && time.Now().Before(end); {
		poll()
	}
	poll()
	return counts
}

func (e *dedupEnv) send(t *testing.T, id string) {
	t.Helper()
	// One terminal means one partition, so messages are handled in the order they are sent.
	r := &kgo.Record{Topic: e.cfg.InputTopic, Key: []byte("term-001"), Value: []byte(txJSON(id, 1000))}
	if err := e.cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func (e *dedupEnv) waitResult(t *testing.T, id string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := e.store.GetResult(context.Background(), id); ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no result for %s in time", id)
}

func (e *dedupEnv) assertOnce(t *testing.T, ids ...string) {
	t.Helper()
	var rows int
	if err := e.pg.QueryRow(context.Background(), "SELECT count(*) FROM transactions").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != len(ids) {
		t.Errorf("transactions = %d, want %d", rows, len(ids))
	}
	if got := e.store.saves.Load(); got != int64(len(ids)) {
		t.Errorf("payments decided = %d, want %d: a duplicate was processed again", got, len(ids))
	}
}

const (
	idA = "0192f0c4-7a3e-7b21-9d4e-00000000a001"
	idB = "0192f0c4-7a3e-7b21-9d4e-00000000a002"
	idC = "0192f0c4-7a3e-7b21-9d4e-00000000a003"
)

// The same transaction_id twice stores one row and processes the payment once.
func TestDuplicateIsProcessedOnce(t *testing.T) {
	e := setupDedup(t, os.Getenv("TEST_REDIS_ADDR"))
	e.send(t, idA)
	e.send(t, idA) // retry from the terminal
	e.send(t, idB) // sentinel behind the duplicate on the same partition
	e.waitResult(t, idB)

	e.assertOnce(t, idA, idB)
	if got := e.resultCounts(t, 2); got[idA] != 1 || got[idB] != 1 || len(got) != 2 {
		t.Errorf("published results = %v, want one for each of the two payments", got)
	}
	// Redis really remembered the id: that is what filtered the retry.
	if n, err := e.redis.Exists(context.Background(), "dedup:tx:"+idA).Result(); err != nil || n != 1 {
		t.Errorf("redis key for %s: exists=%d err=%v, want 1", idA, n, err)
	}
}

// Flushing Redis does not lead to double processing: Postgres still knows the transaction.
func TestRedisFlushDoesNotCauseDoubleProcessing(t *testing.T) {
	e := setupDedup(t, os.Getenv("TEST_REDIS_ADDR"))
	e.send(t, idA)
	e.waitResult(t, idA)

	// Only our keys are removed: the Redis of the compose stack may be shared with other tests.
	if err := e.redis.Del(context.Background(), "dedup:tx:"+idA).Err(); err != nil {
		t.Fatal(err)
	}
	e.send(t, idA)
	e.send(t, idC)
	e.waitResult(t, idC)

	e.assertOnce(t, idA, idC)
	if got := e.resultCounts(t, 2); got[idA] != 1 || got[idC] != 1 || len(got) != 2 {
		t.Errorf("published results = %v, want one for each of the two payments", got)
	}
}

// Redis is only a filter: with it unreachable the same guarantees hold, backed by Postgres alone.
func TestRedisDownFallsBackToPostgres(t *testing.T) {
	e := setupDedup(t, "127.0.0.1:1") // nothing listens there
	e.send(t, idA)
	e.send(t, idA)
	e.send(t, idB)
	e.waitResult(t, idB)

	e.assertOnce(t, idA, idB)
	if got := e.resultCounts(t, 2); got[idA] != 1 || got[idB] != 1 || len(got) != 2 {
		t.Errorf("published results = %v, want one for each of the two payments", got)
	}
}

// hookedStore breaks MarkPublished once for one transaction, like a crash after the publish.
type hookedStore struct {
	*countingStore
	crashOn string
	crashed atomic.Bool
	onCrash func()
}

func (s *hookedStore) MarkPublished(ctx context.Context, id string) error {
	if id == s.crashOn && s.crashed.CompareAndSwap(false, true) {
		s.onCrash()
		return fmt.Errorf("simulated crash after the publish")
	}
	return s.countingStore.MarkPublished(ctx, id)
}

// The service dies after the result reached Kafka but before it was recorded and the offset committed.
// Nothing is lost: the restarted instance publishes that one result again, decides nothing a second
// time, and every other result stays single.
func TestCrashAfterPublishLosesNoResult(t *testing.T) {
	e := setupInfra(t, os.Getenv("TEST_REDIS_ADDR"))

	var first *instance
	hooked := &hookedStore{countingStore: e.store, crashOn: idB}
	hooked.onCrash = func() { go first.cancel() }
	first = e.start(t, hooked)

	e.send(t, idA)
	e.send(t, idB) // the crash happens while this one is being delivered
	e.send(t, idC)

	deadline := time.Now().Add(30 * time.Second)
	for !hooked.crashed.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !hooked.crashed.Load() {
		t.Fatal("the crash point was never reached")
	}
	first.stop()

	// The replacement joins the same group and continues from the last committed offset.
	e.start(t, e.store)
	e.waitResult(t, idC)
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if r, ok, _ := e.store.GetResult(context.Background(), idC); ok && r.Published {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	e.assertOnce(t, idA, idB, idC)
	got := e.resultCounts(t, 4)
	if got[idA] != 1 || got[idB] != 2 || got[idC] != 1 || len(got) != 3 {
		t.Errorf("published results = %v, want A once, B twice (the replay), C once", got)
	}
	for _, id := range []string{idA, idB, idC} {
		if r, ok, _ := e.store.GetResult(context.Background(), id); !ok || !r.Published {
			t.Errorf("%s: result must be recorded as published after the replay: %+v", id, r)
		}
	}
}
