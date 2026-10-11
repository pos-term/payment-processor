package integration

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const terminals = 30

// instanceStore adds a per-instance count of decided payments to the shared one: it shows how the
// work was split between the instances of the group.
type instanceStore struct {
	*countingStore
	own atomic.Int64
}

func (s *instanceStore) SaveResult(ctx context.Context, id, status string, reason *string, at time.Time) error {
	s.own.Add(1)
	return s.countingStore.SaveResult(ctx, id, status, reason, at)
}

// startMember runs one more instance of the group and returns it with its own work counter.
func (e *dedupEnv) startMember(t *testing.T) (*instance, *instanceStore) {
	t.Helper()
	st := &instanceStore{countingStore: e.store}
	return e.start(t, st), st
}

// waitForMembers blocks until the group is stable with n members and all 3 partitions are assigned.
// Messages sent before that could all be read by the first member and prove nothing about scaling.
func (e *dedupEnv) waitForMembers(t *testing.T, n int) {
	t.Helper()
	adm := kadm.NewClient(e.cl)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		groups, err := adm.DescribeGroups(context.Background(), e.cfg.Group)
		if err == nil {
			if g, ok := groups[e.cfg.Group]; ok && g.State == "Stable" && len(g.Members) == n &&
				len(g.AssignedPartitions()[e.cfg.InputTopic]) == 3 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the group did not become stable with %d members", n)
}

// payment is the n-th payment of the test, sent by one of the terminals.
func paymentID(n int) string { return fmt.Sprintf("0192f0c4-7a3e-7b21-9d4e-%012x", 0xc0000+n) }

// sendPayments sends payments from..to, every one of them twice: the second copy is a terminal retry.
func (e *dedupEnv) sendPayments(t *testing.T, from, to int) {
	t.Helper()
	for n := from; n <= to; n++ {
		terminal := fmt.Sprintf("term-%03d", n%terminals)
		value := fmt.Sprintf(`{"schema_version":1,"transaction_id":"%s","terminal_id":"%s","amount_minor":1000,`+
			`"currency":"RUB","created_at":"2026-09-19T10:15:30.123Z","seq":%d}`, paymentID(n), terminal, n)
		for i := 0; i < 2; i++ {
			r := &kgo.Record{Topic: e.cfg.InputTopic, Key: []byte(terminal), Value: []byte(value)}
			if err := e.cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// waitDelivered blocks until n results are recorded as published.
func (e *dedupEnv) waitDelivered(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var got int
		if err := e.pg.QueryRow(context.Background(),
			"SELECT count(*) FROM transaction_results WHERE published_at IS NOT NULL").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got >= n {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("not all %d results were delivered in time", n)
}

// assertExactlyOnce checks the end state: n payments, each stored, decided and published exactly once.
func (e *dedupEnv) assertExactlyOnce(t *testing.T, n int) {
	t.Helper()
	e.assertOnceCount(t, n)
	got := e.resultCounts(t, n)
	if len(got) != n {
		t.Errorf("results for %d payments, want %d", len(got), n)
	}
	for id, c := range got {
		if c != 1 {
			t.Errorf("%s was published %d times, want 1", id, c)
		}
	}
}

func (e *dedupEnv) assertOnceCount(t *testing.T, n int) {
	t.Helper()
	var rows int
	if err := e.pg.QueryRow(context.Background(), "SELECT count(*) FROM transactions").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != n {
		t.Errorf("transactions = %d, want %d", rows, n)
	}
	if got := e.store.saves.Load(); got != int64(n) {
		t.Errorf("payments decided = %d, want %d: some payment was processed twice", got, n)
	}
}

// Three instances in one consumer group share the work and process nothing twice, although every
// payment is sent twice.
func TestThreeInstancesShareTheWorkWithoutDuplicates(t *testing.T) {
	const payments = 300
	e := setupInfra(t, os.Getenv("TEST_REDIS_ADDR"))
	var stores []*instanceStore
	for i := 0; i < 3; i++ {
		_, st := e.startMember(t)
		stores = append(stores, st)
	}
	e.waitForMembers(t, 3)

	e.sendPayments(t, 1, payments)
	e.waitDelivered(t, payments)
	e.assertExactlyOnce(t, payments)

	var total int64
	for i, st := range stores {
		own := st.own.Load()
		total += own
		if own == 0 {
			t.Errorf("instance %d decided nothing: the work was not shared", i)
		}
	}
	if total != payments {
		t.Errorf("instances decided %d payments together, want %d", total, payments)
	}
}

// Instances join and leave while payments flow: nothing is lost and nothing is processed twice.
func TestScalingOutAndInDuringTheFlow(t *testing.T) {
	const payments = 300
	e := setupInfra(t, os.Getenv("TEST_REDIS_ADDR"))
	first, _ := e.startMember(t)
	e.waitForMembers(t, 1)

	e.sendPayments(t, 1, 100)
	e.startMember(t)
	e.startMember(t)
	e.sendPayments(t, 101, 200)
	e.waitForMembers(t, 3)
	first.stop() // scale in: its partitions move to the other two
	e.sendPayments(t, 201, payments)

	e.waitDelivered(t, payments)
	e.assertExactlyOnce(t, payments)
}
