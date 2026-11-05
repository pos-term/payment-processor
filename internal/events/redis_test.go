package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pos-term/payment-processor/internal/storage"
	"github.com/redis/go-redis/v9"
)

type snapshotStore struct {
	view storage.TransactionView
	err  error
}

func (s *snapshotStore) GetTransaction(context.Context, string) (storage.TransactionView, bool, error) {
	return s.view, true, s.err
}

func journal(t *testing.T) (*Redis, *snapshotStore) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is required")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	key := fmt.Sprintf("sse_test:%d", time.Now().UnixNano())
	s := &snapshotStore{view: storage.TransactionView{ID: "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10", TerminalID: "term-001", Status: "pending", Currency: "RUB", AmountMinor: 100, CreatedAt: time.Now().UTC()}}
	t.Cleanup(func() { client.Del(context.Background(), key, key+":epoch"); client.Close() })
	return New(client, s, key), s
}
func read(t *testing.T, r *Redis, cursor string) Batch {
	t.Helper()
	b, err := r.Read(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func publish(t *testing.T, r *Redis) {
	t.Helper()
	if err := r.TransactionUpdated(context.Background(), "id"); err != nil {
		t.Fatal(err)
	}
}

func TestJournalSharedFanoutAndReplay(t *testing.T) {
	writer, s := journal(t)
	reader := New(writer.client, s, writer.key)
	initial := read(t, reader, "")
	publish(t, writer)
	pending := read(t, reader, initial.Cursor)
	if len(pending.Events) != 2 || pending.Events[1].Transaction.Status != "pending" {
		t.Fatalf("pending=%+v", pending)
	}
	after := pending.Events[1].ID
	at := time.Now().UTC()
	s.view.ProcessedAt = &at
	s.view.Status = "success"
	publish(t, writer)
	for _, instance := range []*Redis{reader, New(writer.client, s, writer.key)} {
		b := read(t, instance, after)
		if b.Resync || len(b.Events) != 1 || b.Events[0].Transaction.Status != "success" || b.Events[0].Transaction.ProcessedAt == nil {
			t.Fatalf("replay=%+v", b)
		}
	}
}

func TestJournalTrimLossAndUnknownCursorResync(t *testing.T) {
	r, _ := journal(t)
	initial := read(t, r, "")
	publish(t, r)
	if err := r.client.XTrimMaxLen(context.Background(), r.key, 1).Err(); err != nil {
		t.Fatal(err)
	}
	if b := read(t, r, initial.Cursor); !b.Resync {
		t.Fatal("trim must resync")
	}
	cursor := read(t, r, "").Cursor
	if err := r.client.Del(context.Background(), r.key).Err(); err != nil {
		t.Fatal(err)
	}
	if b := read(t, r, cursor); !b.Resync || b.Cursor == cursor {
		t.Fatalf("lost journal=%+v", b)
	}
	for _, cursor := range []string{"garbage", strings.Repeat("a", 32) + "/1-0", strings.Split(read(t, r, "").Cursor, "/")[0] + "/9999999999999999-0", strings.Split(read(t, r, "").Cursor, "/")[0] + "/0-0"} {
		if b := read(t, r, cursor); !b.Resync {
			t.Fatalf("cursor %s must resync", cursor)
		}
	}
}

func TestJournalFailureInvalidatesNextPublication(t *testing.T) {
	r, s := journal(t)
	initial := read(t, r, "")
	publish(t, r)
	after := read(t, r, initial.Cursor).Cursor
	s.err = errors.New("snapshot unavailable")
	if err := r.TransactionUpdated(context.Background(), "id"); err == nil {
		t.Fatal("expected error")
	}
	s.err = nil
	publish(t, r)
	b := read(t, r, after)
	if len(b.Events) != 2 || b.Events[0].Kind != "resync" || b.Events[1].Kind != "transaction.updated" {
		t.Fatalf("recovery=%+v", b)
	}
}

func TestJournalInvalidationRecoversWithoutAnotherPayment(t *testing.T) {
	r, _ := journal(t)
	initial := read(t, r, "")
	if err := r.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := read(t, r, initial.Cursor).Cursor
	r.Invalidate()
	if err := r.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := read(t, r, after)
	if len(b.Events) != 1 || b.Events[0].Kind != "resync" {
		t.Fatalf("invalidation=%+v", b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
}
