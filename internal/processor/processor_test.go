package processor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/pos-term/payment-processor/internal/consumer"
)

type memDLQ struct {
	letters []consumer.DeadLetter
	err     error
}

func (m *memDLQ) PublishDeadLetter(_ context.Context, d consumer.DeadLetter) error {
	if m.err != nil {
		return m.err
	}
	m.letters = append(m.letters, d)
	return nil
}

func newProc(dlq *memDLQ) *Processor {
	return New(dlq, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func rec(value string) consumer.Record {
	return consumer.Record{Topic: "pos.transactions.v1", Partition: 1, Offset: 10, Key: []byte("term-001"),
		Value: []byte(value), Headers: map[string]string{"trace_id": "trace-1"}}
}

func TestBadMessagesGoToDLQAndAreDone(t *testing.T) {
	dlq := &memDLQ{}
	p := newProc(dlq)
	for _, v := range []string{
		`{"schema_version":1,"amount_minor":0}`,
		`not json`,
		`{"schema_version":7}`,
	} {
		if err := p.Handle(context.Background(), rec(v)); err != nil {
			t.Fatalf("Handle(%q) = %v, want nil so the offset is committed", v, err)
		}
	}

	if len(dlq.letters) != 3 {
		t.Fatalf("dead letters = %d, want 3", len(dlq.letters))
	}
	want := []string{ErrValidationFailed, ErrInvalidJSON, ErrUnsupportedSchemaVersion}
	for i, w := range want {
		if dlq.letters[i].Kind != w {
			t.Errorf("dead letter %d kind = %q, want %q", i, dlq.letters[i].Kind, w)
		}
	}
	if string(dlq.letters[1].Record.Value) != "not json" {
		t.Error("the original payload must be kept unchanged")
	}
}

func TestValidMessageIsNotDeadLettered(t *testing.T) {
	dlq := &memDLQ{}
	if err := newProc(dlq).Handle(context.Background(), rec(validTx)); err != nil {
		t.Fatal(err)
	}
	if len(dlq.letters) != 0 {
		t.Errorf("a valid transaction went to the DLQ: %+v", dlq.letters)
	}
}

// If the DLQ itself is unavailable the offset must stay uncommitted, otherwise the message is lost.
func TestDLQFailureIsReturnedSoTheOffsetIsNotCommitted(t *testing.T) {
	boom := errors.New("kafka is down")
	err := newProc(&memDLQ{err: boom}).Handle(context.Background(), rec(`not json`))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the publish error", err)
	}
}
