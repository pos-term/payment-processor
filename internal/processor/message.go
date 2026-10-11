// Package processor holds the transaction processing logic. It knows nothing about Kafka clients:
// the consumer package feeds it records and gives it the ports it needs.
package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// SchemaVersion is the only supported value of schema_version in pos.transactions.v1.
const SchemaVersion = 1

// DLQ error kinds, the value of the `error` header (see infra/docs/kafka).
const (
	ErrInvalidJSON              = "invalid_json"
	ErrValidationFailed         = "validation_failed"
	ErrUnsupportedSchemaVersion = "unsupported_schema_version"
)

var (
	uuidV7Re     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	terminalIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	currencyRe   = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Transaction is a validated pos.transactions.v1 message.
type Transaction struct {
	TransactionID string
	TerminalID    string
	AmountMinor   int64
	Currency      string
	Seq           int64
	CreatedAt     time.Time
}

// BadMessageError is a failure that a retry cannot fix: the message goes to the DLQ.
type BadMessageError struct {
	Kind          string // one of the Err* constants
	Detail        string
	TransactionID string // best effort, empty when unreadable
}

func (e *BadMessageError) Error() string { return e.Kind + ": " + e.Detail }

// raw mirrors the wire format with pointers, so a missing field differs from a zero value.
type raw struct {
	SchemaVersion json.RawMessage `json:"schema_version"` // already checked by the probe
	TransactionID *string         `json:"transaction_id"`
	TerminalID    *string         `json:"terminal_id"`
	AmountMinor   *int64          `json:"amount_minor"`
	Currency      *string         `json:"currency"`
	CreatedAt     *string         `json:"created_at"`
	Seq           *int64          `json:"seq"`
}

// Parse decodes and validates a pos.transactions.v1 value. Every error is a *BadMessageError.
func Parse(value []byte) (Transaction, error) {
	var probe struct {
		SchemaVersion json.RawMessage `json:"schema_version"`
		TransactionID json.RawMessage `json:"transaction_id"`
	}
	if err := json.Unmarshal(value, &probe); err != nil {
		return Transaction{}, &BadMessageError{Kind: ErrInvalidJSON, Detail: err.Error()}
	}
	id := readableID(probe.TransactionID)

	bad := func(format string, args ...any) (Transaction, error) {
		return Transaction{}, &BadMessageError{Kind: ErrValidationFailed, Detail: fmt.Sprintf(format, args...), TransactionID: id}
	}

	// The version is checked before the rest: a future version may change any other field.
	if string(bytes.TrimSpace(probe.SchemaVersion)) != "1" {
		if len(probe.SchemaVersion) == 0 {
			return bad("schema_version is required")
		}
		return Transaction{}, &BadMessageError{
			Kind: ErrUnsupportedSchemaVersion, Detail: "schema_version " + string(probe.SchemaVersion) + " is not supported", TransactionID: id,
		}
	}

	var m raw
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return bad("%v", err)
	}

	var tx Transaction
	switch {
	case m.TransactionID == nil || !uuidV7Re.MatchString(*m.TransactionID):
		return bad("transaction_id must be a lowercase UUIDv7")
	case m.TerminalID == nil || !terminalIDRe.MatchString(*m.TerminalID):
		return bad("terminal_id must match %s", terminalIDRe)
	case m.Currency == nil || !currencyRe.MatchString(*m.Currency):
		return bad("currency must be an ISO 4217 code like RUB")
	}
	tx.TransactionID, tx.TerminalID, tx.Currency = *m.TransactionID, *m.TerminalID, *m.Currency

	if m.AmountMinor == nil || *m.AmountMinor < 1 {
		return bad("amount_minor must be an integer > 0")
	}
	if m.Seq == nil || *m.Seq < 0 {
		return bad("seq must be an integer >= 0")
	}
	tx.AmountMinor, tx.Seq = *m.AmountMinor, *m.Seq
	if m.CreatedAt == nil {
		return bad("created_at is required")
	}
	var err error
	if tx.CreatedAt, err = time.Parse(time.RFC3339, *m.CreatedAt); err != nil {
		return bad("created_at must be RFC 3339: %v", err)
	}
	tx.CreatedAt = tx.CreatedAt.UTC()
	return tx, nil
}

// readableID returns the transaction_id when it is a JSON string, so the DLQ message can be keyed by it.
func readableID(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil || len(s) > 64 {
		return ""
	}
	return s
}
