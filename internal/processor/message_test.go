package processor

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const validTx = `{"schema_version":1,"transaction_id":"0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10","terminal_id":"term-001",` +
	`"amount_minor":15050,"currency":"RUB","created_at":"2026-09-19T10:15:30.123Z","seq":42}`

func TestParseValid(t *testing.T) {
	tx, err := Parse([]byte(validTx))
	if err != nil {
		t.Fatal(err)
	}
	if tx.TransactionID != "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10" || tx.TerminalID != "term-001" ||
		tx.AmountMinor != 15050 || tx.Currency != "RUB" || tx.Seq != 42 || !tx.CreatedAt.Equal(time.Date(2026, 9, 19, 10, 15, 30, 123_000_000, time.UTC)) {
		t.Errorf("unexpected transaction: %+v", tx)
	}
}

func TestParseRejects(t *testing.T) {
	replace := func(old, new string) string { return strings.Replace(validTx, old, new, 1) }
	cases := []struct {
		name, value, kind, wantID string
	}{
		{"not json", `{"schema_version":`, ErrInvalidJSON, ""},
		{"empty value", ``, ErrInvalidJSON, ""},
		{"json array", `[1]`, ErrInvalidJSON, ""},
		{"unknown schema version", replace(`"schema_version":1`, `"schema_version":2`), ErrUnsupportedSchemaVersion, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"string schema version", replace(`"schema_version":1`, `"schema_version":"1"`), ErrUnsupportedSchemaVersion, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"future version with other shape", `{"schema_version":2,"amount":"1.50"}`, ErrUnsupportedSchemaVersion, ""},
		{"missing schema version", replace(`"schema_version":1,`, ``), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"zero amount", replace(`15050`, `0`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"fractional amount", replace(`15050`, `150.5`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"amount as string", replace(`15050`, `"15050"`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"null amount", replace(`15050`, `null`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"null seq", replace(`"seq":42`, `"seq":null`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"negative seq", replace(`"seq":42`, `"seq":-1`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"missing seq", replace(`,"seq":42`, ``), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"uuid v4", replace(`7b21-9d4e`, `4b21-9d4e`), ErrValidationFailed, "0192f0c4-7a3e-4b21-9d4e-2c1f8a6b5e10"},
		{"uppercase uuid", replace(`0192f0c4`, `0192F0C4`), ErrValidationFailed, "0192F0C4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"bad terminal", replace(`term-001`, `term 001`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"lowercase currency", replace(`RUB`, `rub`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"bad created_at", replace(`2026-09-19T10:15:30.123Z`, `yesterday`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"extra field", replace(`"seq":42`, `"seq":42,"card":"4111"`), ErrValidationFailed, "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"},
		{"transaction_id not a string", replace(`"0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"`, `12`), ErrValidationFailed, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.value))
			var bad *BadMessageError
			if !errors.As(err, &bad) {
				t.Fatalf("err = %v, want *BadMessageError", err)
			}
			if bad.Kind != c.kind {
				t.Errorf("kind = %q (%s), want %q", bad.Kind, bad.Detail, c.kind)
			}
			if bad.TransactionID != c.wantID {
				t.Errorf("transaction id = %q, want %q", bad.TransactionID, c.wantID)
			}
		})
	}
}
