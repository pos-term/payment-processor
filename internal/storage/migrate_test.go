package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	txID1 = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e10"
	txID2 = "0192f0c4-7a3e-7b21-9d4e-2c1f8a6b5e11"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testDB creates an empty database on the server from TEST_POSTGRES_DSN and returns its DSN.
// The tests are skipped when the variable is not set, so plain `go test` needs no Postgres.
func testDB(t *testing.T) string {
	t.Helper()
	adminDSN := os.Getenv("TEST_POSTGRES_DSN")
	if adminDSN == "" {
		t.Skip("TEST_POSTGRES_DSN is not set; start the infra compose stack to run this test")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(ctx) })

	name := fmt.Sprintf("migrate_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)") })

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func tableExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var ok bool
	err := conn.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", name).Scan(&ok)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func insertTx(conn *pgx.Conn, id string) error {
	_, err := conn.Exec(context.Background(),
		`INSERT INTO transactions (transaction_id, terminal_id, amount_minor, currency, seq, created_at)
		 VALUES ($1, 'term-001', 15050, 'RUB', 42, '2026-09-19T10:15:30.123Z')`, id)
	return err
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestMigrateUpAndDown(t *testing.T) {
	dsn := testDB(t)
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatalf("up: %v", err)
	}
	// Running again is a no-op.
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatalf("second up: %v", err)
	}

	conn := connect(t, dsn)
	for _, name := range []string{"transactions", "transaction_results"} {
		if !tableExists(t, conn, name) {
			t.Errorf("table %s is missing after up", name)
		}
	}

	m, err := newMigrator(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMigrator(m, discardLogger())
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	for _, name := range []string{"transactions", "transaction_results"} {
		if tableExists(t, conn, name) {
			t.Errorf("table %s still exists after down", name)
		}
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up after down: %v", err)
	}
}

func TestDuplicateTransactionIDIsRejected(t *testing.T) {
	dsn := testDB(t)
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatal(err)
	}
	conn := connect(t, dsn)

	if err := insertTx(conn, txID1); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if got := sqlState(insertTx(conn, txID1)); got != "23505" {
		t.Errorf("duplicate insert: sqlstate = %q, want 23505 (unique_violation)", got)
	}
}

func TestTransactionConstraints(t *testing.T) {
	dsn := testDB(t)
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatal(err)
	}
	conn := connect(t, dsn)

	cases := map[string]string{
		"zero amount":       `VALUES ('%s', 'term-001', 0, 'RUB', 1, now())`,
		"lowercase ccy":     `VALUES ('%s', 'term-001', 100, 'rub', 1, now())`,
		"bad terminal id":   `VALUES ('%s', 'term 001!', 100, 'RUB', 1, now())`,
		"negative seq":      `VALUES ('%s', 'term-001', 100, 'RUB', -1, now())`,
		"empty terminal id": `VALUES ('%s', '', 100, 'RUB', 1, now())`,
	}
	for name, values := range cases {
		q := "INSERT INTO transactions (transaction_id, terminal_id, amount_minor, currency, seq, created_at) " +
			fmt.Sprintf(values, txID2)
		if got := sqlState(execErr(conn, q)); got != "23514" {
			t.Errorf("%s: sqlstate = %q, want 23514 (check_violation)", name, got)
		}
	}
}

func execErr(conn *pgx.Conn, q string) error {
	_, err := conn.Exec(context.Background(), q)
	return err
}

func TestResultConstraints(t *testing.T) {
	dsn := testDB(t)
	if err := Migrate(dsn, discardLogger()); err != nil {
		t.Fatal(err)
	}
	conn := connect(t, dsn)
	if err := insertTx(conn, txID1); err != nil {
		t.Fatal(err)
	}

	insert := func(id, status, reason string) error {
		var r any
		if reason != "" {
			r = reason
		}
		_, err := conn.Exec(context.Background(),
			`INSERT INTO transaction_results (transaction_id, status, reason, processed_at) VALUES ($1, $2, $3, now())`,
			id, status, r)
		return err
	}

	// Rejected combinations.
	if got := sqlState(insert(txID1, "success", "LIMIT_EXCEEDED")); got != "23514" {
		t.Errorf("success with reason: sqlstate = %q, want 23514", got)
	}
	if got := sqlState(insert(txID1, "failed", "")); got != "23514" {
		t.Errorf("failed without reason: sqlstate = %q, want 23514", got)
	}
	if got := sqlState(insert(txID1, "failed", "limit exceeded")); got != "23514" {
		t.Errorf("badly formatted reason: sqlstate = %q, want 23514", got)
	}
	if got := sqlState(insert(txID1, "pending", "")); got != "23514" {
		t.Errorf("pending is never stored: sqlstate = %q, want 23514", got)
	}
	if got := sqlState(insert(txID2, "success", "")); got != "23503" {
		t.Errorf("result without transaction: sqlstate = %q, want 23503 (foreign_key_violation)", got)
	}

	// Accepted, and only once per transaction.
	if err := insert(txID1, "failed", "LIMIT_EXCEEDED"); err != nil {
		t.Fatalf("valid result: %v", err)
	}
	if got := sqlState(insert(txID1, "success", "")); got != "23505" {
		t.Errorf("second result: sqlstate = %q, want 23505 (unique_violation)", got)
	}
}
