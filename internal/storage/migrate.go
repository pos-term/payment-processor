// Package storage owns the Postgres schema and, later, data access.
package storage

import (
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies all pending migrations. It is safe to call from several
// instances at once: golang-migrate holds a Postgres advisory lock while it works.
func Migrate(dsn string, log *slog.Logger) error {
	m, err := newMigrator(dsn)
	if err != nil {
		return err
	}
	defer closeMigrator(m, log)

	before, _, _ := m.Version()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	after, dirty, err := m.Version()
	if err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	log.Info("migrations applied", "from", before, "to", after, "dirty", dirty)
	return nil
}

func newMigrator(dsn string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5(dsn))
	if err != nil {
		return nil, fmt.Errorf("connect for migrations: %w", err)
	}
	return m, nil
}

// toPgx5 maps the usual postgres:// URL onto the scheme the pgx v5 migrate driver registers.
func toPgx5(dsn string) string {
	for _, scheme := range []string{"postgresql://", "postgres://"} {
		if rest, ok := strings.CutPrefix(dsn, scheme); ok {
			return "pgx5://" + rest
		}
	}
	return dsn
}

func closeMigrator(m *migrate.Migrate, log *slog.Logger) {
	if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
		log.Warn("close migrator", "source_error", srcErr, "database_error", dbErr)
	}
}
