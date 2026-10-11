package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func valid() map[string]string {
	return map[string]string{
		"KAFKA_BROKERS": "kafka:9092, kafka2:9092",
		"POSTGRES_DSN":  "postgres://pos:pos@postgres:5432/pos?sslmode=disable",
		"REDIS_ADDR":    "redis:6379",
		"API_TOKEN":     "secret",
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(valid()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.LogLevel != slog.LevelInfo || cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if !cfg.MigrateOnStart {
		t.Error("MIGRATE_ON_START should default to true")
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "kafka2:9092" {
		t.Errorf("brokers not split and trimmed: %v", cfg.KafkaBrokers)
	}
}

func TestLoadOverrides(t *testing.T) {
	m := valid()
	m["HTTP_ADDR"] = "127.0.0.1:9000"
	m["LOG_LEVEL"] = "debug"
	m["SHUTDOWN_TIMEOUT"] = "3s"
	m["MIGRATE_ON_START"] = "false"
	cfg, err := Load(env(m))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9000" || cfg.LogLevel != slog.LevelDebug || cfg.ShutdownTimeout != 3*time.Second || cfg.MigrateOnStart {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadReportsAllProblems(t *testing.T) {
	_, err := Load(env(map[string]string{
		"HTTP_ADDR":        "nonsense",
		"LOG_LEVEL":        "loud",
		"SHUTDOWN_TIMEOUT": "-1s",
		"MIGRATE_ON_START": "maybe",
		"POSTGRES_DSN":     "mysql://x",
		"REDIS_ADDR":       "redis",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"HTTP_ADDR", "LOG_LEVEL", "SHUTDOWN_TIMEOUT", "MIGRATE_ON_START", "POSTGRES_DSN", "REDIS_ADDR", "KAFKA_BROKERS", "API_TOKEN"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestMaxAmountMinor(t *testing.T) {
	cfg, err := Load(env(valid()))
	if err != nil || cfg.MaxAmountMinor != 10_000_000 {
		t.Fatalf("default: %d, %v; want 10000000", cfg.MaxAmountMinor, err)
	}

	m := valid()
	m["MAX_AMOUNT_MINOR"] = "0"
	if cfg, err = Load(env(m)); err != nil || cfg.MaxAmountMinor != 0 {
		t.Errorf("0 disables the limit: %d, %v", cfg.MaxAmountMinor, err)
	}

	for _, bad := range []string{"-1", "1.5", "lots"} {
		m["MAX_AMOUNT_MINOR"] = bad
		if _, err := Load(env(m)); err == nil || !strings.Contains(err.Error(), "MAX_AMOUNT_MINOR") {
			t.Errorf("MAX_AMOUNT_MINOR=%q: err = %v, want a MAX_AMOUNT_MINOR error", bad, err)
		}
	}
}

func TestDedupTTL(t *testing.T) {
	cfg, err := Load(env(valid()))
	if err != nil || cfg.DedupTTL != 24*time.Hour {
		t.Fatalf("default: %v, %v; want 24h", cfg.DedupTTL, err)
	}
	m := valid()
	m["DEDUP_TTL"] = "90m"
	if cfg, err = Load(env(m)); err != nil || cfg.DedupTTL != 90*time.Minute {
		t.Errorf("override: %v, %v; want 90m", cfg.DedupTTL, err)
	}
	m["DEDUP_TTL"] = "-5s"
	if _, err := Load(env(m)); err == nil || !strings.Contains(err.Error(), "DEDUP_TTL") {
		t.Errorf("negative TTL: err = %v, want a DEDUP_TTL error", err)
	}
}
