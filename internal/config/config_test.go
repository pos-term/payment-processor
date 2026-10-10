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
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "kafka2:9092" {
		t.Errorf("brokers not split and trimmed: %v", cfg.KafkaBrokers)
	}
}

func TestLoadOverrides(t *testing.T) {
	m := valid()
	m["HTTP_ADDR"] = "127.0.0.1:9000"
	m["LOG_LEVEL"] = "debug"
	m["SHUTDOWN_TIMEOUT"] = "3s"
	cfg, err := Load(env(m))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9000" || cfg.LogLevel != slog.LevelDebug || cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadReportsAllProblems(t *testing.T) {
	_, err := Load(env(map[string]string{
		"HTTP_ADDR":        "nonsense",
		"LOG_LEVEL":        "loud",
		"SHUTDOWN_TIMEOUT": "-1s",
		"POSTGRES_DSN":     "mysql://x",
		"REDIS_ADDR":       "redis",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"HTTP_ADDR", "LOG_LEVEL", "SHUTDOWN_TIMEOUT", "POSTGRES_DSN", "REDIS_ADDR", "KAFKA_BROKERS", "API_TOKEN"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}
