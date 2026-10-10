// Package config loads and validates the service configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is the validated runtime configuration of the service.
type Config struct {
	HTTPAddr        string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	KafkaBrokers    []string
	PostgresDSN     string
	RedisAddr       string
	APIToken        string
	MigrateOnStart  bool
}

// Load reads the configuration through lookup (os.LookupEnv in production).
// All problems are collected and returned together, so a misconfigured
// deployment is fixed in one iteration.
func Load(lookup func(string) (string, bool)) (Config, error) {
	l := loader{lookup: lookup}

	cfg := Config{
		HTTPAddr:        l.str("HTTP_ADDR", ":8080"),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 10*time.Second),
		PostgresDSN:     l.required("POSTGRES_DSN"),
		RedisAddr:       l.required("REDIS_ADDR"),
		APIToken:        l.required("API_TOKEN"),
	}
	cfg.MigrateOnStart = l.boolean("MIGRATE_ON_START", true)
	cfg.LogLevel = l.logLevel("LOG_LEVEL", slog.LevelInfo)
	cfg.KafkaBrokers = l.list("KAFKA_BROKERS")

	if _, _, err := net.SplitHostPort(cfg.HTTPAddr); err != nil {
		l.fail("HTTP_ADDR", "must be host:port or :port, got %q", cfg.HTTPAddr)
	}
	if cfg.PostgresDSN != "" {
		if u, err := url.Parse(cfg.PostgresDSN); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
			l.fail("POSTGRES_DSN", "must be a postgres:// URL")
		}
	}
	if cfg.RedisAddr != "" {
		if _, _, err := net.SplitHostPort(cfg.RedisAddr); err != nil {
			l.fail("REDIS_ADDR", "must be host:port, got %q", cfg.RedisAddr)
		}
	}

	if len(l.errs) > 0 {
		return Config{}, errors.Join(l.errs...)
	}
	return cfg, nil
}

type loader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (l *loader) fail(key, format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
}

func (l *loader) str(key, def string) string {
	if v, ok := l.lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) required(key string) string {
	v, ok := l.lookup(key)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		l.fail(key, "is required")
		return ""
	}
	return v
}

func (l *loader) boolean(key string, def bool) bool {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, "must be true or false, got %q", v)
		return def
	}
	return b
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.fail(key, "must be a positive duration like 10s, got %q", v)
		return def
	}
	return d
}

func (l *loader) logLevel(key string, def slog.Level) slog.Level {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		l.fail(key, "must be one of debug, info, warn, error, got %q", v)
		return def
	}
	return lvl
}

func (l *loader) list(key string) []string {
	raw := l.required(key)
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
