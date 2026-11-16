package httpserver

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
)

const readinessTimeout = time.Second

type readinessChecks struct {
	postgres func(context.Context) error
	kafka    func(context.Context) error
}

// WithReadinessChecks configures probes for the dependencies required to process payments.
func WithReadinessChecks(postgres, kafka func(context.Context) error) Option {
	return func(a *api) {
		a.readiness = readinessChecks{postgres: postgres, kafka: kafka}
	}
}

func (c readinessChecks) ready(ctx context.Context) bool {
	if c.postgres == nil || c.kafka == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, readinessTimeout)
	defer cancel()

	// Both checks share one time budget. A failure cancels the other check.
	group, probeCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return c.postgres(probeCtx) })
	group.Go(func() error { return c.kafka(probeCtx) })
	return group.Wait() == nil && ctx.Err() == nil
}

func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if a.readiness.ready(r.Context()) {
		_, _ = w.Write([]byte(`{"status":"ready"}`))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"status":"not_ready"}`))
}
