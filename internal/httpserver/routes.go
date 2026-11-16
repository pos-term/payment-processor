package httpserver

import (
	"log/slog"
	"net/http"
)

// WithMetrics enables a bearer-protected Prometheus endpoint.
func WithMetrics(handler http.Handler) Option { return func(a *api) { a.metrics = handler } }

// routes is the single place where URL patterns are bound to handlers.
// Patterns use the Go 1.22+ "METHOD /path" syntax of net/http.ServeMux.
// The /v1/ routes and metrics require the bearer token. Probes are public.
func routes(token string, store Store, log *slog.Logger, options ...Option) http.Handler {
	a := &api{store: store, log: log}
	for _, option := range options {
		option(a)
	}

	protected := http.NewServeMux()
	protected.HandleFunc("GET /v1/transactions/stream", a.streamTransactions)
	protected.HandleFunc("GET /v1/transactions", a.listTransactions)
	protected.HandleFunc("GET /v1/transactions/{transaction_id}", a.getTransaction)
	protected.HandleFunc("GET /v1/terminals", a.listTerminals)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	if a.metrics != nil {
		mux.Handle("GET /metrics", bearerAuth(token, a.metrics))
	}
	mux.Handle("/v1/", bearerAuth(token, protected))
	return mux
}
