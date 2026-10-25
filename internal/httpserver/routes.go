package httpserver

import (
	"log/slog"
	"net/http"
)

// routes is the single place where URL patterns are bound to handlers.
// Patterns use the Go 1.22+ "METHOD /path" syntax of net/http.ServeMux.
// Everything except /healthz requires the bearer token.
func routes(token string, store Store, log *slog.Logger) http.Handler {
	a := &api{store: store, log: log}

	protected := http.NewServeMux()
	protected.HandleFunc("GET /v1/transactions", a.listTransactions)
	protected.HandleFunc("GET /v1/transactions/{transaction_id}", a.getTransaction)
	protected.HandleFunc("GET /v1/terminals", a.listTerminals)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.Handle("/v1/", bearerAuth(token, protected))
	return mux
}
