package httpserver

import "net/http"

// routes is the single place where URL patterns are bound to handlers.
// Patterns use the Go 1.22+ "METHOD /path" syntax of net/http.ServeMux.
func routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	return mux
}
