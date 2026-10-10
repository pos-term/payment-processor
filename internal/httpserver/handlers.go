package httpserver

import "net/http"

// healthz reports liveness only: the process is up and serving. It needs no token.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
