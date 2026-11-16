package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadinessProbe(t *testing.T) {
	t.Parallel()

	ok := func(context.Context) error { return nil }
	fail := func(context.Context) error { return errors.New("dependency connection refused") }
	wait := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			// Make a missing probe deadline fail the assertion instead of hanging.
			return nil
		}
	}
	for _, tt := range []struct {
		name     string
		postgres func(context.Context) error
		kafka    func(context.Context) error
		status   int
		body     string
	}{
		{"ready", ok, ok, http.StatusOK, `{"status":"ready"}`},
		{"postgres down", fail, ok, http.StatusServiceUnavailable, `{"status":"not_ready"}`},
		{"kafka down", ok, fail, http.StatusServiceUnavailable, `{"status":"not_ready"}`},
		{"check times out", ok, wait, http.StatusServiceUnavailable, `{"status":"not_ready"}`},
		{"check missing", ok, nil, http.StatusServiceUnavailable, `{"status":"not_ready"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := New(":0", testToken, &fakeStore{}, discardLogger(), WithReadinessChecks(tt.postgres, tt.kafka))
			rec := httptest.NewRecorder()
			server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tt.status || rec.Body.String() != tt.body {
				t.Errorf("response = %d %q, want %d %q", rec.Code, rec.Body.String(), tt.status, tt.body)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
		})
	}
}
