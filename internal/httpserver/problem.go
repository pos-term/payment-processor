package httpserver

import (
	"encoding/json"
	"net/http"
)

// Stable machine-readable error codes of the API (the Problem.code enum in openapi.yaml).
const (
	codeInvalidParameter = "invalid_parameter"
	codeUnauthorized     = "unauthorized"
	codeNotFound         = "not_found"
	codeInternal         = "internal_error"
)

// problem is an RFC 9457 error body.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Code   string `json:"code"`
}

func writeProblem(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
		Code:   code,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
