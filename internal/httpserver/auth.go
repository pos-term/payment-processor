package httpserver

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// bearerAuth rejects requests that do not carry "Authorization: Bearer <token>".
// Both sides are hashed first so the comparison takes the same time whatever the length of the guess.
func bearerAuth(token string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, got, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if ok && strings.EqualFold(scheme, "Bearer") {
			sum := sha256.Sum256([]byte(strings.TrimSpace(got)))
			if subtle.ConstantTimeCompare(sum[:], want[:]) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Bearer`)
		writeProblem(w, http.StatusUnauthorized, codeUnauthorized, "missing or wrong bearer token")
	})
}
