package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type ctxKey struct{}

func Role(ctx context.Context) string {
	r, _ := ctx.Value(ctxKey{}).(string)
	return r
}

// unauthorized answers 401 with the challenge RFC 7235 requires.
func unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	obs.WriteError(w, http.StatusUnauthorized, "unauthorized", msg)
}

func Middleware(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if h == "" {
				unauthorized(w, "missing Authorization Bearer token")
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) {
				unauthorized(w, "missing Authorization Bearer token")
				return
			}
			tok := strings.TrimSpace(h[len(prefix):])
			if tok == "" {
				unauthorized(w, "missing Authorization Bearer token")
				return
			}
			role, err := ParseBearer(secret, tok)
			if errors.Is(err, ErrUnknownRole) {
				obs.WriteError(w, http.StatusForbidden, "forbidden", "token role is not allowed")
				return
			}
			if err != nil {
				unauthorized(w, "invalid or expired token")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, role)))
		})
	}
}
