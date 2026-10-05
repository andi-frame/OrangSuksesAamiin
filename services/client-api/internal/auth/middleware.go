package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type ctxKey struct{}

func Role(ctx context.Context) string {
	r, _ := ctx.Value(ctxKey{}).(string)
	return r
}

func Middleware(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if h == "" {
				obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization Bearer token")
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(h, prefix) {
				obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization Bearer token")
				return
			}
			tok := strings.TrimSpace(h[len(prefix):])
			if tok == "" {
				obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization Bearer token")
				return
			}
			role, err := ParseBearer(secret, tok)
			if err != nil {
				obs.WriteError(w, http.StatusForbidden, "forbidden", "invalid or expired token")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, role)))
		})
	}
}
