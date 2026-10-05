package limit

import (
	"net/http"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"golang.org/x/time/rate"
)

type Limiter struct {
	lim *rate.Limiter
}

func New(rps, burst int) *Limiter {
	if rps <= 0 {
		rps = 1000
	}
	if burst <= 0 {
		burst = rps * 2
	}
	return &Limiter{lim: rate.NewLimiter(rate.Limit(rps), burst)}
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.lim.Allow() {
			w.Header().Set("Retry-After", "1")
			obs.WriteError(w, http.StatusTooManyRequests, "too_many_requests", "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}
