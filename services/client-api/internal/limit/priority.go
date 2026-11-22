package limit

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"golang.org/x/time/rate"
)

// Priority is admission control with two classes (M2 Problem 5, criterion 5):
// when the load exceeds capacity, Tim Lapangan (and Pusat) keep being served
// while Media is refused in a controlled way (429 + Retry-After).
//
// Capacity is bounded twice:
//   - rate: one token bucket for everybody. A low-priority request needs the
//     bucket to hold more than the reserved share; a high-priority one only
//     needs one token. Under a flood the bucket hovers near empty, which is
//     exactly the reserve Media cannot touch.
//   - concurrency: requests in flight. Low priority may use at most
//     (1 - reserve) of MaxInFlight, so a slow Aggregator (requests pile up)
//     sheds Media first instead of queueing everybody.
//
// High priority is refused only when there is no capacity at all (no token
// or MaxInFlight reached). Every refusal is a controlled 429 + Retry-After (the
// M1 contract of this API); the metric tells the two apart: outcome "shed"
// (lower priority, capacity kept for others) vs "rejected" (no capacity left).
type Priority struct {
	lim         *rate.Limiter
	reserve     float64 // share of the bucket / in-flight slots kept for high priority
	burst       float64
	maxInFlight int64
	inFlight    atomic.Int64
	roleOf      func(context.Context) string
	high        map[string]bool
}

// NewPriority: rps/burst size the bucket, reserve in (0,1), maxInFlight > 0.
func NewPriority(rps, burst int, reserve float64, maxInFlight int, roleOf func(context.Context) string, highRoles ...string) *Priority {
	if rps <= 0 {
		rps = 1000
	}
	if burst <= 0 {
		burst = rps * 2
	}
	if reserve <= 0 || reserve >= 1 {
		reserve = 0.3
	}
	if maxInFlight <= 0 {
		maxInFlight = 512
	}
	h := map[string]bool{}
	for _, r := range highRoles {
		h[r] = true
	}
	return &Priority{lim: rate.NewLimiter(rate.Limit(rps), burst), reserve: reserve, burst: float64(burst),
		maxInFlight: int64(maxInFlight), roleOf: roleOf, high: h}
}

var admission = obs.NewCounter("client_api_admission_total",
	"Admission decisions per client role: admitted, shed (429, lower priority) or rejected (429, no capacity at all).",
	"role", "outcome")

// Admit decides for one request. release must be called when it is done.
func (p *Priority) Admit(role string) (release func(), code int) {
	high := p.high[role]
	n := p.inFlight.Add(1)
	release = func() { p.inFlight.Add(-1) }
	lowCap := int64(float64(p.maxInFlight) * (1 - p.reserve))
	switch {
	case !high && (n > lowCap || p.lim.Tokens() < p.reserve*p.burst+1):
		release()
		admission.Inc(role, "shed")
		return nil, http.StatusTooManyRequests
	case n > p.maxInFlight || !p.lim.Allow():
		release()
		if high {
			admission.Inc(role, "rejected")
			return nil, http.StatusTooManyRequests
		}
		admission.Inc(role, "shed")
		return nil, http.StatusTooManyRequests
	}
	admission.Inc(role, "admitted")
	return release, http.StatusOK
}

// Middleware must run after authentication (it needs the role).
func (p *Priority) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := p.roleOf(r.Context())
		release, code := p.Admit(role)
		if release == nil {
			w.Header().Set("Retry-After", "1")
			obs.WriteError(w, code, "too_many_requests", "overloaded: request shed by admission control, retry later")
			return
		}
		defer release()
		next.ServeHTTP(w, r)
	})
}

// InFlight is the current number of admitted requests (tests, /metrics).
func (p *Priority) InFlight() int64 { return p.inFlight.Load() }

// String describes the configuration (startup log).
func (p *Priority) String() string {
	return "reserve=" + strconv.FormatFloat(p.reserve, 'f', 2, 64) + " max_in_flight=" + strconv.FormatInt(p.maxInFlight, 10)
}
