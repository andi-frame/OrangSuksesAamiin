package resilience

import (
	"sync"
	"time"
)

// Status is what /internal/sources/status reports for one source.
//
//	state: "unknown" (never polled) | "up" | "degraded" (recent failures, breaker
//	       still closed) | "down" (breaker open / half-open)
//
// StaleSince is set whenever the source is not "up": it is the moment of the last
// successful sync, i.e. the data served for that source is at least that old.
// The Client-Facing API uses it to mark volcanic data as stale during an outage.
type Status struct {
	Source              string     `json:"source"`
	State               string     `json:"state"`
	Breaker             string     `json:"breaker"`
	LastSuccessAt       *time.Time `json:"last_success_at"`
	LastAttemptAt       *time.Time `json:"last_attempt_at"`
	StaleSince          *time.Time `json:"stale_since"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastError           string     `json:"last_error,omitempty"`
	LastLatencyMs       float64    `json:"last_latency_ms"`
	Polls               int64      `json:"polls"`
	Failures            int64      `json:"failures"`
	SkippedByBreaker    int64      `json:"skipped_by_breaker"`
	Quarantined         int64      `json:"quarantined"`
}

// Tracker accumulates the health of one source.
type Tracker struct {
	name    string
	breaker *Breaker
	now     func() time.Time

	mu sync.Mutex
	s  Status
}

func NewTracker(name string, b *Breaker) *Tracker {
	return &Tracker{name: name, breaker: b, now: time.Now, s: Status{Source: name}}
}

func (t *Tracker) Success(latency time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.now().UTC()
	t.s.Polls++
	t.s.LastAttemptAt, t.s.LastSuccessAt = &n, &n
	t.s.ConsecutiveFailures, t.s.LastError = 0, ""
	t.s.LastLatencyMs = float64(latency.Microseconds()) / 1000
}

func (t *Tracker) Failure(err error, latency time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.now().UTC()
	t.s.Polls++
	t.s.Failures++
	t.s.LastAttemptAt = &n
	t.s.ConsecutiveFailures++
	t.s.LastError = err.Error()
	t.s.LastLatencyMs = float64(latency.Microseconds()) / 1000
}

func (t *Tracker) Skipped() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.SkippedByBreaker++
}

func (t *Tracker) AddQuarantined(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.s.Quarantined += int64(n)
}

// Snapshot returns the derived status.
func (t *Tracker) Snapshot() Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.s
	s.Breaker = t.breaker.State()
	switch {
	case s.LastAttemptAt == nil && s.SkippedByBreaker == 0:
		s.State = "unknown"
	case s.Breaker != "closed":
		s.State = "down"
	case s.ConsecutiveFailures > 0:
		s.State = "degraded"
	default:
		s.State = "up"
	}
	s.StaleSince = nil
	if s.State != "up" && s.State != "unknown" && s.LastSuccessAt != nil {
		v := *s.LastSuccessAt
		s.StaleSince = &v
	}
	return s
}
