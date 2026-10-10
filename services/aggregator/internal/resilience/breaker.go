// Package resilience protects the Aggregator from a slow or dead source: a
// circuit breaker per source and a status tracker that tells the rest of the
// system how fresh each source's data is.
package resilience

import (
	"sync"
	"time"
)

// Breaker is a small circuit breaker: after `threshold` consecutive failures it
// opens for `openFor` (calls are skipped, so a dead PVMBG costs nothing), then
// lets one probe through (half-open); a success closes it, a failure re-opens.
type Breaker struct {
	threshold int
	openFor   time.Duration
	now       func() time.Time

	mu       sync.Mutex
	failures int
	openedAt time.Time
	open     bool
	probing  bool
}

func NewBreaker(threshold int, openFor time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, openFor: openFor, now: time.Now}
}

// Allow reports whether a call may go out now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return true
	}
	if b.now().Sub(b.openedAt) < b.openFor {
		return false
	}
	if b.probing { // a probe is already in flight
		return false
	}
	b.probing = true
	return true
}

// Success closes the breaker.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures, b.open, b.probing = 0, false, false
}

// Failure records a failed call and opens the breaker when the threshold is hit
// (or when a half-open probe fails).
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.probing || b.failures >= b.threshold {
		b.open, b.openedAt, b.probing = true, b.now(), false
	}
}

// State is "closed", "open" or "half-open".
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case !b.open:
		return "closed"
	case b.probing || b.now().Sub(b.openedAt) >= b.openFor:
		return "half-open"
	default:
		return "open"
	}
}
