package resilience

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	clock := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	b := NewBreaker(3, 10*time.Second)
	b.now = func() time.Time { return clock }

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatal("closed breaker must allow")
		}
		b.Failure()
	}
	if b.State() != "closed" {
		t.Fatalf("2 failures < threshold: %s", b.State())
	}
	b.Failure()
	if b.State() != "open" || b.Allow() {
		t.Fatalf("3 failures must open the breaker: %s", b.State())
	}

	clock = clock.Add(9 * time.Second)
	if b.Allow() {
		t.Fatal("still open before openFor elapsed")
	}
	clock = clock.Add(2 * time.Second)
	if b.State() != "half-open" {
		t.Fatalf("after openFor: %s", b.State())
	}
	if !b.Allow() {
		t.Fatal("half-open must let one probe through")
	}
	if b.Allow() {
		t.Fatal("only ONE probe while half-open")
	}
	b.Failure() // probe failed -> re-open
	if b.State() != "open" {
		t.Fatalf("failed probe must re-open: %s", b.State())
	}
	clock = clock.Add(11 * time.Second)
	if !b.Allow() {
		t.Fatal("second probe")
	}
	b.Success()
	if b.State() != "closed" || !b.Allow() {
		t.Fatalf("successful probe must close: %s", b.State())
	}
}

func TestTrackerStateAndStaleSince(t *testing.T) {
	b := NewBreaker(2, time.Minute)
	tr := NewTracker("PVMBG", b)
	if tr.Snapshot().State != "unknown" {
		t.Fatalf("initial: %s", tr.Snapshot().State)
	}
	tr.Success(20 * time.Millisecond)
	b.Success()
	s := tr.Snapshot()
	if s.State != "up" || s.StaleSince != nil || s.LastSuccessAt == nil {
		t.Fatalf("after success: %+v", s)
	}
	okAt := *s.LastSuccessAt

	tr.Failure(errors.New("boom"), time.Second)
	b.Failure()
	s = tr.Snapshot()
	if s.State != "degraded" || s.StaleSince == nil || !s.StaleSince.Equal(okAt) {
		t.Fatalf("after 1 failure: %+v", s)
	}
	tr.Failure(errors.New("boom"), time.Second)
	b.Failure()
	s = tr.Snapshot()
	if s.State != "down" || s.Breaker != "open" || s.LastError != "boom" || s.ConsecutiveFailures != 2 {
		t.Fatalf("after 2 failures: %+v", s)
	}
	if !s.StaleSince.Equal(okAt) {
		t.Fatal("stale_since must stay at the last successful sync")
	}

	tr.Success(time.Millisecond)
	b.Success()
	s = tr.Snapshot()
	if s.State != "up" || s.StaleSince != nil {
		t.Fatalf("recovery: %+v", s)
	}
}
