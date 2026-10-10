// Package poller runs the ingest side of the Aggregator: one independent
// polling loop per source. This is the core of Problem 2: polling is decoupled
// from serving, so a slow or dead PVMBG can't delay a request that only needs
// BMKG data. There is no shared, sequential call chain.
package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// Stats summarises one polling cycle.
type Stats struct {
	Fetched     int `json:"fetched"`
	Created     int `json:"created"`
	Updated     int `json:"updated"`
	Unchanged   int `json:"unchanged"`
	Quarantined int `json:"quarantined"`
}

// Source is one upstream agency as seen by the loop.
type Source interface {
	Name() string
	// PollOnce fetches, maps and stores one batch. Errors wrapping
	// *sources.SourceError count against the source's circuit breaker; any other
	// error (e.g. the database) does not.
	PollOnce(ctx context.Context) (Stats, error)
}

// Loop polls one source forever.
type Loop struct {
	Source       Source
	Breaker      *resilience.Breaker
	Tracker      *resilience.Tracker
	Interval     time.Duration
	CycleTimeout time.Duration
	Log          *slog.Logger
}

// Run blocks until ctx is cancelled. The next cycle starts Interval after the
// previous one finished, so a slow source never gets overlapping requests.
func (l *Loop) Run(ctx context.Context) {
	for {
		l.cycle(ctx)
		t := time.NewTimer(l.Interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (l *Loop) cycle(parent context.Context) {
	// every cycle is one traceable flow: poll -> map -> store -> outbox -> broker
	ctx := obs.WithCorrelationID(parent, obs.NewCorrelationID())
	name := l.Source.Name()

	if !l.Breaker.Allow() {
		l.Tracker.Skipped()
		l.Log.DebugContext(ctx, "poll_skipped", "source", name, "reason", "circuit_open")
		return
	}
	cctx, cancel := context.WithTimeout(ctx, l.CycleTimeout)
	defer cancel()
	start := time.Now()
	stats, err := l.Source.PollOnce(cctx)
	took := time.Since(start)

	var se *sources.SourceError
	switch {
	case err == nil:
		l.Breaker.Success()
		l.Tracker.Success(took)
		l.Tracker.AddQuarantined(stats.Quarantined)
		l.Log.InfoContext(ctx, "poll_ok", "source", name, "duration_ms", ms(took),
			"fetched", stats.Fetched, "created", stats.Created, "updated", stats.Updated,
			"unchanged", stats.Unchanged, "quarantined", stats.Quarantined)
	case errors.As(err, &se):
		l.Breaker.Failure()
		l.Tracker.Failure(err, took)
		l.Tracker.AddQuarantined(stats.Quarantined)
		l.Log.WarnContext(ctx, "poll_failed", "source", name, "duration_ms", ms(took),
			"error", err.Error(), "auth_problem", se.IsAuth(), "breaker", l.Breaker.State())
	default:
		// our own storage failed: the source is fine, do not blame (or open the breaker of) it
		l.Log.ErrorContext(ctx, "poll_internal_error", "source", name, "duration_ms", ms(took), "error", err.Error())
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
