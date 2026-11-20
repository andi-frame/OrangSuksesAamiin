// Package poller runs the ingest side of the Aggregator: one independent
// polling loop per source. This is the core of Problem 2: polling is decoupled
// from serving, so a slow or dead PVMBG can't delay a request that only needs
// BMKG data. There is no shared, sequential call chain.
//
// M2: the loops run only on the elected leader (internal/leader), so each
// upstream is polled by exactly one instance.
package poller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// Stats summarises one polling cycle.
type Stats struct {
	Fetched     int `json:"fetched"`
	Created     int `json:"created"`
	Updated     int `json:"updated"`
	Unchanged   int `json:"unchanged"`
	Unprocessed int `json:"unprocessed"` // new dead-letter messages
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

	// Gate, when set, must return nil before every cycle (the leader re-confirms
	// its lease, so a leader that was frozen does not call the upstream again).
	Gate func(ctx context.Context) error
	// Publish, when set, receives the source status after every cycle (stored
	// so that every instance serves the same freshness).
	Publish func(ctx context.Context, st resilience.Status)
}

// Run blocks until ctx is cancelled. The next cycle starts Interval after the
// previous one finished, so a slow source never gets overlapping requests.
func (l *Loop) Run(ctx context.Context) {
	for {
		if errors.Is(l.cycle(ctx), ports.ErrFenced) {
			return
		}
		t := time.NewTimer(l.Interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (l *Loop) cycle(parent context.Context) error {
	// every cycle is one traceable flow: poll -> map -> store -> outbox -> broker
	ctx := obs.WithCorrelationID(parent, obs.NewCorrelationID())
	name := l.Source.Name()
	ctx, span := obs.StartSpan(ctx, "poll "+name, obs.SpanInternal, "source", name)
	defer span.End()

	if l.Gate != nil {
		if err := l.Gate(ctx); err != nil {
			pollSkipped.Inc(name, "not_leader")
			l.Log.WarnContext(ctx, "poll_skipped", "source", name, "reason", "leadership_not_confirmed", "error", err.Error())
			return nil
		}
	}
	defer l.publish(ctx)
	if !l.Breaker.Allow() {
		l.Tracker.Skipped()
		pollSkipped.Inc(name, "circuit_open")
		l.Log.DebugContext(ctx, "poll_skipped", "source", name, "reason", "circuit_open")
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, l.CycleTimeout)
	defer cancel()
	start := time.Now()
	stats, err := l.Source.PollOnce(cctx)
	took := time.Since(start)
	l.Tracker.AddUnprocessed(stats.Unprocessed)
	span.SetAttr("fetched", stats.Fetched)
	span.SetAttr("created", stats.Created)
	span.SetAttr("updated", stats.Updated)
	span.SetAttr("unprocessed", stats.Unprocessed)
	span.RecordError(err)
	recordPoll(name, took, stats, err)

	var se *sources.SourceError
	switch {
	case err == nil:
		l.Breaker.Success()
		l.Tracker.Success(took)
		l.Log.InfoContext(ctx, "poll_ok", "source", name, "duration_ms", ms(took),
			"fetched", stats.Fetched, "created", stats.Created, "updated", stats.Updated,
			"unchanged", stats.Unchanged, "unprocessed", stats.Unprocessed)
	case errors.Is(err, ports.ErrFenced):
		// a newer leader has written to the store: this instance must stop at once
		l.Log.ErrorContext(ctx, "write_fenced", "source", name, "error", err.Error())
		return err
	case errors.As(err, &se):
		l.Breaker.Failure()
		l.Tracker.Failure(err, took)
		l.Log.WarnContext(ctx, "poll_failed", "source", name, "duration_ms", ms(took),
			"error", err.Error(), "auth_problem", se.IsAuth(), "breaker", l.Breaker.State())
	default:
		// our own storage failed: the source is fine, do not blame (or open the breaker of) it
		l.Log.ErrorContext(ctx, "poll_internal_error", "source", name, "duration_ms", ms(took), "error", err.Error())
	}
	return nil
}

func (l *Loop) publish(ctx context.Context) {
	if l.Publish != nil && ctx.Err() == nil {
		l.Publish(ctx, l.Tracker.Snapshot())
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
