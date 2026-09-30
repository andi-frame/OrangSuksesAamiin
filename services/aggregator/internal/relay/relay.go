// Package relay moves outbox entries to the message broker (Problem 5).
//
// The Aggregator never calls a consumer. It writes each hazard change to its
// outbox (same transaction as the hazard); this relay publishes the entries to
// the broker and only then marks them delivered. Consequences:
//   - a slow/dead consumer cannot slow ingestion (consumers are not in this path);
//   - a broker outage cannot lose events or stop ingestion: entries wait in the
//     outbox and are sent, in order, once the broker is back;
//   - delivery is at-least-once (a crash between Publish and MarkPublished
//     re-sends one entry), hence consumers de-duplicate on event_id.
package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

type Relay struct {
	Repo       ports.Repository
	Pub        ports.Publisher
	Log        *slog.Logger
	Interval   time.Duration // idle poll of the outbox
	Batch      int
	MaxBackoff time.Duration
}

// Run blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	backoff := r.Interval
	for {
		n, err := r.Flush(ctx)
		wait := r.Interval
		switch {
		case err != nil:
			backoff = min(backoff*2, r.MaxBackoff)
			wait = backoff
			r.Log.WarnContext(ctx, "outbox_flush_failed", "error", err.Error(), "retry_in_ms", wait.Milliseconds())
		case n >= r.Batch:
			wait = 0 // more waiting: keep going without sleeping
			backoff = r.Interval
		default:
			backoff = r.Interval
		}
		if wait == 0 {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// Flush publishes pending entries oldest-first and stops at the first failure
// (so per-hazard order is preserved). It returns how many were delivered.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	entries, err := r.Repo.PendingOutbox(ctx, r.Batch)
	if err != nil || len(entries) == 0 {
		return 0, err
	}
	done := make([]string, 0, len(entries))
	var pubErr error
	for _, e := range entries {
		ectx := obs.WithCorrelationID(ctx, e.CorrelationID) // same ID as the poll cycle that made the change
		headers := map[string]string{}
		obs.InjectHeaders(ectx, headers)
		headers["event_id"] = e.EventID
		start := time.Now()
		if err := r.Pub.Publish(ectx, ports.Message{Key: e.HazardID, Value: e.Payload, Headers: headers}); err != nil {
			pubErr = err
			break
		}
		r.Log.InfoContext(ectx, "outbox_published", "event_id", e.EventID, "hazard_id", e.HazardID,
			"version", e.Version, "publish_ms", float64(time.Since(start).Microseconds())/1000)
		done = append(done, e.EventID)
	}
	if len(done) > 0 {
		if err := r.Repo.MarkPublished(ctx, done); err != nil {
			return 0, err
		}
	}
	if pubErr != nil {
		return len(done), pubErr
	}
	return len(done), nil
}
