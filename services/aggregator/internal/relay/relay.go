// Package relay moves outbox entries to the message broker (Problem 5).
//
// The Aggregator never calls a consumer. Each hazard change is written to the
// outbox in the same transaction as the hazard, and the relay publishes the
// entries to the broker and marks them delivered only after that. So:
//   - a slow or dead consumer can't slow ingestion, it isn't in this path;
//   - a broker outage doesn't lose events or stop ingestion, the entries wait in
//     the outbox and go out in order once the broker is back;
//   - delivery is at-least-once (a crash between Publish and MarkPublished sends
//     one entry twice), so consumers de-duplicate on event_id.
package relay

import (
	"context"
	"errors"
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
//
// With a BatchPublisher the whole batch goes to the broker in one round trip
// and only the acknowledged prefix is marked delivered: an entry acknowledged
// after a failed one is sent again later (at-least-once; consumers de-duplicate
// on event_id). This keeps a burst (thousands of sensor observations) from
// delaying the critical alert behind it by one broker round trip per entry.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	entries, err := r.Repo.PendingOutbox(ctx, r.Batch)
	if err != nil || len(entries) == 0 {
		return 0, err
	}
	msgs := make([]ports.Message, len(entries))
	spans := make([]*obs.Span, len(entries))
	ctxs := make([]context.Context, len(entries))
	for i, e := range entries {
		ectx := obs.WithCorrelationID(ctx, e.CorrelationID) // same ID (and trace) as the poll cycle that made the change
		ectx, span := obs.StartSpan(ectx, "publish hazard.events", obs.SpanProducer,
			"event_id", e.EventID, "outbox_wait_ms", float64(time.Since(e.CreatedAt).Microseconds())/1000)
		headers := map[string]string{}
		obs.InjectHeaders(ectx, headers)
		headers["event_id"] = e.EventID
		msgs[i] = ports.Message{Key: e.HazardID, Value: e.Payload, Headers: headers}
		spans[i], ctxs[i] = span, ectx
	}
	start := time.Now()
	errs := make([]error, len(entries))
	if bp, ok := r.Pub.(ports.BatchPublisher); ok && len(entries) > 1 {
		errs = bp.PublishBatch(ctx, msgs)
	} else {
		for i := range entries {
			if errs[i] = r.Pub.Publish(ctxs[i], msgs[i]); errs[i] != nil {
				for j := i + 1; j < len(errs); j++ {
					errs[j] = errNotSent
				}
				break
			}
		}
	}
	done := make([]string, 0, len(entries))
	var pubErr error
	for i, e := range entries {
		spans[i].RecordError(errs[i])
		spans[i].End()
		if pubErr != nil {
			continue
		}
		if errs[i] != nil {
			relayFailures.Inc()
			pubErr = errs[i]
			continue
		}
		relayPublished.Inc()
		relayLag.Observe(time.Since(e.CreatedAt).Seconds())
		r.Log.InfoContext(ctxs[i], "outbox_published", "event_id", e.EventID, "hazard_id", e.HazardID,
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

var errNotSent = errors.New("not sent: an earlier entry failed")

var (
	relayPublished = obs.NewCounter("aggregator_outbox_published_total", "Outbox entries delivered to the broker.")
	relayFailures  = obs.NewCounter("aggregator_outbox_publish_failures_total", "Failed broker publishes (the entry stays in the outbox).")
	relayLag       = obs.NewHistogram("aggregator_outbox_delivery_seconds",
		"Time from the hazard write (outbox entry) to the broker acknowledgement.", obs.DefaultBuckets)
)
