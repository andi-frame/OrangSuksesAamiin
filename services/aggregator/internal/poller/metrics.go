package poller

import (
	"errors"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

var (
	pollCycles = obs.NewCounter("aggregator_poll_cycles_total",
		"Polling cycles per source and result (ok, source_error, internal_error, fenced).", "source", "result")
	pollDuration = obs.NewHistogram("aggregator_poll_duration_seconds",
		"Duration of one polling cycle (fetch + map + store).", obs.DefaultBuckets, "source")
	pollRecords = obs.NewCounter("aggregator_poll_records_total",
		"Records seen by the pollers per outcome (fetched, created, updated, unchanged, unprocessed).", "source", "outcome")
	pollSkipped = obs.NewCounter("aggregator_poll_skipped_total",
		"Cycles skipped per reason (circuit_open, not_leader).", "source", "reason")
)

func recordPoll(source string, took time.Duration, st Stats, err error) {
	result := "ok"
	var se *sources.SourceError
	switch {
	case err == nil:
	case errors.As(err, &se):
		result = "source_error"
	default:
		result = "internal_error"
	}
	pollCycles.Inc(source, result)
	pollDuration.Observe(took.Seconds(), source)
	for k, v := range map[string]int{"fetched": st.Fetched, "created": st.Created, "updated": st.Updated,
		"unchanged": st.Unchanged, "unprocessed": st.Unprocessed} {
		pollRecords.Add(float64(v), source, k)
	}
}
