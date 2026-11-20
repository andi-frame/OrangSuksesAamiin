package main

import (
	"context"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/leader"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Gauges read from the store every few seconds. Every instance reports the same
// stored view (the leader writes it), so a dashboard can use max() over pods and
// still be right after a failover.
var (
	sourceState = obs.NewGauge("aggregator_source_state",
		"Source state from the stored status: 1 up, 0.5 degraded, 0 down/unknown.", "source")
	sourceBreakerOpen = obs.NewGauge("aggregator_source_breaker_open",
		"1 when the circuit breaker of the source is open or half-open.", "source")
	sourceStaleSeconds = obs.NewGauge("aggregator_source_stale_seconds",
		"Seconds since the last successful poll (0 while the source is up).", "source")
	sourceSkipped = obs.NewGauge("aggregator_source_skipped_by_breaker",
		"Polls skipped by the breaker since the leader started (stored status).", "source")
	unprocessedOpen = obs.NewGauge("aggregator_unprocessed_messages",
		"Open dead-letter messages per source and status (retrying, parked).", "source", "status")
	isLeader   = obs.NewGauge("aggregator_is_leader", "1 on the instance that holds the ingest lease.")
	leaderTerm = obs.NewGauge("aggregator_leader_term", "Lease term (fencing token) this instance observed.")
)

func exportGauges(ctx context.Context, st api.StatusSource, repo ports.Repository, lead func() leader.Status) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		now := time.Now()
		for _, s := range st.Statuses(ctx) {
			v := 0.0
			switch s.State {
			case "up":
				v = 1
			case "degraded":
				v = 0.5
			}
			sourceState.Set(v, s.Source)
			open := 0.0
			if s.Breaker == "open" || s.Breaker == "half-open" {
				open = 1
			}
			sourceBreakerOpen.Set(open, s.Source)
			stale := 0.0
			if s.StaleSince != nil {
				stale = now.Sub(*s.StaleSince).Seconds()
			}
			sourceStaleSeconds.Set(stale, s.Source)
			sourceSkipped.Set(float64(s.SkippedByBreaker), s.Source)
		}
		if counts, err := repo.CountUnprocessed(ctx); err == nil {
			for src, byStatus := range counts {
				for _, status := range []string{"retrying", "parked"} {
					unprocessedOpen.Set(float64(byStatus[status]), src, status)
				}
			}
		}
		if lead != nil {
			ls := lead()
			v := 0.0
			if ls.Leader {
				v = 1
			}
			isLeader.Set(v)
			leaderTerm.Set(float64(max(ls.Term, ls.ObservedTerm)))
		} else {
			isLeader.Set(1)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
