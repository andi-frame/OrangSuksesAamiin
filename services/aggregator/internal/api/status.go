package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

// StoreStatus serves the upstream status the leader stored (M2 Problem 2).
// Every instance answers reads, but only the leader polls, so the freshness a
// client sees must come from the store, not from the instance's own memory.
// A view older than MaxAge (measured on the store's clock) means no leader is
// polling right now: the source is reported "unknown" and stale since its last
// success, never "up".
type StoreStatus struct {
	Repo    ports.Repository
	Sources []string      // expected sources, reported "unknown" until a view exists
	MaxAge  time.Duration // default 30 s
	TTL     time.Duration // cache, default 1 s (keeps the store out of every client request)
	Log     *slog.Logger

	mu     sync.Mutex
	cached []resilience.Status
	at     time.Time
}

func (v *StoreStatus) Statuses(ctx context.Context) []resilience.Status {
	ttl := v.TTL
	if ttl <= 0 {
		ttl = time.Second
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.cached != nil && time.Since(v.at) < ttl {
		return v.cached
	}
	rows, err := v.Repo.ListSourceStatus(ctx)
	if err != nil {
		v.Log.WarnContext(ctx, "source_status_read_failed", "error", err.Error())
		if v.cached != nil {
			return v.cached
		}
	}
	v.cached, v.at = v.build(rows), time.Now()
	return v.cached
}

func (v *StoreStatus) build(rows []ports.SourceStatus) []resilience.Status {
	maxAge := v.MaxAge
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	byName := map[string]ports.SourceStatus{}
	for _, r := range rows {
		byName[r.Source] = r
	}
	out := make([]resilience.Status, 0, len(v.Sources))
	for _, name := range v.Sources {
		r, ok := byName[name]
		st := resilience.Status{Source: name, State: "unknown", Breaker: "unknown"}
		if ok && json.Unmarshal(r.Doc, &st) == nil {
			age := r.Age.Seconds()
			st.StatusAgeSec = &age
			if r.Age > maxAge {
				st.State = "unknown"
				st.LastError = fmt.Sprintf("status not refreshed for %.0f s: no instance is polling this source", age)
				if st.LastSuccessAt != nil {
					t := *st.LastSuccessAt
					st.StaleSince = &t
				}
			}
		}
		out = append(out, st)
	}
	return out
}
