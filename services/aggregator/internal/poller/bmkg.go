package poller

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

type pendingWarning struct {
	w         mapping.Warning
	firstSeen time.Time
}

// BMKGSource polls seismic events and tsunami warnings (two separate endpoints)
// and correlates them by related_event_id - the Aggregator's job per the spec.
//
// A warning may be issued AFTER its event has already been stored (the mock does
// this on purpose): the stored hazard is then re-evaluated, its severity follows
// the warning, and it is saved as a new version (so consumers get an "updated"
// event). A warning whose event is not known yet waits in `pending` (bounded by
// PendingTTL) and holds the warnings watermark back, so it is re-fetched rather
// than lost, even across an Aggregator restart.
type BMKGSource struct {
	Client     *sources.BMKG
	Repo       ports.Repository
	Ingestor   *ingest.Ingestor
	Quarantine *ingest.Quarantine
	Overlap    time.Duration
	PendingTTL time.Duration
	Log        *slog.Logger

	mu      sync.Mutex
	pending map[string]pendingWarning // by warning_id
}

func (s *BMKGSource) Name() string { return "BMKG" }

func (s *BMKGSource) PollOnce(ctx context.Context) (Stats, error) {
	var st Stats
	evSince, err := sinceFor(ctx, s.Repo, CursorBMKGEvents, s.Overlap)
	if err != nil {
		return st, err
	}
	wSince, err := sinceFor(ctx, s.Repo, CursorBMKGWarnings, s.Overlap)
	if err != nil {
		return st, err
	}

	// both endpoints are independent: fetch them concurrently
	var evRaw, wRaw []any
	var evErr, wErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); evRaw, evErr = s.Client.SeismicEvents(ctx, evSince) }()
	go func() { defer wg.Done(); wRaw, wErr = s.Client.TsunamiWarnings(ctx, wSince) }()
	wg.Wait()
	if evErr != nil && wErr != nil {
		return st, errors.Join(evErr, wErr)
	}

	// ---- warnings: parse, merge with the ones still waiting ----
	merged := map[string]mapping.Warning{}
	s.mu.Lock()
	if s.pending == nil {
		s.pending = map[string]pendingWarning{}
	}
	for id, p := range s.pending {
		merged[id] = p.w
	}
	s.mu.Unlock()

	var settledIssued time.Time // newest issued_at among warnings that are settled (applied/dropped/rejected)
	if wErr == nil {
		st.Fetched += len(wRaw)
		for _, item := range wRaw {
			raw, ok := item.(map[string]any)
			if !ok {
				reject(ctx, s.Quarantine, &st, "BMKG", "", "", "tsunami warning is not a JSON object", item)
				continue
			}
			w, err := mapping.ParseWarning(raw)
			var me *mapping.MappingError
			if errors.As(err, &me) {
				reject(ctx, s.Quarantine, &st, "BMKG", mapping.RecordID(raw, "warning_id"), me.Field, me.Reason, raw)
				if t, ok := parseTime(raw["issued_at"]); ok && t.After(settledIssued) {
					settledIssued = t
				}
				continue
			}
			merged[w.WarningID] = w
		}
	}
	byEvent := map[string][]mapping.Warning{}
	for _, w := range merged {
		byEvent[w.RelatedEventID] = append(byEvent[w.RelatedEventID], w)
	}
	applied := map[string]bool{} // warning ids folded into a stored hazard

	// ---- events ----
	var maxOccurred time.Time
	var batch []*domain.HazardEvent
	inBatch := map[string]bool{}
	if evErr == nil {
		st.Fetched += len(evRaw)
		for _, item := range evRaw {
			raw, ok := item.(map[string]any)
			if !ok {
				reject(ctx, s.Quarantine, &st, "BMKG", "", "", "record is not a JSON object", item)
				continue
			}
			if t, ok := parseTime(raw["occurred_at"]); ok && t.After(maxOccurred) {
				maxOccurred = t
			}
			h, err := mapping.Seismic(raw, time.Now())
			var me *mapping.MappingError
			if errors.As(err, &me) {
				reject(ctx, s.Quarantine, &st, "BMKG", mapping.RecordID(raw, "event_id"), me.Field, me.Reason, raw)
				continue
			} else if err != nil {
				return st, err
			}
			batch = append(batch, h)
			inBatch[h.SourceRefID] = true
		}
	}
	for _, h := range batch {
		ws := byEvent[h.SourceRefID]
		prepare := func(existing, fresh *domain.HazardEvent) {
			list := append([]mapping.Warning(nil), ws...)
			if existing != nil { // keep a warning stored earlier when the event is re-polled
				if w, ok := mapping.ExistingWarning(existing); ok {
					list = append(list, w)
				}
			}
			mapping.ApplyWarnings(fresh, list)
		}
		out, err := s.Ingestor.Upsert(ctx, h, prepare)
		if err != nil {
			return st, err
		}
		count(&st, out)
		for _, w := range ws {
			applied[w.WarningID] = true
		}
	}

	// ---- warnings whose event was stored earlier (the "late warning" case) ----
	eventIDs := make([]string, 0, len(byEvent))
	for id := range byEvent {
		if !inBatch[id] {
			eventIDs = append(eventIDs, id)
		}
	}
	sort.Strings(eventIDs)
	for _, evID := range eventIDs {
		ws := byEvent[evID]
		existing, err := s.Repo.Get(ctx, domain.HazardID(domain.SourceBMKG, evID))
		if errors.Is(err, ports.ErrNotFound) {
			continue // event unknown (yet): stays pending
		}
		if err != nil {
			return st, err
		}
		h := existing.Clone()
		mapping.ApplyWarnings(&h, ws)
		out, err := s.Ingestor.Upsert(ctx, &h, nil)
		if err != nil {
			return st, err
		}
		count(&st, out)
		for _, w := range ws {
			applied[w.WarningID] = true
			if out != ingest.Unchanged { // re-fetched overlap duplicates stay silent
				s.Log.InfoContext(ctx, "late_warning_applied", "warning_id", w.WarningID,
					"hazard_id", h.HazardID, "threat_level", w.Attrs["threat_level"], "outcome", string(out))
			}
		}
	}

	// ---- bookkeeping: what is still waiting, what to drop, where the watermark goes ----
	now := time.Now()
	newPending := map[string]pendingWarning{}
	var pendingMin time.Time
	s.mu.Lock()
	old := s.pending
	s.mu.Unlock()
	for id, w := range merged {
		if applied[id] {
			if w.IssuedAt.After(settledIssued) {
				settledIssued = w.IssuedAt
			}
			continue
		}
		p, known := old[id]
		if !known {
			p = pendingWarning{w: w, firstSeen: now}
		}
		if now.Sub(p.firstSeen) > s.PendingTTL {
			reject(ctx, s.Quarantine, &st, "BMKG", id, "related_event_id",
				"orphan tsunami warning: no seismic event "+w.RelatedEventID+" appeared", w.Attrs)
			if w.IssuedAt.After(settledIssued) {
				settledIssued = w.IssuedAt
			}
			continue
		}
		newPending[id] = p
		if !w.IssuedAt.IsZero() && (pendingMin.IsZero() || w.IssuedAt.Before(pendingMin)) {
			pendingMin = w.IssuedAt
		}
	}
	s.mu.Lock()
	s.pending = newPending
	s.mu.Unlock()

	if evErr == nil {
		if err := advance(ctx, s.Repo, CursorBMKGEvents, maxOccurred); err != nil {
			return st, err
		}
	}
	if wErr == nil {
		to := settledIssued
		if !pendingMin.IsZero() && (to.IsZero() || !pendingMin.After(to)) {
			to = pendingMin.Add(-time.Microsecond) // keep re-fetching what is still unmatched
		}
		if err := advance(ctx, s.Repo, CursorBMKGWarnings, to); err != nil {
			return st, err
		}
	}
	return st, errors.Join(evErr, wErr)
}
