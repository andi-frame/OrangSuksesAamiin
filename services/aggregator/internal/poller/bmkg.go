package poller

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

type pendingWarning struct {
	w         mapping.Warning
	raw       []byte
	firstSeen time.Time
}

// BMKGSource polls seismic events and tsunami warnings (two separate endpoints)
// and correlates them by related_event_id, which the spec assigns to the Aggregator.
//
// A warning can be issued after its event is already stored (the mock does this
// on purpose). The stored hazard is then re-evaluated, takes its severity from the
// warning and is saved as a new version, so consumers get an "updated" event.
// A warning whose event isn't known yet waits in `pending` (bounded by PendingTTL)
// and holds the warnings watermark back, so it is fetched again instead of lost,
// even across an Aggregator restart. After PendingTTL it moves to the dead-letter
// store as retryable, where it is retried until its event shows up.
type BMKGSource struct {
	Client     *sources.BMKG
	Repo       ports.Repository
	Ingestor   *ingest.Ingestor
	DLQ        *dlq.Queue
	Rules      mapping.SchemaRules
	Overlap    time.Duration
	PendingTTL time.Duration
	Log        *slog.Logger

	mu      sync.Mutex
	pending map[string]pendingWarning // by warning_id
}

func (s *BMKGSource) Name() string { return "BMKG" }

// elementKind is the dead-letter kind of a broken element of endpoint kind.
func elementKind(kind string, el sources.Element) string {
	if el.Problem == sources.ProblemTruncated {
		return KindBMKGFragment
	}
	return kind
}

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
	var evRaw, wRaw []sources.Element
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
	raws := map[string][]byte{}
	s.mu.Lock()
	if s.pending == nil {
		s.pending = map[string]pendingWarning{}
	}
	for id, p := range s.pending {
		merged[id], raws[id] = p.w, p.raw
	}
	s.mu.Unlock()

	var settledIssued time.Time // newest issued_at among warnings that are settled (applied/dead-lettered)
	if wErr == nil {
		st.Fetched += len(wRaw)
		for _, el := range wRaw {
			key := recordKey(el.Raw, "warning_id")
			msg := dlq.Message{Source: "BMKG", Kind: elementKind(KindBMKGWarning, el), RecordKey: key, Raw: el.Raw, Size: el.Size}
			if el.Problem != "" {
				if err := deadLetter(ctx, s.DLQ, &st, msg, elementFailure(el)); err != nil {
					return st, err
				}
				continue
			}
			w, raw, err := s.parseWarning(el.Raw)
			if err != nil {
				if err := deadLetter(ctx, s.DLQ, &st, msg, dlq.Classify(err)); err != nil {
					return st, err
				}
				if t, ok := parseTime(raw["issued_at"]); ok && t.After(settledIssued) {
					settledIssued = t
				}
				continue
			}
			merged[w.WarningID], raws[w.WarningID] = w, el.Raw
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
		for _, el := range evRaw {
			key := recordKey(el.Raw, "event_id")
			msg := dlq.Message{Source: "BMKG", Kind: elementKind(KindBMKGEvent, el), RecordKey: key, Raw: el.Raw, Size: el.Size}
			if el.Problem != "" {
				if err := deadLetter(ctx, s.DLQ, &st, msg, elementFailure(el)); err != nil {
					return st, err
				}
				continue
			}
			ahead, err := s.DLQ.OpenAhead(ctx, "BMKG", KindBMKGEvent, key)
			if err != nil {
				return st, err
			}
			h, raw, err := s.mapEvent(el.Raw)
			if t, ok := parseTime(raw["occurred_at"]); ok && t.After(maxOccurred) {
				maxOccurred = t
			}
			if ahead != nil {
				if ahead.MessageID != dlq.MessageID(msg.Source, msg.Kind, msg.Raw) {
					f := dlq.Transient("ordered_after: %s (older message of event %s is %s)", ahead.MessageID, key, ahead.Status)
					if err := deadLetter(ctx, s.DLQ, &st, msg, f); err != nil {
						return st, err
					}
				}
				continue
			}
			if err != nil {
				if err := deadLetter(ctx, s.DLQ, &st, msg, dlq.Classify(err)); err != nil {
					return st, err
				}
				continue
			}
			batch = append(batch, h)
			inBatch[h.SourceRefID] = true
		}
	}
	for _, h := range batch {
		ws := byEvent[h.SourceRefID]
		out, err := s.Ingestor.Upsert(ctx, h, keepWarnings(ws))
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
		out, err := s.applyToStored(ctx, evID, ws)
		if errors.Is(err, ports.ErrNotFound) {
			continue // event unknown (yet): stays pending
		}
		if err != nil {
			return st, err
		}
		count(&st, out)
		for _, w := range ws {
			applied[w.WarningID] = true
			if out != ingest.Unchanged { // re-fetched overlap duplicates stay silent
				s.Log.InfoContext(ctx, "late_warning_applied", "warning_id", w.WarningID,
					"hazard_id", domain.HazardID(domain.SourceBMKG, evID), "threat_level", w.Attrs["threat_level"], "outcome", string(out))
			}
		}
	}

	// ---- bookkeeping: what is still waiting, what to dead-letter, where the watermark goes ----
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
			p = pendingWarning{w: w, raw: raws[id], firstSeen: now}
		}
		if now.Sub(p.firstSeen) > s.PendingTTL {
			msg := dlq.Message{Source: "BMKG", Kind: KindBMKGWarning, RecordKey: id, Raw: p.raw}
			f := dlq.Transient("dependency_missing: seismic event %s has not been stored yet", w.RelatedEventID)
			if err := deadLetter(ctx, s.DLQ, &st, msg, f); err != nil {
				return st, err
			}
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

// keepWarnings folds the warnings of this cycle and one stored earlier into
// the freshly mapped event.
func keepWarnings(ws []mapping.Warning) ingest.PrepareFunc {
	return func(existing, fresh *domain.HazardEvent) {
		list := append([]mapping.Warning(nil), ws...)
		if existing != nil { // keep a warning stored earlier when the event is re-polled
			if w, ok := mapping.ExistingWarning(existing); ok {
				list = append(list, w)
			}
		}
		mapping.ApplyWarnings(fresh, list)
	}
}

func (s *BMKGSource) mapEvent(b []byte) (*domain.HazardEvent, map[string]any, error) {
	raw, err := decodeObject(b)
	if err != nil {
		return nil, nil, err
	}
	s.Rules.Apply("BMKG", raw)
	h, err := mapping.Seismic(raw, time.Now())
	if err != nil {
		return nil, raw, mappingFailure(err)
	}
	return h, raw, nil
}

func (s *BMKGSource) parseWarning(b []byte) (mapping.Warning, map[string]any, error) {
	raw, err := decodeObject(b)
	if err != nil {
		return mapping.Warning{}, nil, err
	}
	s.Rules.Apply("BMKG", raw)
	w, err := mapping.ParseWarning(raw)
	if err != nil {
		return w, raw, mappingFailure(err)
	}
	return w, raw, nil
}

// applyToStored folds warnings into an event that is already stored.
// ErrNotFound means the event is not there (yet).
func (s *BMKGSource) applyToStored(ctx context.Context, eventID string, ws []mapping.Warning) (ingest.Outcome, error) {
	existing, err := s.Repo.Get(ctx, domain.HazardID(domain.SourceBMKG, eventID))
	if err != nil {
		return "", err
	}
	h := existing.Clone()
	mapping.ApplyWarnings(&h, ws)
	return s.Ingestor.Upsert(ctx, &h, nil)
}

// ReprocessEvent is the dead-letter handler for KindBMKGEvent.
func (s *BMKGSource) ReprocessEvent(ctx context.Context, m ports.Unprocessed) error {
	h, _, err := s.mapEvent(m.Raw)
	if err != nil {
		return err
	}
	_, err = s.Ingestor.Upsert(ctx, h, keepWarnings(nil))
	return err
}

// ReprocessWarning is the dead-letter handler for KindBMKGWarning.
func (s *BMKGSource) ReprocessWarning(ctx context.Context, m ports.Unprocessed) error {
	w, _, err := s.parseWarning(m.Raw)
	if err != nil {
		return err
	}
	_, err = s.applyToStored(ctx, w.RelatedEventID, []mapping.Warning{w})
	if errors.Is(err, ports.ErrNotFound) {
		return dlq.Transient("dependency_missing: seismic event %s has not been stored yet", w.RelatedEventID)
	}
	return err
}
