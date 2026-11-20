package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// Sensor Pantai (M2 Problem 5). The sensor is the least trustworthy source:
// it can be silent, send everything at once, answer /healthz while
// /v1/observations fails, or serve expired data. The loop (loop.go) gives it
// the same circuit breaker as BMKG/PVMBG, in its own goroutine with its own
// HTTP client, so its trouble never reaches the other sources (bulkhead).
// This file adds what is specific to it:
//
//   - the cursor is persisted as a fenced checkpoint and saved only after
//     every fetched observation is stored or set aside (no loss, and a retry
//     re-reads the same page: storing is idempotent on observation_id);
//   - a burst is read in pages (up to MaxPages per cycle) and critical
//     observations are stored first, so an alert never waits behind a backlog;
//   - every observation is checked against the base contract
//     (mapping.Coastal); a violation is set aside in the dead-letter store
//     with its reason and never becomes a HazardEvent;
//   - health is judged from the data, not from /healthz: a generated_at that
//     stops advancing (FreezeAfter) or jumps backwards is a failure of the
//     source, observations far older than generated_at are expired, and an
//     observation id that comes back with different values is a conflict.
const (
	CursorCoastal          = "sensor-pantai.cursor"
	KindCoastalObservation = "sensor-pantai.observation"
	KindCoastalResponse    = "sensor-pantai.response"
)

// CoastalSource polls Sensor Pantai.
type CoastalSource struct {
	Client   *sources.Coastal
	Repo     ports.Repository
	Store    ports.SatelliteStore // fenced checkpoint document (cursor)
	Ingestor *ingest.Ingestor
	DLQ      *dlq.Queue
	Log      *slog.Logger

	PageSize    int           // default 500
	MaxPages    int           // per cycle, default 20
	FreezeAfter time.Duration // generated_at not advancing for this long = stale source; default 15 s
	MaxLag      time.Duration // observed_at older than generated_at by more than this (sensor time) = expired; default 2 h
	StationsTTL time.Duration // default 5 min
	Now         func() time.Time

	mu         sync.Mutex
	stations   map[string]mapping.Station
	stationsAt time.Time
	lastGen    time.Time // newest generated_at seen
	genWall    time.Time // wall time when lastGen last advanced
	lastOkWall time.Time // wall time of the previous successful fetch
	speed      float64   // sensor seconds per wall second (estimate)
}

func (s *CoastalSource) Name() string { return string(domain.SourceCoastal) }

func (s *CoastalSource) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

type coastalCheckpoint struct {
	Cursor      string    `json:"cursor"`
	GeneratedAt time.Time `json:"generated_at"`
}

func (s *CoastalSource) loadCursor(ctx context.Context) (string, error) {
	b, err := s.Store.LoadSatellite(ctx, CursorCoastal)
	if err != nil || len(b) == 0 {
		return "", err
	}
	var c coastalCheckpoint
	if json.Unmarshal(b, &c) != nil {
		return "", nil
	}
	return c.Cursor, nil
}

func (s *CoastalSource) saveCursor(ctx context.Context, cursor string, gen time.Time) error {
	b, _ := json.Marshal(coastalCheckpoint{Cursor: cursor, GeneratedAt: gen})
	return s.Store.StoreSatellite(ctx, CursorCoastal, b)
}

func (s *CoastalSource) stationTable(ctx context.Context, force bool) (map[string]mapping.Station, error) {
	ttl := s.StationsTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	s.mu.Lock()
	cur, at := s.stations, s.stationsAt
	s.mu.Unlock()
	if cur != nil && !force && s.now().Sub(at) < ttl {
		return cur, nil
	}
	items, err := s.Client.Stations(ctx)
	if err != nil {
		if cur != nil {
			return cur, nil // keep the last good table
		}
		return nil, err
	}
	table := map[string]mapping.Station{}
	for _, raw := range items {
		var st mapping.Station
		if json.Unmarshal(raw, &st) != nil || st.StationID == "" {
			coastalRejected.Inc("station_list")
			continue
		}
		table[st.StationID] = st
	}
	if len(table) == 0 && cur != nil {
		return cur, nil
	}
	s.mu.Lock()
	s.stations, s.stationsAt = table, s.now()
	s.mu.Unlock()
	coastalStations.Set(float64(len(table)))
	return table, nil
}

type coastalItem struct {
	raw      []byte
	key      string
	m        map[string]any
	err      error
	critical bool
}

func (s *CoastalSource) PollOnce(ctx context.Context) (Stats, error) {
	var st Stats
	stations, err := s.stationTable(ctx, false)
	if err != nil {
		return st, err
	}
	cursor, err := s.loadCursor(ctx)
	if err != nil {
		return st, err
	}
	pageSize, maxPages := s.PageSize, s.MaxPages
	if pageSize <= 0 {
		pageSize = 500
	}
	if maxPages <= 0 {
		maxPages = 20
	}

	fetchStart := s.now()
	var items []coastalItem
	var gen time.Time
	var fetchErr error
	next := cursor
	for range maxPages {
		page, err := s.Client.Observations(ctx, next, pageSize)
		if errors.Is(err, sources.ErrBadCursor) {
			// The sensor does not know our position any more (it restarted).
			// Starting over is safe: storing is idempotent on observation_id.
			s.Log.WarnContext(ctx, "sensor_cursor_reset", "source", s.Name(), "cursor", next)
			coastalAnomalies.Inc("cursor_rejected")
			if next == "" {
				fetchErr = &sources.SourceError{Source: "sensor-pantai", Status: 400, Err: err}
				break
			}
			next, cursor = "", ""
			continue
		}
		var mr *sources.MalformedResponse
		if errors.As(err, &mr) {
			coastalAnomalies.Inc("malformed_response")
			msg := dlq.Message{Source: s.Name(), Kind: KindCoastalResponse, Raw: mr.Raw}
			if derr := deadLetter(ctx, s.DLQ, &st, msg, dlq.Permanent("malformed: %s", mr.Reason)); derr != nil {
				return st, derr
			}
			if !mr.GeneratedAt.IsZero() {
				gen = mr.GeneratedAt
			}
			for _, el := range mr.Items { // what can still be read is processed like any page
				it := coastalItem{raw: el.Raw, key: recordKey(el.Raw, "observation_id")}
				if el.Problem != "" {
					it.err = elementFailure(el)
				} else if it.m, it.err = decodeObject(el.Raw); it.err == nil {
					it.critical, _ = it.m["critical"].(bool)
				}
				items = append(items, it)
			}
			if mr.NextCursor != "" && mr.NextCursor != next {
				// the page is kept whole in the dead-letter store; continue after it
				s.Log.WarnContext(ctx, "sensor_page_set_aside", "reason", mr.Reason, "next_cursor", mr.NextCursor,
					"salvaged", len(mr.Items))
				next = mr.NextCursor
				continue
			}
			fetchErr = &sources.SourceError{Source: "sensor-pantai", Status: 200, Err: mr}
			break
		}
		if err != nil {
			fetchErr = err
			break
		}
		if !page.GeneratedAt.IsZero() {
			gen = page.GeneratedAt
		}
		for _, raw := range page.Items {
			it := coastalItem{raw: raw, key: recordKey(raw, "observation_id")}
			it.m, it.err = decodeObject(raw)
			if it.err == nil {
				it.critical, _ = it.m["critical"].(bool)
			}
			items = append(items, it)
		}
		if page.NextCursor == "" || page.NextCursor == next || len(page.Items) == 0 {
			if page.NextCursor != "" {
				next = page.NextCursor
			}
			break
		}
		next = page.NextCursor
	}
	st.Fetched = len(items)
	coastalFetched.Add(float64(len(items)))

	allowedLag := s.allowedLag(gen)

	// critical observations first: an alert never waits behind a burst
	sort.SliceStable(items, func(i, j int) bool { return items[i].critical && !items[j].critical })
	for _, it := range items {
		if err := s.handle(ctx, &st, it, stations, gen, allowedLag, fetchStart); err != nil {
			return st, err // store problem or fenced: the cursor is not saved, the page is read again
		}
	}
	if next != cursor || (!gen.IsZero() && len(items) > 0) {
		if err := s.saveCursor(ctx, next, gen); err != nil {
			return st, err
		}
	}
	if fetchErr != nil {
		return st, fetchErr
	}
	return st, s.checkFreshness(ctx, gen)
}

// allowedLag is MaxLag plus the sensor time that passed while we could not read
// (our own outage or a failover): a backlog we fetch late is not "expired".
func (s *CoastalSource) allowedLag(gen time.Time) time.Duration {
	maxLag := s.MaxLag
	if maxLag <= 0 {
		maxLag = 2 * time.Hour
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastOkWall.IsZero() || gen.IsZero() {
		return -1 // first cycle of this leader: no basis, do not judge
	}
	speed := s.speed
	if speed <= 0 {
		speed = 1
	}
	gap := s.now().Sub(s.lastOkWall)
	return maxLag + time.Duration(float64(gap)*math.Max(speed, 1))
}

func (s *CoastalSource) handle(ctx context.Context, st *Stats, it coastalItem, stations map[string]mapping.Station,
	gen time.Time, allowedLag time.Duration, fetched time.Time) error {
	msg := dlq.Message{Source: s.Name(), Kind: KindCoastalObservation, RecordKey: it.key, Raw: it.raw}
	reject := func(f *dlq.Failure, reason string) error {
		coastalRejected.Inc(reason)
		if it.critical {
			coastalCritical.Inc("rejected")
		}
		s.Log.WarnContext(ctx, "sensor_observation_set_aside", "observation_id", it.key, "reason", f.Reason, "critical", it.critical)
		return deadLetter(ctx, s.DLQ, st, msg, f)
	}
	if it.err != nil {
		return reject(dlq.Classify(it.err), "malformed")
	}
	h, err := mapping.Coastal(it.m, stations, gen)
	var me *mapping.MappingError
	if errors.As(err, &me) && me.Field == mapping.ErrUnknownStation {
		// the station list may be out of date: refresh once, then decide
		if fresh, ferr := s.stationTable(ctx, true); ferr == nil {
			h, err = mapping.Coastal(it.m, fresh, gen)
		}
		if errors.As(err, &me) && me.Field == mapping.ErrUnknownStation {
			return reject(dlq.Transient("contract_violation: field '%s': %s", me.Field, me.Reason), me.Field)
		}
	}
	if errors.As(err, &me) {
		return reject(dlq.Permanent("contract_violation: field '%s': %s", me.Field, me.Reason), me.Field)
	}
	if err != nil {
		return reject(dlq.Classify(err), "other")
	}
	if allowedLag > 0 && gen.Sub(h.OccurredAt) > allowedLag {
		return reject(dlq.Permanent("expired: observed_at %s is %s older than generated_at %s",
			h.OccurredAt.Format(time.RFC3339), gen.Sub(h.OccurredAt).Round(time.Second), gen.Format(time.RFC3339)), "expired")
	}
	existing, err := s.Repo.Get(ctx, h.HazardID)
	switch {
	case err == nil:
		if sameReading(existing.Attributes, h.Attributes) && existing.OccurredAt.Equal(h.OccurredAt) {
			coastalDuplicates.Inc()
			st.Unchanged++
			return nil
		}
		// an observation is immutable: the same id with other values is a conflict, kept for Ops
		return reject(dlq.Permanent("conflicting_duplicate: observation %s was already stored with other values", it.key), "conflicting_duplicate")
	case !errors.Is(err, ports.ErrNotFound):
		return err
	}
	out, err := s.Ingestor.Upsert(ctx, h, nil)
	if err != nil {
		return err
	}
	count(st, out)
	coastalStored.Inc(string(h.Severity))
	if it.critical {
		coastalCritical.Inc("stored")
		coastalCriticalIngest.Observe(s.now().Sub(fetched).Seconds())
		s.Log.WarnContext(ctx, "sensor_critical_stored", "observation_id", it.key, "hazard_id", h.HazardID,
			"area_name", h.AreaName, "ingest_ms", float64(s.now().Sub(fetched).Microseconds())/1000)
	}
	return nil
}

// checkFreshness turns "healthy but expired" into a visible failure.
func (s *CoastalSource) checkFreshness(ctx context.Context, gen time.Time) error {
	freeze := s.FreezeAfter
	if freeze <= 0 {
		freeze = 15 * time.Second
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() { s.lastOkWall = now }()
	if gen.IsZero() {
		return nil
	}
	switch {
	case s.lastGen.IsZero():
		s.lastGen, s.genWall = gen, now
	case gen.After(s.lastGen):
		if dw := now.Sub(s.genWall).Seconds(); dw > 0 {
			sp := gen.Sub(s.lastGen).Seconds() / dw
			if s.speed == 0 {
				s.speed = sp
			} else {
				s.speed = 0.8*s.speed + 0.2*sp
			}
			coastalSpeed.Set(s.speed)
		}
		s.lastGen, s.genWall = gen, now
	case gen.Before(s.lastGen.Add(-time.Minute)):
		// The sensor restarted (or replays old data): its positions no longer
		// match our cursor. Read again from the start; storing is idempotent.
		coastalAnomalies.Inc("generated_at_backwards")
		prev := s.lastGen
		s.Log.WarnContext(ctx, "sensor_clock_backwards", "generated_at", gen, "previous", prev, "action", "cursor_reset")
		s.lastGen, s.genWall = gen, now
		if err := s.saveCursor(ctx, "", gen); err != nil {
			return err
		}
		return &sources.SourceError{Source: "sensor-pantai", Status: 200,
			Err: fmt.Errorf("stale: generated_at went backwards from %s to %s", prev.Format(time.RFC3339), gen.Format(time.RFC3339))}
	}
	frozen := now.Sub(s.genWall)
	coastalGenFrozen.Set(frozen.Seconds())
	if frozen > freeze {
		coastalAnomalies.Inc("generated_at_frozen")
		return &sources.SourceError{Source: "sensor-pantai", Status: 200,
			Err: fmt.Errorf("stale: generated_at frozen at %s for %s while the sensor answers", s.lastGen.Format(time.RFC3339), frozen.Round(time.Second))}
	}
	return nil
}

// Reprocess is the dead-letter handler for KindCoastalObservation (after a
// station appears, or after Ops requeued a message once the system changed).
func (s *CoastalSource) Reprocess(ctx context.Context, m ports.Unprocessed) error {
	obj, err := decodeObject(m.Raw)
	if err != nil {
		return err
	}
	stations, err := s.stationTable(ctx, false)
	if err != nil {
		return dlq.Transient("stations unavailable: %v", err)
	}
	h, err := mapping.Coastal(obj, stations, time.Time{})
	var me *mapping.MappingError
	if errors.As(err, &me) {
		if me.Field == mapping.ErrUnknownStation {
			return dlq.Transient("contract_violation: field '%s': %s", me.Field, me.Reason)
		}
		return dlq.Permanent("contract_violation: field '%s': %s", me.Field, me.Reason)
	}
	if err != nil {
		return err
	}
	existing, err := s.Repo.Get(ctx, h.HazardID)
	if err == nil {
		if sameReading(existing.Attributes, h.Attributes) {
			return nil
		}
		return dlq.Permanent("conflicting_duplicate: observation %s was already stored with other values", h.SourceRefID)
	}
	if !errors.Is(err, ports.ErrNotFound) {
		return err
	}
	_, err = s.Ingestor.Upsert(ctx, h, nil)
	return err
}

func sameReading(a, b map[string]any) bool {
	for _, k := range []string{"station_id", "sea_level_anomaly_m", "wave_height_m", "quality", "critical"} {
		if !reflect.DeepEqual(norm(a[k]), norm(b[k])) {
			return false
		}
	}
	return true
}

func norm(v any) any {
	switch x := v.(type) {
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return v
}

var (
	coastalFetched = obs.NewCounter("sensor_pantai_observations_fetched_total", "Observations read from Sensor Pantai.")
	coastalStored  = obs.NewCounter("sensor_pantai_observations_stored_total",
		"Observations stored as HazardEvent COASTAL, by severity.", "severity")
	coastalRejected = obs.NewCounter("sensor_pantai_observations_rejected_total",
		"Observations set aside (dead-letter store) by reason: the violated field, expired, conflicting_duplicate, malformed.", "reason")
	coastalDuplicates = obs.NewCounter("sensor_pantai_duplicates_total", "Observations the sensor sent again unchanged (ignored).")
	coastalCritical   = obs.NewCounter("sensor_pantai_critical_total",
		"Observations flagged critical by the sensor: stored (alert goes out) or rejected (contract violation).", "outcome")
	coastalCriticalIngest = obs.NewHistogram("sensor_pantai_critical_ingest_seconds",
		"Critical observations: fetch to stored (outbox written).", []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10})
	coastalAnomalies = obs.NewCounter("sensor_pantai_anomalies_total",
		"Misbehaviour of the sensor detected from its data (malformed_response, generated_at_frozen, generated_at_backwards, cursor_rejected).", "kind")
	coastalGenFrozen = obs.NewGauge("sensor_pantai_generated_at_frozen_seconds",
		"Wall seconds since generated_at last advanced (stale data while /healthz says ok).")
	coastalSpeed    = obs.NewGauge("sensor_pantai_clock_speed", "Estimated sensor seconds per wall second.")
	coastalStations = obs.NewGauge("sensor_pantai_stations", "Stations known from GET /v1/stations.")
)
