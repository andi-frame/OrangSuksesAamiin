package api

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Analytics (M2 Problem 1, criterion 7): aggregates of every HazardEvent per
// hazard category, severity and area, computed across every storage unit.
//
// How the numbers are computed: one full scan of the store in the audit order
// (the same ports.Repository.List the Audit API uses, so the same rows and the
// same "active" rule), page by page across all shards, cached for CacheTTL.
// The counts are therefore exactly what counting GET /internal/audit/events
// gives. When a unit is unreachable, List still answers with the rows of the
// units it reached and names the missing ones; the summary carries
// partial=true and unavailable_units, and the dashboard shows the numbers as
// incomplete instead of presenting them as the truth.
//
// Category: hazard_type, except that a SEISMIC event BMKG attached a tsunami
// warning to (attributes.warning_id) is counted as TSUNAMI.
type Analytics struct {
	Repo     ports.Repository
	CacheTTL time.Duration // default 5 s

	mu   sync.Mutex
	snap *analyticsSnap
}

type slim struct {
	cat, sev, area, src string
	at, ingested        time.Time
}

type analyticsSnap struct {
	at          time.Time
	rows        []slim
	unavailable []string
	took        time.Duration
}

// Categories in display order.
var Categories = []string{"SEISMIC", "TSUNAMI", "VOLCANIC", "WILDFIRE", "COASTAL"}

// Category of one hazard.
func Category(h domain.HazardEvent) string {
	if h.HazardType == domain.HazardSeismic {
		if id, _ := h.Attributes["warning_id"].(string); id != "" {
			return "TSUNAMI"
		}
	}
	return string(h.HazardType)
}

var analyticsScan = obs.NewHistogram("aggregator_analytics_scan_seconds",
	"Full cross-shard scan behind the analytics summary.", []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10})

func (a *Analytics) snapshot(ctx context.Context) (*analyticsSnap, error) {
	ttl := a.CacheTTL
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.snap != nil && time.Since(a.snap.at) < ttl {
		return a.snap, nil
	}
	start := time.Now()
	ctx, span := obs.StartSpan(ctx, "analytics scan", obs.SpanInternal)
	defer span.End()
	s := &analyticsSnap{}
	missing := map[string]bool{}
	f := ports.ListFilter{AuditOrder: true, Limit: 1000}
	for pages := 0; pages < 10000; pages++ {
		page, err := a.Repo.List(ctx, f)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		for _, u := range unitNames(page.UnavailableUnits) {
			missing[u] = true
		}
		for _, h := range page.Items {
			s.rows = append(s.rows, slim{cat: Category(h), sev: string(h.Severity), area: h.AreaName,
				src: string(h.Source), at: h.OccurredAt, ingested: h.IngestedAt})
		}
		if page.NextCursor == "" {
			break
		}
		f.Cursor = page.NextCursor
	}
	for u := range missing {
		s.unavailable = append(s.unavailable, u)
	}
	sort.Strings(s.unavailable)
	s.at, s.took = time.Now(), time.Since(start)
	analyticsScan.Observe(s.took.Seconds())
	span.SetAttr("rows", len(s.rows))
	span.SetAttr("partial", len(s.unavailable) > 0)
	// a partial scan is not cached for long: retry soon to show complete numbers again
	if len(s.unavailable) > 0 {
		s.at = s.at.Add(-ttl + time.Second)
	}
	a.snap = s
	return s, nil
}

// handler: GET /internal/analytics/summary?from=&to=&bucket=hour|day|month|year&ingested_before=
//
// from/to select occurred_at (the chart's time range). ingested_before counts only
// what was stored before that instant (ingested_at < t): the same cut applied to
// GET /internal/audit/events makes both counts comparable while ingestion goes on.
func (a *Analytics) handler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var from, to time.Time
	var cut time.Time
	for k, dst := range map[string]*time.Time{"from": &from, "to": &to, "ingested_before": &cut} {
		if v := q.Get(k); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				obs.WriteError(w, http.StatusBadRequest, "bad_request", k+" must be RFC 3339")
				return
			}
			*dst = t.UTC()
		}
	}
	snap, err := a.snapshot(r.Context())
	if err != nil {
		obs.WriteError(w, http.StatusServiceUnavailable, "store_unavailable", "canonical store unavailable")
		return
	}
	var lo, hi time.Time
	byCat := map[string]int{}
	for _, c := range Categories {
		byCat[c] = 0
	}
	bySev := map[string]int{"NORMAL": 0, "WASPADA": 0, "SIAGA": 0, "AWAS": 0}
	byArea, bySrc := map[string]int{}, map[string]int{}
	var in []slim
	for _, s := range snap.rows {
		if (!from.IsZero() && s.at.Before(from)) || (!to.IsZero() && !s.at.Before(to)) || (!cut.IsZero() && !s.ingested.Before(cut)) {
			continue
		}
		in = append(in, s)
		byCat[s.cat]++
		bySev[s.sev]++
		byArea[s.area]++
		bySrc[s.src]++
		if lo.IsZero() || s.at.Before(lo) {
			lo = s.at
		}
		if s.at.After(hi) {
			hi = s.at
		}
	}
	bucket := q.Get("bucket")
	if bucket == "" {
		span := hi.Sub(lo)
		if !from.IsZero() && !to.IsZero() {
			span = to.Sub(from)
		}
		switch {
		case span <= 48*time.Hour:
			bucket = "hour"
		case span <= 120*24*time.Hour:
			bucket = "day"
		case span <= 10*365*24*time.Hour:
			bucket = "month"
		default:
			bucket = "year"
		}
	}
	trunc := func(t time.Time) time.Time {
		switch bucket {
		case "hour":
			return t.Truncate(time.Hour)
		case "month":
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		case "year":
			return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		default:
			bucket = "day"
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		}
	}
	points := map[time.Time]map[string]int{}
	for _, s := range in {
		b := trunc(s.at.UTC())
		if points[b] == nil {
			points[b] = map[string]int{}
		}
		points[b][s.cat]++
	}
	keys := make([]time.Time, 0, len(points))
	for k := range points {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Before(keys[j]) })
	// every bucket between the first and the last one, empty ones as 0: a line
	// chart must not draw a straight line across a period without events
	if len(keys) > 1 {
		step := func(t time.Time) time.Time {
			switch bucket {
			case "hour":
				return t.Add(time.Hour)
			case "month":
				return t.AddDate(0, 1, 0)
			case "year":
				return t.AddDate(1, 0, 0)
			default:
				return t.AddDate(0, 0, 1)
			}
		}
		var full []time.Time
		for t := keys[0]; !t.After(keys[len(keys)-1]) && len(full) <= 2000; t = step(t) {
			full = append(full, t)
		}
		if len(full) <= 2000 {
			keys = full
		}
	}
	series := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		p := map[string]any{"t": k.Format(time.RFC3339)}
		for _, c := range Categories {
			p[c] = points[k][c]
		}
		series = append(series, p)
	}
	type area struct {
		Area  string `json:"area_name"`
		Count int    `json:"count"`
	}
	areas := make([]area, 0, len(byArea))
	for k, v := range byArea {
		areas = append(areas, area{k, v})
	}
	sort.Slice(areas, func(i, j int) bool {
		if areas[i].Count != areas[j].Count {
			return areas[i].Count > areas[j].Count
		}
		return strings.Compare(areas[i].Area, areas[j].Area) < 0
	})
	unavailable := snap.unavailable
	if unavailable == nil {
		unavailable = []string{}
	}
	if len(unavailable) > 0 {
		partialReads.Inc("analytics")
	}
	obs.WriteJSON(w, http.StatusOK, map[string]any{
		"generated_at": snap.at.UTC().Format(time.RFC3339Nano), "scan_ms": snap.took.Milliseconds(),
		"partial": len(unavailable) > 0, "unavailable_units": unavailable,
		"range":      map[string]any{"from": rfc(from), "to": rfc(to), "first": rfc(lo), "last": rfc(hi), "ingested_before": rfc(cut)},
		"total":      len(in),
		"categories": Categories, "by_category": byCat, "by_severity": bySev, "by_source": bySrc, "by_area": areas,
		"series":        map[string]any{"bucket": bucket, "points": series},
		"category_rule": "hazard_type; SEISMIC with a BMKG tsunami warning (attributes.warning_id) counts as TSUNAMI",
	})
}

func rfc(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}
