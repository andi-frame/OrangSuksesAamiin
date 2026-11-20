package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

// missingUnit makes the store answer like a sharded store with one shard down.
type missingUnit struct {
	ports.Repository
	down bool
}

func (m *missingUnit) List(ctx context.Context, f ports.ListFilter) (ports.Page, error) {
	p, err := m.Repository.List(ctx, f)
	if m.down && err == nil {
		// the rows of shard-1 are not reachable: drop every second one
		var kept []domain.HazardEvent
		for i, h := range p.Items {
			if i%2 == 0 {
				kept = append(kept, h)
			}
		}
		p.Items, p.UnavailableUnits = kept, []string{"shard-1"}
	}
	return p, err
}

func TestAnalyticsMatchesAuditCountsAndFlagsPartial(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	repo := memory.New()
	ing := ingest.New(repo, log)
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	add := func(id string, typ domain.HazardType, src domain.Source, sev domain.Severity, area string, day int, attrs map[string]any) {
		h := &domain.HazardEvent{HazardID: id, Source: src, SourceRefID: id, HazardType: typ, Severity: sev,
			AreaName: area, OccurredAt: base.AddDate(0, 0, day), Attributes: attrs}
		if _, err := ing.Upsert(context.Background(), h, nil); err != nil {
			t.Fatal(err)
		}
	}
	add("bmkg-1", domain.HazardSeismic, domain.SourceBMKG, domain.SeverityWaspada, "Aceh", 0, map[string]any{})
	add("bmkg-2", domain.HazardSeismic, domain.SourceBMKG, domain.SeverityAwas, "Aceh", 1, map[string]any{"warning_id": "TW-1"})
	add("pvmbg-1", domain.HazardVolcanic, domain.SourcePVMBG, domain.SeveritySiaga, "Jawa Tengah", 1, nil)
	add("satelit-1", domain.HazardWildfire, domain.SourceSatellite, domain.SeveritySiaga, "Riau", 2, nil)
	add("sensor-pantai-1", domain.HazardCoastal, domain.SourceCoastal, domain.SeverityNormal, "Aceh", 2, nil)
	add("sensor-pantai-2", domain.HazardCoastal, domain.SourceCoastal, domain.SeverityAwas, "Sumatera Barat", 2, nil)

	store := &missingUnit{Repository: repo}
	bt := resilience.NewTracker("BMKG", resilience.NewBreaker(1, time.Minute))
	srv := &Server{Repo: store, Status: fixed{bt}, ServiceKey: key, Log: log}
	srv.analytics.CacheTTL = time.Nanosecond
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	f := &fixture{ts: ts}

	resp, body := f.get(t, "/internal/analytics/summary", ok())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d %v", resp.StatusCode, body)
	}
	cat := body["by_category"].(map[string]any)
	want := map[string]float64{"SEISMIC": 1, "TSUNAMI": 1, "VOLCANIC": 1, "WILDFIRE": 1, "COASTAL": 2}
	for k, v := range want {
		if cat[k] != v {
			t.Errorf("by_category[%s] = %v, want %v", k, cat[k], v)
		}
	}
	if body["total"] != 6.0 || body["partial"] != false {
		t.Fatalf("total/partial: %v %v", body["total"], body["partial"])
	}
	sev := body["by_severity"].(map[string]any)
	if sev["AWAS"] != 2.0 || sev["SIAGA"] != 2.0 || sev["NORMAL"] != 1.0 || sev["WASPADA"] != 1.0 {
		t.Fatalf("by_severity %v", sev)
	}
	areas := body["by_area"].([]any)
	if top := areas[0].(map[string]any); top["area_name"] != "Aceh" || top["count"] != 3.0 {
		t.Fatalf("by_area %v", areas)
	}
	series := body["series"].(map[string]any)
	if series["bucket"] != "hour" && series["bucket"] != "day" {
		t.Fatalf("bucket %v", series["bucket"])
	}

	// time range filter
	_, ranged := f.get(t, "/internal/analytics/summary?from=2026-11-02T00:00:00Z&to=2026-11-03T00:00:00Z", ok())
	if ranged["total"] != 2.0 {
		t.Fatalf("range total %v", ranged["total"])
	}

	// a cut on ingested_at (comparison with the Audit API while ingestion goes on)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if _, b := f.get(t, "/internal/analytics/summary?ingested_before="+future, ok()); b["total"] != 6.0 {
		t.Fatalf("ingested_before in the future: %v", b["total"])
	}
	if _, b := f.get(t, "/internal/analytics/summary?ingested_before=2000-01-01T00:00:00Z", ok()); b["total"] != 0.0 {
		t.Fatalf("ingested_before in the past: %v", b["total"])
	}

	// one unit down: answered, but flagged incomplete
	store.down = true
	_, partial := f.get(t, "/internal/analytics/summary", ok())
	if partial["partial"] != true || len(partial["unavailable_units"].([]any)) != 1 {
		t.Fatalf("a missing unit must be flagged: %v %v", partial["partial"], partial["unavailable_units"])
	}
	if r, _ := f.get(t, "/internal/analytics/summary", nil); r.StatusCode != http.StatusUnauthorized {
		t.Fatal("service key required")
	}
}
