package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/andi-frame/orangsuksesaamiin/pkg/obs/schemacheck"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

const key = "svc-key"

type fixture struct {
	ts    *httptest.Server
	repo  *memory.Repo
	pvmbg *resilience.Tracker
	pvBrk *resilience.Breaker
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	repo := memory.New()
	ing := ingest.New(repo, log)
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	add := func(id string, typ domain.HazardType, src domain.Source, sev domain.Severity, off time.Duration, attrs map[string]any) {
		lat, lon := -6.1, 105.8
		h := &domain.HazardEvent{HazardID: id, Source: src, SourceRefID: id, HazardType: typ, Severity: sev,
			AreaName: "Area " + id, Latitude: &lat, Longitude: &lon, OccurredAt: base.Add(off), Attributes: attrs}
		if _, err := ing.Upsert(context.Background(), h, nil); err != nil {
			t.Fatal(err)
		}
	}
	add("bmkg-1", domain.HazardSeismic, domain.SourceBMKG, domain.SeverityNormal, 1*time.Minute, map[string]any{"magnitude": 4.0})
	add("bmkg-2", domain.HazardSeismic, domain.SourceBMKG, domain.SeverityAwas, 2*time.Minute, map[string]any{"magnitude": 7.5})
	add("pvmbg-1", domain.HazardVolcanic, domain.SourcePVMBG, domain.SeveritySiaga, 3*time.Minute, map[string]any{"eruption_count_24h": 4.0})
	add("pvmbg-2", domain.HazardVolcanic, domain.SourcePVMBG, domain.SeveritySiaga, 4*time.Minute, map[string]any{"confidence_level": 0.9})

	bb, pb := resilience.NewBreaker(2, time.Minute), resilience.NewBreaker(2, time.Minute)
	bt, pt := resilience.NewTracker("BMKG", bb), resilience.NewTracker("PVMBG", pb)
	bt.Success(time.Millisecond)
	pt.Success(time.Millisecond)
	srv := &Server{Repo: repo, Trackers: []*resilience.Tracker{bt, pt}, Quarantine: ingest.NewQuarantine(log, 10), ServiceKey: key, Log: log}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &fixture{ts: ts, repo: repo, pvmbg: pt, pvBrk: pb}
}

func (f *fixture) get(t *testing.T, path string, hdr map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.ts.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp, body
}

func ok() map[string]string { return map[string]string{HeaderServiceKey: key} }

func TestServiceKeyRequired(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/internal/hazards", "/internal/hazards/bmkg-1", "/internal/sources/status", "/internal/quarantine"} {
		if r, _ := f.get(t, p, nil); r.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s without key: %d", p, r.StatusCode)
		}
		if r, _ := f.get(t, p, map[string]string{HeaderServiceKey: "wrong"}); r.StatusCode != http.StatusForbidden {
			t.Errorf("%s wrong key: %d", p, r.StatusCode)
		}
		if r, _ := f.get(t, p, ok()); r.StatusCode != http.StatusOK {
			t.Errorf("%s valid key: %d", p, r.StatusCode)
		}
	}
	// a BMKG/PVMBG credential is not a service credential
	if r, _ := f.get(t, "/internal/hazards", map[string]string{"X-BMKG-Key": "k", "Authorization": "Bearer t"}); r.StatusCode != http.StatusUnauthorized {
		t.Errorf("foreign credentials accepted: %d", r.StatusCode)
	}
}

func TestProbesNeedNoKey(t *testing.T) {
	f := newFixture(t)
	if r, _ := f.get(t, "/health", nil); r.StatusCode != 200 {
		t.Fatalf("health %d", r.StatusCode)
	}
	if r, _ := f.get(t, "/ready", nil); r.StatusCode != 200 {
		t.Fatalf("ready %d", r.StatusCode)
	}
	f.repo.Fail(errors.New("db down"))
	if r, _ := f.get(t, "/ready", nil); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready must be 503 when the store is down, got %d", r.StatusCode)
	}
	if r, _ := f.get(t, "/health", nil); r.StatusCode != 200 {
		t.Fatal("liveness must stay 200 while the store is down")
	}
}

func items(body map[string]any) []map[string]any {
	var out []map[string]any
	for _, i := range body["items"].([]any) {
		out = append(out, i.(map[string]any))
	}
	return out
}

func TestListFiltersAndOrder(t *testing.T) {
	f := newFixture(t)
	_, all := f.get(t, "/internal/hazards", ok())
	got := items(all)
	if len(got) != 4 || got[0]["hazard_id"] != "pvmbg-2" || got[3]["hazard_id"] != "bmkg-1" {
		t.Fatalf("newest-first order: %v", got)
	}
	for _, c := range []struct {
		q    string
		want int
	}{
		{"hazard_type=SEISMIC", 2}, {"hazard_type=volcanic", 2}, {"source=PVMBG", 2},
		{"severity=awas", 1}, {"since=2026-10-01T09:03:00Z", 2}, {"limit=1", 1},
	} {
		if _, b := f.get(t, "/internal/hazards?"+c.q, ok()); len(items(b)) != c.want {
			t.Errorf("%s: %d items, want %d", c.q, len(items(b)), c.want)
		}
	}
	_, p1 := f.get(t, "/internal/hazards?limit=3", ok())
	next, _ := p1["next_cursor"].(string)
	if len(items(p1)) != 3 || next == "" {
		t.Fatal("expected a next_cursor")
	}
	if _, p2 := f.get(t, "/internal/hazards?limit=3&cursor="+next, ok()); len(items(p2)) != 1 {
		t.Fatalf("page 2: %v", p2)
	}
}

func TestListIncludesRawFieldsForTheClientAPIToFilter(t *testing.T) {
	f := newFixture(t)
	_, b := f.get(t, "/internal/hazards?hazard_type=VOLCANIC", ok())
	h := items(b)[0]
	for _, k := range []string{"hazard_id", "source", "source_ref_id", "hazard_type", "severity", "area_name",
		"latitude", "longitude", "occurred_at", "ingested_at", "attributes", "version", "updated_at"} {
		if _, has := h[k]; !has {
			t.Errorf("field %s missing", k)
		}
	}
	if h["attributes"].(map[string]any)["confidence_level"] != 0.9 {
		t.Errorf("attributes: %v", h["attributes"])
	}
}

func TestBadParametersAre400(t *testing.T) {
	f := newFixture(t)
	for _, q := range []string{"hazard_type=FLOOD", "source=X", "severity=RED", "since=yesterday", "limit=0", "limit=abc", "cursor=%25%25"} {
		if r, _ := f.get(t, "/internal/hazards?"+q, ok()); r.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d", q, r.StatusCode)
		}
	}
}

func TestGetOneAndNotFound(t *testing.T) {
	f := newFixture(t)
	if r, b := f.get(t, "/internal/hazards/bmkg-2", ok()); r.StatusCode != 200 || b["severity"] != "AWAS" {
		t.Fatalf("get: %d %v", r.StatusCode, b)
	}
	if r, b := f.get(t, "/internal/hazards/nope", ok()); r.StatusCode != 404 || b["error"] == nil {
		t.Fatalf("404: %d %v", r.StatusCode, b)
	}
}

// Freshness is what lets the Client-Facing API answer during a PVMBG outage
// with the last data plus stale_since (Problem 2).
func TestFreshnessReflectsSourceOutage(t *testing.T) {
	f := newFixture(t)
	_, b := f.get(t, "/internal/hazards?hazard_type=VOLCANIC", ok())
	fr := b["freshness"].(map[string]any)
	if fr["PVMBG"].(map[string]any)["state"] != "up" || fr["PVMBG"].(map[string]any)["stale_since"] != nil {
		t.Fatalf("healthy: %v", fr)
	}

	for i := 0; i < 2; i++ {
		f.pvmbg.Failure(errors.New("503"), time.Second)
		f.pvBrk.Failure()
	}
	r, b := f.get(t, "/internal/hazards?hazard_type=VOLCANIC", ok())
	if r.StatusCode != 200 || len(items(b)) != 2 {
		t.Fatalf("during the outage the store must still answer: %d %v", r.StatusCode, b)
	}
	fr = b["freshness"].(map[string]any)
	pv := fr["PVMBG"].(map[string]any)
	if pv["state"] != "down" || pv["stale_since"] == nil {
		t.Fatalf("outage not visible: %v", pv)
	}
	if fr["BMKG"].(map[string]any)["state"] != "up" {
		t.Fatal("BMKG must be unaffected")
	}

	_, st := f.get(t, "/internal/sources/status", ok())
	srcs := st["sources"].([]any)
	if len(srcs) != 2 || srcs[1].(map[string]any)["breaker"] != "open" {
		t.Fatalf("status: %v", st)
	}
}

// Everything the API returns must conform to the published HazardEvent contract.
func TestHazardsConformToContractSchema(t *testing.T) {
	sc, err := schemacheck.Load("../../../../contracts/schemas/hazard-event.json")
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	_, b := f.get(t, "/internal/hazards", ok())
	if len(items(b)) == 0 {
		t.Fatal("no items")
	}
	for i, h := range items(b) {
		if errs := sc.Validate(h); len(errs) != 0 {
			t.Fatalf("items[%d] violates hazard-event.json: %v", i, errs)
		}
	}
	_, one := f.get(t, "/internal/hazards/bmkg-2", ok())
	if errs := sc.Validate(one); len(errs) != 0 {
		t.Fatalf("GET one violates hazard-event.json: %v", errs)
	}
}
