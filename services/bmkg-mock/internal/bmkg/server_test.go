package bmkg

import (
	"bytes"
	"encoding/json"
	"github.com/andi-frame/orangsuksesaamiin/pkg/obs/schemacheck"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func testServer(t *testing.T) (*httptest.Server, *Store, *Generator, Config) {
	t.Helper()
	cfg := Config{
		Port: "0", APIKey: "test-bmkg-key", AdminToken: "test-admin",
		DelayMin: 50 * time.Millisecond, DelayMax: 150 * time.Millisecond,
		EventInterval: 0, WarningDelayMin: 20 * time.Millisecond, WarningDelayMax: 40 * time.Millisecond,
		SeedCount: 24,
	}
	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	store := NewStore()
	gen := NewGenerator(cfg, store, log)
	gen.Seed(time.Now())
	ts := httptest.NewServer(NewHandler(cfg, store, gen, log))
	t.Cleanup(ts.Close)
	return ts, store, gen, cfg
}

func get(t *testing.T, url string, hdr map[string]string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestAuthMatrix(t *testing.T) {
	ts, _, _, cfg := testServer(t)
	cases := []struct {
		name string
		hdr  map[string]string
		want int
	}{
		{"no credential", nil, http.StatusUnauthorized},
		{"wrong key", map[string]string{HeaderKey: "nope"}, http.StatusForbidden},
		{"PVMBG bearer token", map[string]string{"Authorization": "Bearer pvmbg.v1.x.y"}, http.StatusForbidden},
		{"valid key", map[string]string{HeaderKey: cfg.APIKey}, http.StatusOK},
	}
	for _, path := range []string{"/seismic-events", "/tsunami-warnings"} {
		for _, c := range cases {
			if got := get(t, ts.URL+path, c.hdr).StatusCode; got != c.want {
				t.Errorf("%s %s: got %d want %d", path, c.name, got, c.want)
			}
		}
	}
}

func TestHealthNeedsNoCredential(t *testing.T) {
	ts, _, _, _ := testServer(t)
	if got := get(t, ts.URL+"/health", nil).StatusCode; got != 200 {
		t.Fatalf("health = %d", got)
	}
}

func TestSeedAtLeast20AndWarningsOnlyForTsunamiEvents(t *testing.T) {
	ts, store, _, cfg := testServer(t)
	var events []SeismicEvent
	resp := get(t, ts.URL+"/seismic-events", map[string]string{HeaderKey: cfg.APIKey})
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) < 20 {
		t.Fatalf("seed has %d events, contract requires >= 20", len(events))
	}
	tsunami := map[string]bool{}
	var haveNormal, haveWaspada, haveSiaga bool
	for _, e := range events {
		tsunami[e.EventID] = e.PotentialTsunami
		switch {
		case !e.PotentialTsunami && e.Magnitude < 5.0:
			haveNormal = true
		case !e.PotentialTsunami && e.Magnitude < 6.5:
			haveWaspada = true
		case !e.PotentialTsunami:
			haveSiaga = true
		}
	}
	if !haveNormal || !haveWaspada || !haveSiaga {
		t.Errorf("seed lacks a severity branch: normal=%v waspada=%v siaga=%v", haveNormal, haveWaspada, haveSiaga)
	}
	levels := map[string]bool{}
	for _, w := range store.WarningsSince(time.Time{}) {
		if !tsunami[w.RelatedEventID] {
			t.Errorf("warning %s relates to event %s which has potential_tsunami=false", w.WarningID, w.RelatedEventID)
		}
		levels[w.ThreatLevel] = true
	}
	for _, l := range []string{"Waspada", "Siaga", "Awas"} {
		if !levels[l] {
			t.Errorf("seed lacks a %s warning", l)
		}
	}
}

func TestDelayWithinContractRange(t *testing.T) {
	ts, _, _, cfg := testServer(t)
	for i := 0; i < 5; i++ {
		start := time.Now()
		get(t, ts.URL+"/seismic-events", map[string]string{HeaderKey: cfg.APIKey})
		d := time.Since(start)
		if d < 50*time.Millisecond {
			t.Errorf("response after %v, want >= 50ms", d)
		}
		if d > 400*time.Millisecond {
			t.Errorf("response after %v, way above 150ms", d)
		}
	}
}

func TestSinceFilterAndValidation(t *testing.T) {
	ts, store, _, cfg := testServer(t)
	hdr := map[string]string{HeaderKey: cfg.APIKey}
	all := store.EventsSince(time.Time{})
	cut := all[len(all)-3].OccurredAt
	var got []SeismicEvent
	resp := get(t, ts.URL+"/seismic-events?since="+url.QueryEscape(cut.Format(time.RFC3339Nano)), hdr)
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if len(got) != 2 {
		t.Fatalf("since filter returned %d events, want 2", len(got))
	}
	if get(t, ts.URL+"/seismic-events?since=garbage", hdr).StatusCode != http.StatusBadRequest {
		t.Fatal("invalid since must be 400")
	}
}

func TestGenerateProducesEventThenLateWarning(t *testing.T) {
	ts, store, _, _ := testServer(t)
	before, wBefore := store.Counts()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/generate", bytes.NewBufferString(`{"count":1,"magnitude":7.6,"tsunami":true}`))
	req.Header.Set(HeaderAdmin, "test-admin")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("generate failed: %v %v", err, resp)
	}
	resp.Body.Close()
	after, wAfter := store.Counts()
	if after != before+1 {
		t.Fatalf("events %d -> %d", before, after)
	}
	if wAfter != wBefore {
		t.Fatal("warning must be issued later than the event, not at the same instant")
	}
	time.Sleep(150 * time.Millisecond)
	if _, w := store.Counts(); w != wBefore+1 {
		t.Fatalf("late warning not issued (%d -> %d)", wBefore, w)
	}
	// admin needs its own token
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/generate", nil)
	resp2, _ := http.DefaultClient.Do(req2)
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin without token = %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}

// The mock must honour the M1 contract exactly, as defined in contracts/schemas.
func TestResponsesConformToContractSchemas(t *testing.T) {
	ts, _, gen, cfg := testServer(t)
	tsu := true
	gen.Generate(2, 7.6, &tsu) // a fresh tsunami event; its warning follows shortly
	time.Sleep(100 * time.Millisecond)
	hdr := map[string]string{HeaderKey: cfg.APIKey}
	for path, schema := range map[string]string{
		"/seismic-events":   "seismic-event.json",
		"/tsunami-warnings": "tsunami-warning.json",
	} {
		sc, err := schemacheck.Load("../../../../contracts/schemas/" + schema)
		if err != nil {
			t.Fatal(err)
		}
		resp := get(t, ts.URL+path, hdr)
		var items []any
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			t.Fatal(err)
		}
		if len(items) < 20 && path == "/seismic-events" {
			t.Fatalf("%s: only %d items", path, len(items))
		}
		for i, it := range items {
			if errs := sc.Validate(it); len(errs) != 0 {
				t.Fatalf("%s[%d] violates %s: %v", path, i, schema, errs)
			}
		}
	}
}
