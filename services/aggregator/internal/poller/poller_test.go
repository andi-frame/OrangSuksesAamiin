package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/andi-frame/orangsuksesaamiin/pkg/obs/schemacheck"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

const (
	bmkgKey    = "k-bmkg"
	pvmbgToken = "t-pvmbg"
)

var t0 = time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

func ts(off time.Duration) string { return t0.Add(off).Format(time.RFC3339Nano) }

func testLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

// ---------- fake upstreams ----------

type fakeBMKG struct {
	*httptest.Server
	mu       sync.Mutex
	events   []map[string]any
	warnings []map[string]any
	fail     bool
	sinces   []string // `since` of every /seismic-events request
}

func newFakeBMKG(t *testing.T) *fakeBMKG {
	f := &fakeBMKG{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("X-BMKG-Key") != bmkgKey {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		if f.fail {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		since, _ := time.Parse(time.RFC3339Nano, r.URL.Query().Get("since"))
		var src []map[string]any
		var field string
		switch r.URL.Path {
		case "/seismic-events":
			src, field = f.events, "occurred_at"
			f.sinces = append(f.sinces, r.URL.Query().Get("since"))
		case "/tsunami-warnings":
			src, field = f.warnings, "issued_at"
		default:
			http.NotFound(w, r)
			return
		}
		out := []map[string]any{}
		for _, m := range src {
			tm, _ := time.Parse(time.RFC3339Nano, m[field].(string))
			if tm.After(since) {
				out = append(out, m)
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeBMKG) addEvent(id string, mag float64, tsunami bool, off time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, map[string]any{
		"event_id": id, "magnitude": mag, "depth_km": 10.0, "epicenter_lat": -6.1, "epicenter_lon": 105.8,
		"region_name": "Selat Sunda", "occurred_at": ts(off), "potential_tsunami": tsunami,
	})
}

func (f *fakeBMKG) addWarning(id, event, level string, off time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.warnings = append(f.warnings, map[string]any{
		"warning_id": id, "related_event_id": event, "threat_level": level,
		"affected_zones": []string{"Anyer"}, "estimated_arrival": ts(off + 20*time.Minute), "issued_at": ts(off),
	})
}

func (f *fakeBMKG) setFail(v bool) { f.mu.Lock(); f.fail = v; f.mu.Unlock() }

type fakePVMBG struct {
	*httptest.Server
	mu      sync.Mutex
	reports []map[string]any
	status  int
	delay   time.Duration
	calls   atomic.Int64
}

func newFakePVMBG(t *testing.T) *fakePVMBG {
	f := &fakePVMBG{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		f.mu.Lock()
		delay, status := f.delay, f.status
		f.mu.Unlock()
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if r.Header.Get("Authorization") != "Bearer "+pvmbgToken {
			http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
			return
		}
		if status != 0 {
			http.Error(w, "down", status)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		since, _ := time.Parse(time.RFC3339Nano, r.URL.Query().Get("since"))
		out := []map[string]any{}
		for _, m := range f.reports {
			tm, _ := time.Parse(time.RFC3339Nano, m["reported_at"].(string))
			if tm.After(since) {
				out = append(out, m)
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakePVMBG) add(id, volcano, level string, off time.Duration, extra map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := map[string]any{
		"report_id": id, "volcano_id": volcano, "alert_level": level,
		"eruption_count_24h": 3, "ash_column_height_m": 900.5, "reported_at": ts(off),
	}
	for k, v := range extra {
		m[k] = v
	}
	f.reports = append(f.reports, m)
}

func (f *fakePVMBG) setStatus(s int)          { f.mu.Lock(); f.status = s; f.mu.Unlock() }
func (f *fakePVMBG) setDelay(d time.Duration) { f.mu.Lock(); f.delay = d; f.mu.Unlock() }

// ---------- harness ----------

type harness struct {
	repo  *memory.Repo
	ing   *ingest.Ingestor
	quar  *ingest.Quarantine
	bmkg  *BMKGSource
	pvmbg *PVMBGSource
}

var volcanoes = mapping.VolcanoTable{
	"merapi": {Name: "Gunung Merapi", Latitude: -7.5407, Longitude: 110.4457},
}

func newHarness(t *testing.T, fb *fakeBMKG, fp *fakePVMBG) *harness {
	t.Helper()
	log := testLog()
	repo := memory.New()
	ing := ingest.New(repo, log)
	quar := ingest.NewQuarantine(log, 50)
	h := &harness{repo: repo, ing: ing, quar: quar}
	if fb != nil {
		h.bmkg = &BMKGSource{
			Client: &sources.BMKG{HTTP: obs.NewHTTPClient(log, "bmkg", 2*time.Second), BaseURL: fb.URL, APIKey: bmkgKey},
			Repo:   repo, Ingestor: ing, Quarantine: quar, Overlap: 5 * time.Second, PendingTTL: time.Minute, Log: log,
		}
	}
	if fp != nil {
		h.pvmbg = &PVMBGSource{
			Client: &sources.PVMBG{HTTP: obs.NewHTTPClient(log, "pvmbg", 2*time.Second), BaseURL: fp.URL, Token: pvmbgToken},
			Repo:   repo, Ingestor: ing, Quarantine: quar, Volcanoes: volcanoes, Overlap: 5 * time.Second, Log: log,
		}
	}
	return h
}

func (h *harness) hazard(t *testing.T, id string) domain.HazardEvent {
	t.Helper()
	got, err := h.repo.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("hazard %s: %v", id, err)
	}
	return *got
}

func (h *harness) outbox(t *testing.T) []ports.OutboxEntry {
	t.Helper()
	o, _ := h.repo.PendingOutbox(context.Background(), 1000)
	return o
}

func pollOK(t *testing.T, s Source) Stats {
	t.Helper()
	st, err := s.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("%s poll: %v", s.Name(), err)
	}
	return st
}

// ---------- BMKG ----------

func TestBMKGMapsAllSeverityBranches(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("e-low", 4.0, false, 1*time.Second)
	fb.addEvent("e-mid", 5.5, false, 2*time.Second)
	fb.addEvent("e-high", 6.8, false, 3*time.Second)
	fb.addEvent("e-tsu", 6.0, true, 4*time.Second)
	fb.addWarning("w-tsu", "e-tsu", "Awas", 5*time.Second)
	h := newHarness(t, fb, nil)

	st := pollOK(t, h.bmkg)
	if st.Created != 4 || st.Quarantined != 0 {
		t.Fatalf("stats: %+v", st)
	}
	want := map[string]domain.Severity{
		"bmkg-e-low": domain.SeverityNormal, "bmkg-e-mid": domain.SeverityWaspada,
		"bmkg-e-high": domain.SeveritySiaga, "bmkg-e-tsu": domain.SeverityAwas,
	}
	for id, sev := range want {
		if got := h.hazard(t, id); got.Severity != sev || got.Version != 1 {
			t.Errorf("%s: severity %s v%d, want %s v1", id, got.Severity, got.Version, sev)
		}
	}
	tsu := h.hazard(t, "bmkg-e-tsu")
	if tsu.Attributes["warning_id"] != "w-tsu" || tsu.Attributes["threat_level"] != "Awas" {
		t.Errorf("warning fields not in attributes: %v", tsu.Attributes)
	}
	if len(h.outbox(t)) != 4 {
		t.Fatalf("outbox = %d, want 4 created events", len(h.outbox(t)))
	}

	// idempotent: polling again (overlap window re-delivers records) changes nothing
	st = pollOK(t, h.bmkg)
	if st.Created != 0 || st.Updated != 0 {
		t.Fatalf("second poll must be a no-op: %+v", st)
	}
	if len(h.outbox(t)) != 4 {
		t.Fatalf("no-op poll produced events: %d", len(h.outbox(t)))
	}
}

func TestBMKGLateWarningRaisesSeverityAsNewVersion(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("e1", 6.0, true, 1*time.Second)
	h := newHarness(t, fb, nil)
	pollOK(t, h.bmkg)
	if got := h.hazard(t, "bmkg-e1"); got.Severity != domain.SeverityWaspada || got.Version != 1 {
		t.Fatalf("before warning: %s v%d", got.Severity, got.Version)
	}

	fb.addWarning("w1", "e1", "Siaga", 6*time.Second) // issued AFTER the event was ingested
	st := pollOK(t, h.bmkg)
	if st.Updated != 1 {
		t.Fatalf("stats after late warning: %+v", st)
	}
	got := h.hazard(t, "bmkg-e1")
	if got.Severity != domain.SeveritySiaga || got.Version != 2 || got.Attributes["warning_id"] != "w1" {
		t.Fatalf("after late warning: %s v%d %v", got.Severity, got.Version, got.Attributes)
	}
	ob := h.outbox(t)
	if len(ob) != 2 || ob[0].EventID != "bmkg-e1:1" || ob[1].EventID != "bmkg-e1:2" {
		t.Fatalf("outbox: %+v", ob)
	}
	var env domain.Envelope
	_ = json.Unmarshal(ob[1].Payload, &env)
	if env.Kind != domain.KindUpdated || env.Hazard.Severity != domain.SeveritySiaga {
		t.Fatalf("v2 envelope: %+v", env)
	}

	// further polls (event + warning both re-delivered by the overlap) must not flip-flop
	for i := 0; i < 3; i++ {
		if st := pollOK(t, h.bmkg); st.Created+st.Updated != 0 {
			t.Fatalf("poll %d not idempotent: %+v", i, st)
		}
	}
	if got := h.hazard(t, "bmkg-e1"); got.Version != 2 || got.Severity != domain.SeveritySiaga {
		t.Fatalf("flip-flop: %s v%d", got.Severity, got.Version)
	}
}

func TestBMKGWarningBeforeEventWaitsInPending(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addWarning("w1", "e1", "Awas", 2*time.Second)
	h := newHarness(t, fb, nil)
	pollOK(t, h.bmkg)
	if _, err := h.repo.Get(context.Background(), "bmkg-e1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("nothing to store yet")
	}
	if c, _, _ := h.repo.GetCursor(context.Background(), CursorBMKGWarnings); c.After(t0.Add(2*time.Second - time.Microsecond)) {
		t.Fatalf("warnings watermark must not pass an unmatched warning: %v", c)
	}
	fb.addEvent("e1", 6.0, true, 1*time.Second) // the event shows up later
	pollOK(t, h.bmkg)
	got := h.hazard(t, "bmkg-e1")
	if got.Severity != domain.SeverityAwas || got.Version != 1 {
		t.Fatalf("got %s v%d, want AWAS v1 (warning folded into the first version)", got.Severity, got.Version)
	}
}

func TestBMKGOrphanWarningExpires(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addWarning("w-orphan", "ghost", "Awas", 2*time.Second)
	h := newHarness(t, fb, nil)
	h.bmkg.PendingTTL = 30 * time.Millisecond
	pollOK(t, h.bmkg)
	time.Sleep(60 * time.Millisecond)
	pollOK(t, h.bmkg)
	if got := h.quar.Total()["BMKG"]; got != 1 {
		t.Fatalf("orphan warning should be quarantined once, got %d", got)
	}
}

func TestBMKGQuarantinesBadRecordAndContinues(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("good", 5.2, false, 1*time.Second)
	fb.mu.Lock()
	fb.events = append(fb.events, map[string]any{ // magnitude changed type: unsupported schema change
		"event_id": "bad", "magnitude": "big", "epicenter_lat": 1.0, "epicenter_lon": 2.0,
		"region_name": "X", "occurred_at": ts(2 * time.Second),
	})
	fb.mu.Unlock()
	h := newHarness(t, fb, nil)
	st := pollOK(t, h.bmkg)
	if st.Created != 1 || st.Quarantined != 1 {
		t.Fatalf("stats: %+v", st)
	}
	// the overlap window re-delivers the bad record: it must not be counted twice
	for i := 0; i < 3; i++ {
		if again := pollOK(t, h.bmkg); again.Quarantined != 0 {
			t.Fatalf("re-polled bad record counted again: %+v", again)
		}
	}
	if h.quar.Total()["BMKG"] != 1 {
		t.Fatalf("quarantine total = %v, want 1", h.quar.Total())
	}
	rec := h.quar.Recent()
	if len(rec) != 1 || rec[0].RecordID != "bad" || rec[0].Field != "magnitude" {
		t.Fatalf("quarantine: %+v", rec)
	}
}

func TestBMKGUsesPersistedWatermark(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("e1", 5.0, false, 10*time.Second)
	h := newHarness(t, fb, nil)
	pollOK(t, h.bmkg)
	pollOK(t, h.bmkg)
	fb.mu.Lock()
	sinces := append([]string(nil), fb.sinces...)
	fb.mu.Unlock()
	if sinces[0] != "" {
		t.Fatalf("first poll must ask for everything, got since=%q", sinces[0])
	}
	got, err := time.Parse(time.RFC3339Nano, sinces[1])
	if err != nil || !got.Equal(t0.Add(10*time.Second-5*time.Second)) {
		t.Fatalf("second since = %q, want watermark minus 5s overlap", sinces[1])
	}
	// a rebuilt aggregator sharing the same store resumes from the watermark
	h2 := newHarness(t, fb, nil)
	h2.bmkg.Repo, h2.bmkg.Ingestor = h.repo, h.ing
	pollOK(t, h2.bmkg)
	fb.mu.Lock()
	last := fb.sinces[len(fb.sinces)-1]
	fb.mu.Unlock()
	if last == "" {
		t.Fatal("restarted aggregator lost its watermark")
	}
}

func TestBMKGHalfFailureStillStoresWhatItGot(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("e1", 5.0, false, 1*time.Second)
	h := newHarness(t, fb, nil)
	// warnings endpoint alone fails
	orig := fb.Server.Config.Handler
	fb.Server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tsunami-warnings" {
			http.Error(w, "boom", http.StatusBadGateway)
			return
		}
		orig.ServeHTTP(w, r)
	})
	st, err := h.bmkg.PollOnce(context.Background())
	var se *sources.SourceError
	if !errors.As(err, &se) {
		t.Fatalf("partial failure must surface as SourceError, got %v", err)
	}
	if st.Created != 1 {
		t.Fatalf("events must still be stored: %+v", st)
	}
}

// ---------- PVMBG / Problem 1 ----------

func TestPVMBGSchemaChangeWithoutRestart(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("r-old-1", "merapi", "Siaga", 1*time.Second, nil)
	fp.add("r-old-2", "merapi", "Waspada", 2*time.Second, nil)
	h := newHarness(t, nil, fp)

	// 1) initial schema
	pollOK(t, h.pvmbg)
	old := h.hazard(t, "pvmbg-r-old-1")
	if old.Severity != domain.SeveritySiaga || old.AreaName != "Gunung Merapi" || *old.Latitude != -7.5407 {
		t.Fatalf("mapping rule violated: %+v", old)
	}
	if _, has := old.Attributes["confidence_level"]; has {
		t.Fatal("initial schema has no confidence_level")
	}

	// 2) PVMBG starts sending confidence_level (same running process, no reload)
	fp.add("r-new-1", "merapi", "Awas", 3*time.Second, map[string]any{"confidence_level": 0.87})
	fp.add("r-new-2", "merapi", "Awas", 4*time.Second, map[string]any{"confidence_level": 0.6, "brand_new_field": map[string]any{"x": []int{1}}})
	st := pollOK(t, h.pvmbg)
	if st.Created != 2 || st.Quarantined != 0 {
		t.Fatalf("after schema change: %+v", st)
	}

	// 3) valid HazardEvents, field kept per the declared policy, old records untouched
	neu := h.hazard(t, "pvmbg-r-new-1")
	if neu.Severity != domain.SeverityAwas || neu.Attributes["confidence_level"] != 0.87 {
		t.Fatalf("new record: %+v", neu)
	}
	if _, ok := h.hazard(t, "pvmbg-r-new-2").Attributes["brand_new_field"]; !ok {
		t.Fatal("completely unknown field must be forwarded to attributes too")
	}
	if got := h.hazard(t, "pvmbg-r-old-1"); !domain.SameContent(old, got) || got.Version != 1 {
		t.Fatal("record stored before the change must be unchanged")
	}
	page, _ := h.repo.List(context.Background(), ports.ListFilter{HazardType: domain.HazardVolcanic})
	if len(page.Items) != 4 {
		t.Fatalf("records before and after the change must be listed side by side, got %d", len(page.Items))
	}
}

func TestPVMBGBadRecordDoesNotBlockWatermark(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("ok-1", "merapi", "Normal", 1*time.Second, nil)
	fp.add("bad", "merapi", "Darurat", 2*time.Second, nil) // unknown enum value
	fp.add("ok-2", "merapi", "Awas", 3*time.Second, nil)
	h := newHarness(t, nil, fp)
	st := pollOK(t, h.pvmbg)
	if st.Created != 2 || st.Quarantined != 1 {
		t.Fatalf("stats: %+v", st)
	}
	if c, _, _ := h.repo.GetCursor(context.Background(), CursorPVMBG); !c.Equal(t0.Add(3 * time.Second)) {
		t.Fatalf("watermark = %v", c)
	}
}

// ---------- failure handling (Problem 2) ----------

func newLoop(s Source, interval time.Duration) *Loop {
	b := resilience.NewBreaker(2, 150*time.Millisecond)
	return &Loop{
		Source: s, Breaker: b, Tracker: resilience.NewTracker(s.Name(), b),
		Interval: interval, CycleTimeout: 2 * time.Second, Log: testLog(),
	}
}

func TestSourceFailureOpensBreakerKeepsDataAndRecovers(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("r1", "merapi", "Siaga", 1*time.Second, nil)
	h := newHarness(t, nil, fp)
	l := newLoop(h.pvmbg, time.Hour)
	ctx := context.Background()

	l.cycle(ctx)
	if s := l.Tracker.Snapshot(); s.State != "up" {
		t.Fatalf("state: %+v", s)
	}
	stored := h.hazard(t, "pvmbg-r1")
	okAt := *l.Tracker.Snapshot().LastSuccessAt

	fp.setStatus(http.StatusServiceUnavailable) // outage
	l.cycle(ctx)
	if s := l.Tracker.Snapshot(); s.State != "degraded" || s.StaleSince == nil {
		t.Fatalf("after 1 failure: %+v", s)
	}
	l.cycle(ctx)
	s := l.Tracker.Snapshot()
	if s.State != "down" || s.Breaker != "open" || !s.StaleSince.Equal(okAt) {
		t.Fatalf("after 2 failures: %+v", s)
	}
	callsWhenOpen := fp.calls.Load()
	l.cycle(ctx) // breaker open: must not even call the dead source
	if fp.calls.Load() != callsWhenOpen || l.Tracker.Snapshot().SkippedByBreaker == 0 {
		t.Fatal("an open breaker must skip the call")
	}
	if got := h.hazard(t, "pvmbg-r1"); !domain.SameContent(stored, got) {
		t.Fatal("stored data must survive the outage untouched")
	}

	fp.setStatus(0) // outage ends - no restart
	time.Sleep(200 * time.Millisecond)
	l.cycle(ctx) // half-open probe
	if s := l.Tracker.Snapshot(); s.State != "up" || s.StaleSince != nil {
		t.Fatalf("recovery: %+v", s)
	}
}

func TestCredentialForOtherAgencyIsRejectedUpstream(t *testing.T) {
	fp := newFakePVMBG(t)
	h := newHarness(t, nil, fp)
	h.pvmbg.Client.Token = bmkgKey // wrong-domain credential
	_, err := h.pvmbg.PollOnce(context.Background())
	var se *sources.SourceError
	if !errors.As(err, &se) || !se.IsAuth() {
		t.Fatalf("want an auth SourceError, got %v", err)
	}
}

func TestStoreFailureIsNotBlamedOnTheSource(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("r1", "merapi", "Siaga", 1*time.Second, nil)
	h := newHarness(t, nil, fp)
	h.repo.Fail(errors.New("db down"))
	l := newLoop(h.pvmbg, time.Hour)
	for i := 0; i < 5; i++ {
		l.cycle(context.Background())
	}
	if s := l.Tracker.Snapshot(); s.Breaker != "closed" || s.ConsecutiveFailures != 0 {
		t.Fatalf("our own DB failure must not trip PVMBG's breaker: %+v", s)
	}
}

// The heart of Problem 2 at the ingest layer: a slow source must not hold up a
// fast one, and cycles of one source never overlap.
func TestSlowSourceDoesNotBlockFastSourceAndCyclesNeverOverlap(t *testing.T) {
	fb := newFakeBMKG(t)
	fb.addEvent("e1", 5.0, false, 1*time.Second)
	fp := newFakePVMBG(t)
	fp.add("r1", "merapi", "Siaga", 1*time.Second, nil)
	fp.setDelay(600 * time.Millisecond) // "PVMBG is slow"
	h := newHarness(t, fb, fp)

	var bmkgCycles, running, maxRunning atomic.Int64
	counting := &countingSource{Source: h.bmkg, cycles: &bmkgCycles, running: &running, max: &maxRunning}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	fast, slow := newLoop(counting, 20*time.Millisecond), newLoop(h.pvmbg, 20*time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); fast.Run(ctx) }()
	go func() { defer wg.Done(); slow.Run(ctx) }()
	wg.Wait()

	// PVMBG can complete at most ~2 cycles in 1.5 s; BMKG must have run many times
	if bmkgCycles.Load() < 20 {
		t.Fatalf("BMKG loop ran only %d cycles while PVMBG was slow: head-of-line blocking", bmkgCycles.Load())
	}
	if maxRunning.Load() != 1 {
		t.Fatalf("BMKG cycles overlapped (max %d concurrent)", maxRunning.Load())
	}
	if fp.calls.Load() > 4 {
		t.Fatalf("PVMBG got %d requests in 1.5 s with a 600 ms delay: cycles overlap", fp.calls.Load())
	}
}

type countingSource struct {
	Source
	cycles, running, max *atomic.Int64
}

func (c *countingSource) PollOnce(ctx context.Context) (Stats, error) {
	c.cycles.Add(1)
	n := c.running.Add(1)
	defer c.running.Add(-1)
	for {
		m := c.max.Load()
		if n <= m || c.max.CompareAndSwap(m, n) {
			break
		}
	}
	return c.Source.PollOnce(ctx)
}

// Each polling cycle is one traceable flow: the same correlation ID appears on
// the outbound calls and on the stored/rejected records.
func TestCycleCarriesOneCorrelationID(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("r1", "merapi", "Siaga", 1*time.Second, nil)
	var buf bytes.Buffer
	log := slog.New(slogCtx{slog.NewJSONHandler(&buf, nil)})
	h := newHarness(t, nil, fp)
	h.pvmbg.Client.HTTP = obs.NewHTTPClient(log, "pvmbg", time.Second)
	h.ing.Log = log
	l := newLoop(h.pvmbg, time.Hour)
	l.Log = log
	l.cycle(context.Background())

	seen := map[string]map[string]bool{} // msg -> set of cids
	dec := json.NewDecoder(&buf)
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			t.Fatal(err)
		}
		msg, _ := m["msg"].(string)
		cid, _ := m["cid"].(string)
		if seen[msg] == nil {
			seen[msg] = map[string]bool{}
		}
		seen[msg][cid] = true
	}
	var all []string
	for _, msg := range []string{"outbound_call", "hazard_stored", "poll_ok"} {
		if len(seen[msg]) != 1 {
			t.Fatalf("%s: want exactly one cid, got %v", msg, seen[msg])
		}
		for c := range seen[msg] {
			if c == "" {
				t.Fatalf("%s has no cid", msg)
			}
			all = append(all, c)
		}
	}
	if all[0] != all[1] || all[1] != all[2] {
		t.Fatalf("outbound call, storage and poll result carry different cids: %v", all)
	}
}

// slogCtx copies the correlation ID of the context into the record like obs does.
type slogCtx struct{ slog.Handler }

func (h slogCtx) Handle(ctx context.Context, r slog.Record) error {
	if id := obs.CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("cid", id))
	}
	return h.Handler.Handle(ctx, r)
}

// Every event that leaves the Aggregator towards the broker follows the published envelope contract.
func TestOutboxEnvelopesConformToContractSchema(t *testing.T) {
	sc, err := schemacheck.Load("../../../../contracts/schemas/kafka-envelope.json")
	if err != nil {
		t.Fatal(err)
	}
	fb, fp := newFakeBMKG(t), newFakePVMBG(t)
	fb.addEvent("e1", 6.0, true, 1*time.Second)
	fb.addWarning("w1", "e1", "Siaga", 2*time.Second)
	fp.add("r1", "merapi", "Awas", 1*time.Second, map[string]any{"confidence_level": 0.9})
	h := newHarness(t, fb, fp)
	pollOK(t, h.bmkg)
	pollOK(t, h.pvmbg)
	fb.addWarning("w2", "e1", "Awas", 5*time.Second) // produces an "updated" event too
	pollOK(t, h.bmkg)
	entries := h.outbox(t)
	if len(entries) < 3 {
		t.Fatalf("expected created+created+updated, got %d", len(entries))
	}
	kinds := map[string]bool{}
	for _, e := range entries {
		if errs := sc.ValidateJSON(e.Payload); len(errs) != 0 {
			t.Fatalf("%s violates kafka-envelope.json: %v", e.EventID, errs)
		}
		var env domain.Envelope
		_ = json.Unmarshal(e.Payload, &env)
		kinds[env.Kind] = true
	}
	if !kinds["created"] || !kinds["updated"] {
		t.Fatalf("kinds seen: %v", kinds)
	}
}

// If PVMBG rolls its schema back, records already captured with confidence_level must keep it:
// the newest record is re-fetched on every poll (overlap window) and would otherwise be overwritten
// by the poorer representation.
func TestSchemaRollbackDoesNotEraseCapturedFields(t *testing.T) {
	fp := newFakePVMBG(t)
	fp.add("r1", "merapi", "Siaga", 1*time.Second, map[string]any{"confidence_level": 0.87})
	h := newHarness(t, nil, fp)
	pollOK(t, h.pvmbg)

	fp.mu.Lock()
	delete(fp.reports[0], "confidence_level") // same record, field gone from the response
	fp.mu.Unlock()
	st := pollOK(t, h.pvmbg)
	if st.Updated != 0 || st.Created != 0 {
		t.Fatalf("a missing field must not change the record: %+v", st)
	}
	got := h.hazard(t, "pvmbg-r1")
	if got.Version != 1 || got.Attributes["confidence_level"] != 0.87 {
		t.Fatalf("captured confidence_level was erased: v%d %v", got.Version, got.Attributes)
	}
	// a NEW record without the field simply does not have it
	fp.add("r2", "merapi", "Siaga", 2*time.Second, nil)
	pollOK(t, h.pvmbg)
	if _, has := h.hazard(t, "pvmbg-r2").Attributes["confidence_level"]; has {
		t.Fatal("new records must not invent the field")
	}
}
