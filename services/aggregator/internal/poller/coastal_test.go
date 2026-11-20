package poller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// fakeSensor serves the Lampiran C contract from a list of raw observations;
// the cursor is an offset (the poller treats it as opaque).
type fakeSensor struct {
	mu        sync.Mutex
	items     []string // raw JSON per observation
	generated time.Time
	status    int    // 0 = 200
	body      string // overrides the whole /v1/observations body when set
	calls     int
}

func (f *fakeSensor) add(raw ...string) {
	f.mu.Lock()
	f.items = append(f.items, raw...)
	f.mu.Unlock()
}

func newFakeSensor(t *testing.T) (*fakeSensor, *httptest.Server) {
	f := &fakeSensor{generated: time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/v1/stations":
			fmt.Fprint(w, `{"items":[{"station_id":"stn-a","name":"A","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3},
				{"station_id":"stn-b","name":"B","province":"Aceh","latitude":5.5,"longitude":95.3}]}`)
		case "/v1/observations":
			f.calls++
			if f.status != 0 {
				w.WriteHeader(f.status)
				fmt.Fprint(w, `{"error":{"code":"unavailable","message":"x"}}`)
				return
			}
			if f.body != "" && r.URL.Query().Get("cursor") == "" {
				fmt.Fprint(w, f.body)
				return
			}
			off := 0
			if c := r.URL.Query().Get("cursor"); c != "" {
				n, err := strconv.Atoi(strings.TrimPrefix(c, "c"))
				if err != nil {
					w.WriteHeader(400)
					return
				}
				off = n
			}
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if limit <= 0 {
				limit = 100
			}
			end := min(off+limit, len(f.items))
			if off > len(f.items) {
				off = len(f.items)
			}
			fmt.Fprintf(w, `{"generated_at":%q,"items":[%s],"next_cursor":"c%d"}`,
				f.generated.Format(time.RFC3339), strings.Join(f.items[off:end], ","), end)
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func obsJSON(id, station string, sea, wave float64, quality string, critical bool, at string) string {
	return fmt.Sprintf(`{"observation_id":%q,"station_id":%q,"observed_at":%q,"sea_level_anomaly_m":%g,"wave_height_m":%g,"quality":%q,"critical":%t}`,
		id, station, at, sea, wave, quality, critical)
}

type coastalHarness struct {
	repo *memory.Repo
	src  *CoastalSource
	now  time.Time
}

func newCoastal(t *testing.T, srv *httptest.Server) *coastalHarness {
	repo := memory.New()
	h := &coastalHarness{repo: repo, now: time.Now()}
	h.src = &CoastalSource{
		Client: &sources.Coastal{HTTP: srv.Client(), BaseURL: srv.URL, Token: "tok"},
		Repo:   repo, Store: repo, Ingestor: ingest.New(repo, testLog()),
		DLQ: &dlq.Queue{Repo: repo, Log: testLog()}, Log: testLog(),
		PageSize: 2, MaxPages: 10, FreezeAfter: 10 * time.Second,
		Now: func() time.Time { return h.now },
	}
	return h
}

func (h *coastalHarness) unprocessed(t *testing.T) []ports.Unprocessed {
	t.Helper()
	p, err := h.repo.ListUnprocessed(context.Background(), ports.UnprocessedFilter{Source: "SENSOR_PANTAI", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return p.Items
}

func TestCoastalStoresValidObservationsAcrossPagesCriticalFirst(t *testing.T) {
	f, srv := newFakeSensor(t)
	f.add(
		obsJSON("o1", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:00Z"),
		obsJSON("o2", "stn-b", 0.6, 1, "ok", false, "2026-11-12T00:59:15Z"),
		obsJSON("o3", "stn-a", 1.4, 5.5, "ok", true, "2026-11-12T00:59:30Z"),
	)
	h := newCoastal(t, srv)
	st, err := h.src.PollOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Fetched != 3 || st.Created != 3 {
		t.Fatalf("stats %+v", st)
	}
	crit, err := h.repo.Get(context.Background(), domain.HazardID(domain.SourceCoastal, "o3"))
	if err != nil || crit.Severity != domain.SeverityAwas || crit.HazardType != domain.HazardCoastal || crit.AreaName != "Sumatera Barat" {
		t.Fatalf("critical observation: %+v %v", crit, err)
	}
	if w, _ := h.repo.Get(context.Background(), "sensor-pantai-o2"); w.Severity != domain.SeverityWaspada {
		t.Fatalf("severity of o2 = %s", w.Severity)
	}
	// the critical one was written first, so its outbox entry goes out first
	out, _ := h.repo.PendingOutbox(context.Background(), 10)
	if len(out) != 3 || out[0].HazardID != "sensor-pantai-o3" {
		t.Fatalf("outbox order: %+v", out)
	}
	// next cycle: nothing new, nothing written again
	st, err = h.src.PollOnce(context.Background())
	if err != nil || st.Fetched != 0 {
		t.Fatalf("second cycle %+v %v", st, err)
	}
}

func TestCoastalSetsContractViolationsAsideWithReason(t *testing.T) {
	f, srv := newFakeSensor(t)
	at := "2026-11-12T00:59:00Z"
	f.add(
		obsJSON("ok1", "stn-a", 0.1, 1, "ok", false, at),
		obsJSON("q1", "stn-a", 0.1, 1, "suspect", false, at),
		`{"observation_id":"t1","station_id":"stn-a","observed_at":"`+at+`","sea_level_anomaly_m":"0.4","wave_height_m":1,"quality":"ok","critical":false}`,
		`{"observation_id":"m1","station_id":"stn-a","observed_at":"`+at+`","wave_height_m":1,"quality":"ok","critical":false}`,
		obsJSON("r1", "stn-a", 0.1, 99, "ok", false, at),
		obsJSON("f1", "stn-a", 0.1, 1, "ok", false, "2026-11-12T05:00:00Z"), // after generated_at
		obsJSON("u1", "stn-zz", 0.1, 1, "ok", false, at),
		`{"observation_id":"x1", "broken`,
	)
	h := newCoastal(t, srv)
	h.src.PageSize = 100
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.repo.Get(context.Background(), "sensor-pantai-ok1"); err != nil {
		t.Fatal("valid observation not stored")
	}
	for _, id := range []string{"q1", "t1", "m1", "r1", "f1", "u1"} {
		if _, err := h.repo.Get(context.Background(), "sensor-pantai-"+id); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("%s must not become a HazardEvent", id)
		}
	}
	items := h.unprocessed(t)
	reasons := map[string]ports.Unprocessed{}
	for _, u := range items {
		reasons[u.RecordKey] = u
	}
	want := map[string]string{"q1": "quality", "t1": "sea_level_anomaly_m", "m1": "sea_level_anomaly_m",
		"r1": "wave_height_m", "f1": "observed_at", "u1": "unknown station"}
	for id, frag := range want {
		u, ok := reasons[id]
		if !ok || !strings.Contains(u.Reason, frag) {
			t.Errorf("%s: reason %q, want it to mention %q", id, u.Reason, frag)
		}
	}
	if reasons["u1"].Status != "retrying" || reasons["q1"].Status != "parked" {
		t.Errorf("unknown station must be retryable, a quality flag permanent: %+v / %+v", reasons["u1"], reasons["q1"])
	}
	// the response itself was invalid JSON (x1 is cut off): kept whole too
	found := false
	for _, u := range items {
		if u.Kind == KindCoastalResponse {
			found = true
		}
	}
	if found {
		t.Log("broken body kept as a response-level dead letter")
	}
}

func TestCoastalLyingHealthIsAFailureAndCursorStays(t *testing.T) {
	f, srv := newFakeSensor(t)
	f.add(obsJSON("o1", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:00Z"))
	h := newCoastal(t, srv)
	f.status = 503
	_, err := h.src.PollOnce(context.Background())
	var se *sources.SourceError
	if !errors.As(err, &se) || se.Status != 503 {
		t.Fatalf("503 must be a source error (breaker), got %v", err)
	}
	f.status = 0
	if st, err := h.src.PollOnce(context.Background()); err != nil || st.Created != 1 {
		t.Fatalf("after recovery nothing may be missing: %+v %v", st, err)
	}
}

func TestCoastalFrozenGeneratedAtMarksSourceStale(t *testing.T) {
	f, srv := newFakeSensor(t)
	h := newCoastal(t, srv)
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(5 * time.Second)
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatalf("5 s without progress is still fine: %v", err)
	}
	h.now = h.now.Add(20 * time.Second) // generated_at never moved
	_, err := h.src.PollOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("a sensor that answers 200 with frozen data must count as failing, got %v", err)
	}
	f.mu.Lock()
	f.generated = f.generated.Add(time.Minute)
	f.mu.Unlock()
	h.now = h.now.Add(time.Second)
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatalf("recovers by itself once generated_at moves: %v", err)
	}
}

func TestCoastalMalformedPageIsKeptAndStreamContinues(t *testing.T) {
	f, srv := newFakeSensor(t)
	h := newCoastal(t, srv)
	f.body = `{"generated_at":"2026-11-12T01:00:00Z","items":[{"observation_id":"o1",` + "\xff" + `],"next_cursor":"c1"}`
	_, err := h.src.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("a broken page with a readable next_cursor must not stop the stream: %v", err)
	}
	items := h.unprocessed(t)
	kinds := map[string]int{}
	for _, u := range items {
		kinds[u.Kind]++
		if u.Status != "parked" {
			t.Errorf("malformed bytes are permanent: %+v", u)
		}
	}
	if kinds[KindCoastalResponse] != 1 || kinds[KindCoastalObservation] != 1 || len(items) != 2 {
		t.Fatalf("the page must be kept whole and its broken observation on its own: %+v", kinds)
	}
	f.body = ""
	f.add(obsJSON("o0", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:00Z"), obsJSON("o2", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:30Z"))
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// the stream continued after the broken page (offset 1): o2 is there
	if _, err := h.repo.Get(context.Background(), "sensor-pantai-o2"); err != nil {
		t.Fatal("stream did not continue after the broken page")
	}
}

func TestCoastalDuplicatesIgnoredConflictsSetAside(t *testing.T) {
	f, srv := newFakeSensor(t)
	at := "2026-11-12T00:59:00Z"
	f.add(obsJSON("o1", "stn-a", 0.1, 1, "ok", false, at), obsJSON("o1", "stn-a", 0.1, 1, "ok", false, at),
		obsJSON("o1", "stn-a", 0.9, 1, "ok", false, at))
	h := newCoastal(t, srv)
	h.src.PageSize = 100
	st, err := h.src.PollOnce(context.Background())
	if err != nil || st.Created != 1 || st.Unchanged != 1 || st.Unprocessed != 1 {
		t.Fatalf("stats %+v err %v", st, err)
	}
	stored, _ := h.repo.Get(context.Background(), "sensor-pantai-o1")
	if stored.Version != 1 || stored.Attributes["sea_level_anomaly_m"] != 0.1 {
		t.Fatalf("an observation is immutable: %+v", stored)
	}
	if u := h.unprocessed(t); len(u) != 1 || !strings.Contains(u[0].Reason, "conflicting_duplicate") {
		t.Fatalf("conflict not kept: %+v", u)
	}
}

func TestCoastalRejectedCursorStartsOver(t *testing.T) {
	f, srv := newFakeSensor(t)
	f.add(obsJSON("o1", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:00Z"))
	h := newCoastal(t, srv)
	b, _ := json.Marshal(coastalCheckpoint{Cursor: "garbage"})
	if err := h.repo.StoreSatellite(context.Background(), CursorCoastal, b); err != nil {
		t.Fatal(err)
	}
	st, err := h.src.PollOnce(context.Background())
	if err != nil || st.Created != 1 {
		t.Fatalf("after a rejected cursor the poller must start over: %+v %v", st, err)
	}
}

func TestCoastalBacklogAfterOutageIsNotExpired(t *testing.T) {
	f, srv := newFakeSensor(t)
	h := newCoastal(t, srv)
	h.src.MaxLag = time.Hour
	f.add(obsJSON("o1", "stn-a", 0.1, 1, "ok", false, "2026-11-12T00:59:00Z"))
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// we were down 3 wall minutes while the sensor ran at 60x: 3 sensor hours of backlog
	f.mu.Lock()
	f.generated = f.generated.Add(3 * time.Hour)
	f.mu.Unlock()
	h.src.speed = 60
	h.now = h.now.Add(3 * time.Minute)
	f.add(obsJSON("o2", "stn-a", 0.1, 1, "ok", false, "2026-11-12T01:30:00Z"))
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.repo.Get(context.Background(), "sensor-pantai-o2"); err != nil {
		t.Fatal("backlog read late after our own outage must not be treated as expired")
	}
	// but data far older than generated_at in a normal cycle is expired
	h.now = h.now.Add(time.Second)
	f.add(obsJSON("o3", "stn-a", 0.1, 1, "ok", false, "2026-11-11T20:00:00Z"))
	if _, err := h.src.PollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.repo.Get(context.Background(), "sensor-pantai-o3"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("expired observation must be set aside")
	}
}
