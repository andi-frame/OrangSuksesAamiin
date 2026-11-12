package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// rawPVMBG answers /volcanic-reports with whatever body the test sets, byte for byte.
type rawPVMBG struct {
	*httptest.Server
	mu   sync.Mutex
	body string
}

func newRawPVMBG(t *testing.T) *rawPVMBG {
	f := &rawPVMBG{body: "[]"}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_, _ = w.Write([]byte(f.body))
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *rawPVMBG) set(elements ...string) {
	f.mu.Lock()
	f.body = "[" + strings.Join(elements, ",") + "]"
	f.mu.Unlock()
}

func (f *rawPVMBG) setBody(b string) {
	f.mu.Lock()
	f.body = b
	f.mu.Unlock()
}

type p3 struct {
	repo   *memory.Repo
	src    *PVMBGSource
	worker *dlq.Worker
	hours  time.Duration
}

func newP3(t *testing.T, f *rawPVMBG) *p3 {
	log := testLog()
	repo := memory.New()
	q := &dlq.Queue{Repo: repo, Log: log}
	src := &PVMBGSource{
		Client: &sources.PVMBG{HTTP: obs.NewHTTPClient(log, "pvmbg", 2*time.Second), BaseURL: f.URL, Token: pvmbgToken,
			Limits: sources.Limits{MaxBytes: 64 << 10, MaxDepth: 32}},
		Repo: repo, Ingestor: ingest.New(repo, log), DLQ: q, Volcanoes: volcanoes, Overlap: 5 * time.Second, Log: log,
	}
	p := &p3{repo: repo, src: src}
	// every worker pass runs an hour after the previous one: every retry is due
	wq := &dlq.Queue{Repo: repo, Log: log, Now: func() time.Time { return time.Now().Add(p.hours) }}
	p.worker = &dlq.Worker{Queue: wq, Log: log, Handlers: map[string]dlq.Handler{
		KindPVMBGReport:   src.Reprocess,
		KindPVMBGFragment: FragmentHandler(repo, domain.SourcePVMBG),
	}}
	return p
}

func (p *p3) retry(t *testing.T) dlq.Result {
	t.Helper()
	p.hours += time.Hour
	res, err := p.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (p *p3) open(t *testing.T) []ports.Unprocessed {
	t.Helper()
	pg, err := p.repo.ListUnprocessed(context.Background(), ports.UnprocessedFilter{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return pg.Items
}

func report(id, level, at string) string {
	return `{"report_id":"` + id + `","volcano_id":"merapi","alert_level":"` + level +
		`","eruption_count_24h":3,"ash_column_height_m":900.5,"reported_at":"` + at + `"}`
}

// Criterion 3: broken messages neither crash the poller nor stop the others.
func TestMalformedMessagesAreParkedAndOthersProcessed(t *testing.T) {
	f := newRawPVMBG(t)
	p := newP3(t, f)
	deep := `{"report_id":"VR-deep","x":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`
	huge := `{"report_id":"VR-huge","blob":"` + strings.Repeat("A", 200<<10) + `"}`
	f.set(report("VR-1", "Siaga", ts(1*time.Second)), deep, "{\"report_id\":\"VR-utf8\",\"n\":\"\xff\"}", huge,
		`{"report_id":"VR-syntax",}`, `"just a string"`, report("VR-2", "Awas", ts(2*time.Second)))
	st := pollOK(t, p.src)
	if st.Created != 2 || st.Unprocessed != 5 {
		t.Fatalf("stats: %+v", st)
	}
	want := map[string]string{
		"VR-deep": "malformed: too_deep", "VR-utf8": "malformed: invalid_utf8", "VR-huge": "malformed: too_large",
		"VR-syntax": "malformed: invalid_json", "": "schema_incompatible: record is not a JSON object",
	}
	for _, m := range p.open(t) {
		if !strings.HasPrefix(m.Reason, want[m.RecordKey]) || m.Status != ports.StatusParked || m.Retryable {
			t.Errorf("%q: %s (%s)", m.RecordKey, m.Reason, m.Status)
		}
	}
	// re-delivery stores nothing twice
	if again := pollOK(t, p.src); again.Unprocessed != 0 {
		t.Fatalf("re-poll: %+v", again)
	}
	if c, _, _ := p.repo.GetCursor(context.Background(), CursorPVMBG); !c.Equal(t0.Add(2 * time.Second)) {
		t.Fatalf("watermark = %v", c)
	}
}

// A response cut off in transit: the complete elements are stored, the cut one
// waits as retryable and resolves itself once the full record arrives.
func TestTruncatedResponseRecoversWithoutIntervention(t *testing.T) {
	f := newRawPVMBG(t)
	p := newP3(t, f)
	full := "[" + report("VR-1", "Siaga", ts(1*time.Second)) + "," + report("VR-2", "Awas", ts(2*time.Second)) + "]"
	f.setBody(full[:len(full)-40])
	st := pollOK(t, p.src)
	if st.Created != 1 || st.Unprocessed != 1 {
		t.Fatalf("stats: %+v", st)
	}
	frag := p.open(t)
	if len(frag) != 1 || frag[0].Kind != KindPVMBGFragment || frag[0].Status != ports.StatusRetrying ||
		frag[0].RecordKey != "VR-2" || !frag[0].Retryable {
		t.Fatalf("fragment: %+v", frag)
	}
	if res := p.retry(t); res.Retrying != 1 {
		t.Fatalf("not redelivered yet must keep retrying: %+v", res)
	}
	f.setBody(full) // the source answers completely again
	pollOK(t, p.src)
	if res := p.retry(t); res.Resolved != 1 {
		t.Fatalf("fragment not resolved: %+v", res)
	}
	if len(p.open(t)) != 0 {
		t.Fatal("resolved fragment still listed")
	}
}

// Criteria 2 and 4: an incompatible schema change is parked (not guessed); after
// the system is adjusted (schema rules) and the messages are requeued, all of
// them recover, without duplicates, in order per source_ref_id.
func TestIncompatibleChangeRecoversAfterAdjustmentInOrderWithoutDuplicates(t *testing.T) {
	f := newRawPVMBG(t)
	p := newP3(t, f)
	ctx := context.Background()
	epoch := func(off time.Duration) string { return strconv.FormatInt(t0.Add(off).Unix(), 10) }
	renamed := `{"report_id":"VR-1","volcano_id":"merapi","level":"Siaga","reported_at":"` + ts(1*time.Second) + `"}`
	typed := `{"report_id":"VR-2","volcano_id":"merapi","alert_level":"Waspada","reported_at":` + epoch(2*time.Second) + `}`
	moved := `{"report_id":"VR-3","location":{"volcano_code":"merapi"},"alert_level":"Normal","reported_at":"` + ts(3*time.Second) + `"}`
	f.set(renamed, typed, moved)
	st := pollOK(t, p.src)
	if st.Created != 0 || st.Unprocessed != 3 {
		t.Fatalf("stats: %+v", st)
	}
	for _, m := range p.open(t) {
		if m.Status != ports.StatusParked || !strings.HasPrefix(m.Reason, "schema_incompatible") {
			t.Fatalf("not parked as incompatible: %+v", m)
		}
	}
	// parked messages are never retried automatically
	if res := p.retry(t); res != (dlq.Result{}) {
		t.Fatalf("worker touched parked messages: %+v", res)
	}

	// PVMBG sends a newer version of VR-1 while the old one is parked: it must wait behind it
	renamedV2 := `{"report_id":"VR-1","volcano_id":"merapi","level":"Awas","reported_at":"` + ts(1*time.Second) + `"}`
	f.set(renamedV2)
	if st := pollOK(t, p.src); st.Unprocessed != 1 {
		t.Fatalf("newer message of a parked record: %+v", st)
	}

	// the system is adjusted: rules declared, messages requeued by Internal Ops
	p.src.Rules = mapping.SchemaRules{"PVMBG": {
		Rename: map[string]string{"level": "alert_level", "location.volcano_code": "volcano_id"},
		Coerce: map[string]string{"reported_at": mapping.CoerceUnixSeconds},
	}}
	if n, _ := p.repo.RequeueUnprocessed(ctx, "PVMBG", nil, time.Now()); n != 3 {
		t.Fatalf("requeued %d", n)
	}
	if res := p.retry(t); res.Resolved != 4 {
		t.Fatalf("one pass in arrival order must recover all four: %+v", res)
	}
	if open := p.open(t); len(open) != 0 {
		t.Fatalf("still open: %+v", open)
	}
	vr1 := hazardOf(t, p.repo, "pvmbg-VR-1")
	if vr1.Severity != domain.SeverityAwas || vr1.Version != 2 {
		t.Fatalf("VR-1 must end at the newer message (AWAS v2), got %s v%d", vr1.Severity, vr1.Version)
	}
	if h := hazardOf(t, p.repo, "pvmbg-VR-2"); !h.OccurredAt.Equal(t0.Add(2*time.Second)) || h.Severity != domain.SeverityWaspada {
		t.Fatalf("VR-2: %+v", h)
	}
	if h := hazardOf(t, p.repo, "pvmbg-VR-3"); h.AreaName != "Gunung Merapi" {
		t.Fatalf("VR-3: %+v", h)
	}
	if _, has := hazardOf(t, p.repo, "pvmbg-VR-3").Attributes["location"]; has {
		t.Fatal("the moved field must not stay behind as an attribute")
	}
	page, _ := p.repo.List(ctx, ports.ListFilter{})
	if len(page.Items) != 3 {
		t.Fatalf("duplicates: %d hazards", len(page.Items))
	}
	out, _ := p.repo.PendingOutbox(ctx, 100)
	if len(out) != 4 { // VR-1 v1, VR-1 v2, VR-2, VR-3
		t.Fatalf("outbox events: %d", len(out))
	}
	if out[0].HazardID != "pvmbg-VR-1" || out[0].Version != 1 {
		t.Fatalf("VR-1 must be processed before its newer message: %+v", out[0])
	}
}

func hazardOf(t *testing.T, r *memory.Repo, id string) domain.HazardEvent {
	t.Helper()
	h, err := r.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return *h
}
