package satellite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

const keyB = "N21|2026-12-02|6|-1.21125|131.06761"

const startStream = `id: 0
event: detection
data: {"offset":0,"detection":{"satellite":"N20","acq_date":"2026-12-02","acq_time":5,"latitude":-1.21125,"longitude":131.06300,"confidence":"n","frp":10}}

id: 1
event: detection
data: {"offset":1,"detection":{"satellite":"N21","acq_date":"2026-12-02","acq_time":6,"latitude":-1.21125,"longitude":131.06761,"confidence":"l","frp":19}}

id: 2
event: update
data: {"offset":2,"key":"N21|2026-12-02|6|-1.21125|131.06761","changes":{"confidence":"n","frp":20}}

`

const endStream = `id: 2
event: update
data: {"offset":2,"key":"N21|2026-12-02|6|-1.21125|131.06761","changes":{"confidence":"n","frp":20}}

id: 3
event: heartbeat
data: {"offset":3,"virtual_now":"2026-12-02T01:38:48Z"}

id: 4
event: retract
data: {"offset":4,"reason":"false_positive","key":"N21|2026-12-02|6|-1.21125|131.06761"}

`

func TestInclusiveResumePreservesOffsetsAndRetiresOriginalHazard(t *testing.T) {
	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			http.Error(w, "unauthorized", 401)
			return
		}
		switch r.URL.Path {
		case "/v1/session":
			fmt.Fprint(w, `{"group":"test","profile":"practice","latest_offset":4}`)
		case "/v1/stream":
			switch r.URL.Query().Get("from") {
			case "0":
				fmt.Fprint(w, startStream)
			case "2":
				fmt.Fprint(w, endStream)
			default:
				t.Errorf("unexpected offset: %s", r.URL.RawQuery)
			}
		}
	}))
	defer feed.Close()
	repo := memory.New()
	r := Runner{Repo: repo, Store: repo, Client: feed.Client(), URL: feed.URL, Token: "test", Lateness: 24 * time.Hour,
		Provinces: []province{{Name: "Papua Barat Daya", Lat: -0.88, Lon: 131.26}},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx := context.Background()
	if err := r.consume(ctx); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	page, err := repo.List(ctx, ports.ListFilter{})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("page: %+v, %v", page, err)
	}
	fire := page.Items[0]
	if fire.Version != 1 || fire.Severity != "SIAGA" || fire.Attributes["detection_count"] != 2.0 ||
		fire.Attributes["detection_keys"].([]any)[1] != keyB || fire.Attributes["detection_keys"].([]any)[0] != "N20|2026-12-02|5|-1.21125|131.06300" {
		t.Fatalf("fire: %+v", fire)
	}
	// A new runner reloads the committed checkpoint and skips the repeated ID.
	if err := r.consume(ctx); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	h, err := repo.Get(ctx, fire.HazardID)
	if err != nil || !h.Retired || h.Version != 2 {
		t.Fatalf("fire tombstone: %+v, %v", h, err)
	}
	page, err = repo.List(ctx, ports.ListFilter{AuditOrder: true})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("active audit: %+v, %v", page, err)
	}
	doc, err := repo.LoadSatellite(ctx, feed.URL+"|test|practice|")
	var checkpoint state
	if err != nil || json.Unmarshal(doc, &checkpoint) != nil || checkpoint.Applied == nil || *checkpoint.Applied != 4 {
		t.Fatalf("checkpoint did not advance: %v", err)
	}
}

type interruptSave struct {
	ports.Repository
	calls int
}

func (r *interruptSave) Save(ctx context.Context, h domain.HazardEvent, prior int, e ports.OutboxEntry) error {
	r.calls++
	if r.calls == 2 {
		return errors.New("injected processor interruption")
	}
	return r.Repository.Save(ctx, h, prior, e)
}

func TestPendingChangesResumeAfterPartialApply(t *testing.T) {
	repo := memory.New()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := Runner{Repo: &interruptSave{Repository: repo}, Store: repo, Log: log}
	at := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	h := domain.HazardEvent{HazardID: "satelit-fire-test", Version: 1, IngestedAt: at, UpdatedAt: at}
	next := h
	next.Version = 2
	next.Retired = true
	offset := int64(70)
	s := state{Offset: &offset, Pending: []domain.HazardEvent{h, next}}
	if err := r.flush(ctx, "feed", &s, offset); err == nil {
		t.Fatal("expected the processor interruption")
	}
	persisted, err := repo.LoadSatellite(ctx, "feed")
	if err != nil {
		t.Fatal(err)
	}
	var resumed state
	if err := json.Unmarshal(persisted, &resumed); err != nil {
		t.Fatal(err)
	}
	r.Repo = repo
	if err := r.flush(ctx, "feed", &resumed, offset); err != nil {
		t.Fatal(err)
	}
	out, err := repo.PendingOutbox(ctx, 10)
	if err != nil || len(out) != 2 || out[0].Version != 1 || out[1].Version != 2 || *resumed.Applied != offset {
		t.Fatalf("replayed outbox: %+v, %v", out, err)
	}
}

// Point this at a bounded practice capture starting at offset zero. It never
// contacts the external feed and accepts a truncated final SSE message.
func TestPracticeCaptureReplay(t *testing.T) {
	path := os.Getenv("SATELLITE_TEST_CAPTURE")
	if path == "" {
		t.Skip("SATELLITE_TEST_CAPTURE not set")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan event)
	go func() {
		defer close(events)
		_ = readEvents(ctx, file, events)
	}()
	s := newState()
	p := newProcessor(&s, []province{{Name: "fixture"}}, 24*time.Hour, "practice")
	kinds := map[string]int{}
	next := int64(0)
	for e := range events {
		offset, numbered, err := offsetOf(e)
		if err != nil {
			t.Fatal(err)
		}
		if !numbered {
			continue
		}
		if offset != next {
			t.Fatalf("got offset %d, want %d", offset, next)
		}
		if err := p.apply(e); err != nil {
			t.Fatalf("%s at offset %d: %v", e.kind, offset, err)
		}
		kinds[e.kind]++
		next++
	}
	if next == 0 {
		t.Fatal("empty capture")
	}
	t.Logf("complete offsets 0 to %d, frame counts %v, active fires %d, hazard mutations %d, late detections %d",
		next-1, kinds, len(s.Membership), len(s.Pending), s.Late)
}

func TestTransitiveEventTimeClusteringAndLateDetections(t *testing.T) {
	s := newState()
	p := newProcessor(&s, []province{{Name: "test"}}, 24*time.Hour, "practice")
	apply := func(id int, kind, data string) {
		t.Helper()
		if err := p.apply(event{id: strconv.Itoa(id), kind: kind, data: []byte(data), received: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	detect := func(id int, satellite, date, minute, lon string) {
		t.Helper()
		apply(id, "detection", fmt.Sprintf(`{"satellite":%q,"acq_date":%q,"acq_time":%q,"latitude":0,"longitude":%s,"confidence":"n","frp":12}`,
			satellite, date, minute, lon))
	}
	detect(0, "N20", "2026-12-01", "0", "130")
	detect(1, "N21", "2026-12-01", "600", "130.008")
	detect(2, "N20", "2026-12-01", "1200", "130.016")
	if len(s.Membership) != 1 {
		t.Fatalf("the bridge must connect both ends: %v", s.Membership)
	}
	original := s.Pending[0].HazardID
	last := s.Pending[len(s.Pending)-1]
	if last.HazardID != original || last.Attributes["detection_count"] != 3.0 || last.Severity != domain.SeveritySiaga {
		t.Fatalf("chained fire: %+v", last)
	}
	detect(3, "N20", "2026-12-04", "0", "130")
	detect(4, "N20", "2026-12-02", "0", "130")
	detect(5, "N20", "2026-12-02", "0", "130")
	if s.Late != 1 {
		t.Fatalf("distinct late detections: %d", s.Late)
	}
	apply(6, "retract", `{"key":"N21|2026-12-01|600|0|130.008"}`)
	last = s.Pending[len(s.Pending)-1]
	if len(s.Membership) != 0 || !last.Retired || last.HazardID != original {
		t.Fatalf("retracted bridge should retire the original: %+v", last)
	}
}
