package ingest

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func log() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func lat(v float64) *float64 { return &v }

func hz(id string, sev domain.Severity, attrs map[string]any) *domain.HazardEvent {
	return &domain.HazardEvent{
		HazardID: id, Source: domain.SourceBMKG, SourceRefID: "ref", HazardType: domain.HazardSeismic,
		Severity: sev, AreaName: "A", Latitude: lat(1), Longitude: lat(2),
		OccurredAt: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC), Attributes: attrs,
	}
}

func TestUpsertCreatedUnchangedUpdated(t *testing.T) {
	repo := memory.New()
	clock := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	ing := &Ingestor{Repo: repo, Log: log(), Now: func() time.Time { clock = clock.Add(time.Second); return clock }}
	ctx := obs.WithCorrelationID(context.Background(), "cid-1")

	out, err := ing.Upsert(ctx, hz("bmkg-1", domain.SeverityWaspada, map[string]any{"m": 5.0}), nil)
	if err != nil || out != Created {
		t.Fatalf("first: %v %v", out, err)
	}
	first, _ := repo.Get(ctx, "bmkg-1")
	if first.Version != 1 || first.IngestedAt.IsZero() {
		t.Fatalf("first: %+v", first)
	}

	out, _ = ing.Upsert(ctx, hz("bmkg-1", domain.SeverityWaspada, map[string]any{"m": 5.0}), nil)
	if out != Unchanged {
		t.Fatalf("same content must be unchanged, got %v", out)
	}
	if p, _ := repo.PendingOutbox(ctx, 10); len(p) != 1 {
		t.Fatalf("unchanged must not emit an event: %d", len(p))
	}

	out, _ = ing.Upsert(ctx, hz("bmkg-1", domain.SeveritySiaga, map[string]any{"m": 5.0}), nil)
	if out != Updated {
		t.Fatalf("changed content must be an update, got %v", out)
	}
	second, _ := repo.Get(ctx, "bmkg-1")
	if second.Version != 2 || !second.IngestedAt.Equal(first.IngestedAt) || !second.UpdatedAt.After(first.UpdatedAt) {
		t.Fatalf("update must bump version, keep ingested_at, advance updated_at: %+v", second)
	}

	p, _ := repo.PendingOutbox(ctx, 10)
	if len(p) != 2 || p[1].EventID != "bmkg-1:2" || p[1].CorrelationID != "cid-1" || p[1].HazardID != "bmkg-1" {
		t.Fatalf("outbox: %+v", p)
	}
	var env domain.Envelope
	if err := json.Unmarshal(p[1].Payload, &env); err != nil {
		t.Fatal(err)
	}
	if env.EventID != "bmkg-1:2" || env.Kind != domain.KindUpdated || env.Hazard.Version != 2 || env.SchemaName != domain.SchemaName {
		t.Fatalf("envelope: %+v", env)
	}
}

func TestPrepareHookSeesExistingHazard(t *testing.T) {
	repo := memory.New()
	ing := New(repo, log())
	ctx := context.Background()
	_, _ = ing.Upsert(ctx, hz("h", domain.SeveritySiaga, nil), nil)
	var saw *domain.HazardEvent
	_, _ = ing.Upsert(ctx, hz("h", domain.SeverityNormal, nil), func(existing, fresh *domain.HazardEvent) {
		saw = existing
		fresh.Severity = existing.Severity // carry the stored state over
	})
	if saw == nil || saw.Severity != domain.SeveritySiaga {
		t.Fatalf("hook did not receive the stored hazard: %+v", saw)
	}
	if got, _ := repo.Get(ctx, "h"); got.Version != 1 {
		t.Fatalf("carrying state over must make the write a no-op, version=%d", got.Version)
	}
}

// conflictOnce simulates a second writer sneaking in between our Get and Save.
type conflictOnce struct {
	ports.Repository
	done  bool
	rival func()
}

func (c *conflictOnce) Save(ctx context.Context, h domain.HazardEvent, exp int, o ports.OutboxEntry) error {
	if !c.done {
		c.done = true
		c.rival()
	}
	return c.Repository.Save(ctx, h, exp, o)
}

func TestVersionConflictIsRetried(t *testing.T) {
	inner := memory.New()
	ctx := context.Background()
	repo := &conflictOnce{Repository: inner}
	repo.rival = func() {
		rival := *hz("h", domain.SeverityWaspada, map[string]any{"by": "rival"})
		rival.Version = 1
		_ = inner.Save(ctx, rival, 0, ports.OutboxEntry{EventID: "h:1", HazardID: "h", Version: 1})
	}
	ing := New(repo, log())
	out, err := ing.Upsert(ctx, hz("h", domain.SeveritySiaga, map[string]any{"by": "us"}), nil)
	if err != nil || out != Updated {
		t.Fatalf("conflict must be resolved by re-reading: %v %v", out, err)
	}
	got, _ := inner.Get(ctx, "h")
	if got.Version != 2 || got.Attributes["by"] != "us" {
		t.Fatalf("final state: %+v", got)
	}
}

func TestQuarantineBoundsAndCounts(t *testing.T) {
	q := NewQuarantine(log(), 3)
	for i := 0; i < 5; i++ {
		q.Add(context.Background(), "PVMBG", string(rune('a'+i)), "alert_level", "bad", map[string]any{"i": i})
	}
	if q.Total()["PVMBG"] != 5 {
		t.Fatalf("total: %v", q.Total())
	}
	rec := q.Recent()
	if len(rec) != 3 || rec[0].RecordID != "e" {
		t.Fatalf("ring buffer should keep the 3 newest, newest first: %+v", rec)
	}
}

func TestQuarantineDeduplicatesRefetchedRecords(t *testing.T) {
	q := NewQuarantine(log(), 10)
	ctx := context.Background()
	if !q.Add(ctx, "PVMBG", "r1", "alert_level", "unknown enum", nil) {
		t.Fatal("first rejection must be reported as new")
	}
	// the overlap window delivers the same record again on the next cycles
	for i := 0; i < 3; i++ {
		if q.Add(ctx, "PVMBG", "r1", "alert_level", "unknown enum", nil) {
			t.Fatal("a re-fetched record must not be counted again")
		}
	}
	if q.Total()["PVMBG"] != 1 || len(q.Recent()) != 1 {
		t.Fatalf("total=%v recent=%d", q.Total(), len(q.Recent()))
	}
	// a different reason for the same record, or another record, IS new
	if !q.Add(ctx, "PVMBG", "r1", "volcano_id", "missing", nil) || !q.Add(ctx, "PVMBG", "r2", "alert_level", "unknown enum", nil) {
		t.Fatal("distinct rejections must be recorded")
	}
	// records without an id are identified by their content
	if !q.Add(ctx, "BMKG", "", "", "not an object", "junk-a") || !q.Add(ctx, "BMKG", "", "", "not an object", "junk-b") {
		t.Fatal("different id-less records are different")
	}
	if q.Add(ctx, "BMKG", "", "", "not an object", "junk-a") {
		t.Fatal("same id-less record must be de-duplicated")
	}
}

// A source that stops sending a field (schema rolled back, partial response, ...)
// must not erase what was already captured for that record: attributes are merged
// additively on update. Otherwise a rollback would silently destroy confidence_level
// on every record still inside the polling overlap window.
func TestAttributesMergeAdditivelyOnUpdate(t *testing.T) {
	repo := memory.New()
	ing := New(repo, log())
	ctx := context.Background()

	_, _ = ing.Upsert(ctx, hz("pvmbg-1", domain.SeveritySiaga, map[string]any{"eruption_count_24h": 4.0, "confidence_level": 0.87}), nil)

	// same record re-polled after the schema was rolled back: field missing
	out, err := ing.Upsert(ctx, hz("pvmbg-1", domain.SeveritySiaga, map[string]any{"eruption_count_24h": 4.0}), nil)
	if err != nil || out != Unchanged {
		t.Fatalf("a missing field must not count as a change: %v %v", out, err)
	}
	got, _ := repo.Get(ctx, "pvmbg-1")
	if got.Version != 1 || got.Attributes["confidence_level"] != 0.87 {
		t.Fatalf("confidence_level was lost: v%d %v", got.Version, got.Attributes)
	}

	// a real change of a value (or a new field) still updates the record
	out, _ = ing.Upsert(ctx, hz("pvmbg-1", domain.SeveritySiaga, map[string]any{"eruption_count_24h": 9.0, "new_field": "x"}), nil)
	if out != Updated {
		t.Fatalf("changed value must update, got %v", out)
	}
	got, _ = repo.Get(ctx, "pvmbg-1")
	if got.Version != 2 || got.Attributes["eruption_count_24h"] != 9.0 || got.Attributes["new_field"] != "x" || got.Attributes["confidence_level"] != 0.87 {
		t.Fatalf("merged attributes wrong: v%d %v", got.Version, got.Attributes)
	}
}
