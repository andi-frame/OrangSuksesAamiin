// Package repotest is the behavioural contract of ports.Repository. Every
// implementation (memory, postgres, ...) must pass Run; the PostgreSQL adapter
// is "done" when its test calls repotest.Run against a real database.
//
//	func TestPostgresRepo(t *testing.T) {
//	    repotest.Run(t, func(t *testing.T) ports.Repository { return newPGRepoOnCleanDB(t) })
//	}
package repotest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Factory returns an EMPTY repository for one sub-test.
type Factory func(t *testing.T) ports.Repository

func f64(v float64) *float64 { return &v }

var base = time.Date(2026, 10, 1, 9, 0, 0, 123456000, time.UTC) // microsecond precision

func hazard(id string, typ domain.HazardType, occurredAt time.Time, version int, attrs map[string]any) domain.HazardEvent {
	src := domain.SourceBMKG
	if typ == domain.HazardVolcanic {
		src = domain.SourcePVMBG
	}
	return domain.HazardEvent{
		HazardID: id, Source: src, SourceRefID: "ref-" + id, HazardType: typ,
		Severity: domain.SeverityWaspada, AreaName: "Area " + id,
		Latitude: f64(-6.1), Longitude: f64(105.8),
		OccurredAt: occurredAt, IngestedAt: occurredAt.Add(time.Second), UpdatedAt: occurredAt.Add(time.Second),
		Attributes: attrs, Version: version,
	}
}

func entry(h domain.HazardEvent) ports.OutboxEntry {
	return ports.OutboxEntry{
		EventID: domain.EventID(h.HazardID, h.Version), HazardID: h.HazardID, Version: h.Version,
		Payload: []byte(fmt.Sprintf(`{"event_id":%q}`, domain.EventID(h.HazardID, h.Version))), CorrelationID: "cid-" + h.HazardID,
	}
}

func mustSave(t *testing.T, r ports.Repository, h domain.HazardEvent, expected int) {
	t.Helper()
	if err := r.Save(context.Background(), h, expected, entry(h)); err != nil {
		t.Fatalf("Save(%s v%d): %v", h.HazardID, h.Version, err)
	}
}

// Run executes the whole contract.
func Run(t *testing.T, newRepo Factory) {
	ctx := context.Background()

	t.Run("save_get_roundtrip_preserves_content_and_types", func(t *testing.T) {
		r := newRepo(t)
		attrs := map[string]any{
			"magnitude": 6.6, "potential_tsunami": true, "affected_zones": []any{"Anyer", "Carita"},
			"nested": map[string]any{"a": []any{1.0, "x", nil}}, "confidence_level": 0.87, "nothing": nil,
		}
		h := hazard("bmkg-1", domain.HazardSeismic, base, 1, attrs)
		mustSave(t, r, h, 0)
		got, err := r.Get(ctx, "bmkg-1")
		if err != nil {
			t.Fatal(err)
		}
		if !domain.SameContent(h, *got) {
			t.Fatalf("content changed across the store:\nwant %+v\ngot  %+v", h, *got)
		}
		if !reflect.DeepEqual(h.Attributes, got.Attributes) {
			t.Fatalf("attributes changed: %#v vs %#v", h.Attributes, got.Attributes)
		}
		if got.Version != 1 || !got.IngestedAt.Equal(h.IngestedAt) || !got.UpdatedAt.Equal(h.UpdatedAt) {
			t.Fatalf("version/timestamps lost: %+v", got)
		}
		if !got.OccurredAt.Equal(base) {
			t.Fatalf("microsecond precision lost: %v vs %v", got.OccurredAt, base)
		}
	})

	t.Run("nullable_coordinates", func(t *testing.T) {
		r := newRepo(t)
		h := hazard("pvmbg-x", domain.HazardVolcanic, base, 1, map[string]any{})
		h.Latitude, h.Longitude = nil, nil
		mustSave(t, r, h, 0)
		got, _ := r.Get(ctx, "pvmbg-x")
		if got.Latitude != nil || got.Longitude != nil {
			t.Fatal("null coordinates must stay null")
		}
	})

	t.Run("get_missing", func(t *testing.T) {
		if _, err := newRepo(t).Get(ctx, "nope"); !errors.Is(err, ports.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("optimistic_concurrency_and_atomic_outbox", func(t *testing.T) {
		r := newRepo(t)
		h1 := hazard("bmkg-1", domain.HazardSeismic, base, 1, map[string]any{"k": 1.0})
		mustSave(t, r, h1, 0)

		// creating again must fail and must not leave an outbox entry behind
		if err := r.Save(ctx, h1, 0, entry(h1)); !errors.Is(err, ports.ErrVersionConflict) {
			t.Fatalf("duplicate create: want conflict, got %v", err)
		}
		// stale update
		h2 := h1
		h2.Version = 2
		if err := r.Save(ctx, h2, 5, entry(h2)); !errors.Is(err, ports.ErrVersionConflict) {
			t.Fatalf("stale expectedVersion: want conflict, got %v", err)
		}
		// version must be expected+1
		h3 := h1
		h3.Version = 7
		if err := r.Save(ctx, h3, 1, entry(h3)); !errors.Is(err, ports.ErrVersionConflict) {
			t.Fatalf("version jump: want conflict, got %v", err)
		}
		pend, _ := r.PendingOutbox(ctx, 100)
		if len(pend) != 1 {
			t.Fatalf("failed saves must not write outbox entries; pending=%d", len(pend))
		}
		mustSave(t, r, h2, 1)
		got, _ := r.Get(ctx, "bmkg-1")
		if got.Version != 2 {
			t.Fatalf("version = %d, want 2", got.Version)
		}
	})

	t.Run("records_from_before_and_after_a_schema_change_coexist", func(t *testing.T) {
		r := newRepo(t)
		old := hazard("pvmbg-old", domain.HazardVolcanic, base, 1, map[string]any{"eruption_count_24h": 3.0})
		neu := hazard("pvmbg-new", domain.HazardVolcanic, base.Add(time.Minute), 1,
			map[string]any{"eruption_count_24h": 4.0, "confidence_level": 0.87})
		mustSave(t, r, old, 0)
		mustSave(t, r, neu, 0) // no migration step between the two saves
		page, err := r.List(ctx, ports.ListFilter{HazardType: domain.HazardVolcanic})
		if err != nil || len(page.Items) != 2 {
			t.Fatalf("list: %v, %d items", err, len(page.Items))
		}
		byID := map[string]domain.HazardEvent{}
		for _, h := range page.Items {
			byID[h.HazardID] = h
		}
		if _, has := byID["pvmbg-old"].Attributes["confidence_level"]; has {
			t.Error("old record must stay without confidence_level")
		}
		if byID["pvmbg-new"].Attributes["confidence_level"] != 0.87 {
			t.Error("new record must carry confidence_level")
		}
	})

	t.Run("outbox_order_limit_and_mark_published", func(t *testing.T) {
		r := newRepo(t)
		for i := 1; i <= 5; i++ {
			mustSave(t, r, hazard(fmt.Sprintf("h%d", i), domain.HazardSeismic, base.Add(time.Duration(i)*time.Second), 1, nil), 0)
		}
		first, _ := r.PendingOutbox(ctx, 2)
		if len(first) != 2 || first[0].EventID != "h1:1" || first[1].EventID != "h2:1" {
			t.Fatalf("oldest-first / limit broken: %+v", first)
		}
		if first[0].CorrelationID != "cid-h1" || first[0].HazardID != "h1" || len(first[0].Payload) == 0 {
			t.Fatalf("outbox entry fields lost: %+v", first[0])
		}
		if err := r.MarkPublished(ctx, []string{"h1:1", "h2:1", "does-not-exist:9"}); err != nil {
			t.Fatal(err)
		}
		rest, _ := r.PendingOutbox(ctx, 100)
		if len(rest) != 3 || rest[0].EventID != "h3:1" {
			t.Fatalf("after MarkPublished: %+v", rest)
		}
		// a new version of an already-published hazard is a new pending entry
		h1v2 := hazard("h1", domain.HazardSeismic, base.Add(time.Second), 2, map[string]any{"changed": true})
		mustSave(t, r, h1v2, 1)
		rest, _ = r.PendingOutbox(ctx, 100)
		if len(rest) != 4 || rest[3].EventID != "h1:2" {
			t.Fatalf("v2 entry not appended: %+v", rest)
		}
	})

	t.Run("polling_cursors", func(t *testing.T) {
		r := newRepo(t)
		if _, ok, err := r.GetCursor(ctx, "bmkg.events"); err != nil || ok {
			t.Fatalf("unset cursor: ok=%v err=%v", ok, err)
		}
		if err := r.SetCursor(ctx, "bmkg.events", base); err != nil {
			t.Fatal(err)
		}
		later := base.Add(time.Hour)
		_ = r.SetCursor(ctx, "bmkg.events", later)
		_ = r.SetCursor(ctx, "pvmbg.reports", base)
		got, ok, _ := r.GetCursor(ctx, "bmkg.events")
		if !ok || !got.Equal(later) {
			t.Fatalf("cursor = %v (%v), want %v", got, ok, later)
		}
		other, _, _ := r.GetCursor(ctx, "pvmbg.reports")
		if !other.Equal(base) {
			t.Fatal("cursors must be independent per name")
		}
	})

	t.Run("list_filters_order_and_pagination", func(t *testing.T) {
		r := newRepo(t)
		// 7 seismic (2 are AWAS) + 3 volcanic; occurred_at strictly increasing with i
		for i := 0; i < 10; i++ {
			typ := domain.HazardSeismic
			if i >= 7 {
				typ = domain.HazardVolcanic
			}
			h := hazard(fmt.Sprintf("h%02d", i), typ, base.Add(time.Duration(i)*time.Minute), 1, nil)
			if i == 2 || i == 4 {
				h.Severity = domain.SeverityAwas
			}
			mustSave(t, r, h, 0)
		}
		// same occurred_at as h05: tie must be broken by hazard_id
		tie := hazard("h05b", domain.HazardSeismic, base.Add(5*time.Minute), 1, nil)
		mustSave(t, r, tie, 0)

		all, _ := r.List(ctx, ports.ListFilter{Limit: 500})
		if len(all.Items) != 11 || all.NextCursor != "" {
			t.Fatalf("all: %d items cursor=%q", len(all.Items), all.NextCursor)
		}
		for i := 1; i < len(all.Items); i++ {
			a, b := all.Items[i-1], all.Items[i]
			if a.OccurredAt.Before(b.OccurredAt) || (a.OccurredAt.Equal(b.OccurredAt) && a.HazardID < b.HazardID) {
				t.Fatalf("not ordered (occurred_at DESC, hazard_id DESC) at %d: %s then %s", i, a.HazardID, b.HazardID)
			}
		}

		var seen []string
		cursor := ""
		for pages := 0; pages < 20; pages++ {
			p, err := r.List(ctx, ports.ListFilter{Limit: 3, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			for _, h := range p.Items {
				seen = append(seen, h.HazardID)
			}
			if p.NextCursor == "" {
				break
			}
			cursor = p.NextCursor
		}
		if len(seen) != 11 {
			t.Fatalf("pagination saw %d items: %v", len(seen), seen)
		}
		uniq := map[string]bool{}
		for i, id := range seen {
			if uniq[id] {
				t.Fatalf("duplicate %s across pages", id)
			}
			uniq[id] = true
			if id != all.Items[i].HazardID {
				t.Fatalf("paginated order differs at %d: %s vs %s", i, id, all.Items[i].HazardID)
			}
		}

		vol, _ := r.List(ctx, ports.ListFilter{HazardType: domain.HazardVolcanic})
		if len(vol.Items) != 3 {
			t.Fatalf("volcanic filter: %d", len(vol.Items))
		}
		awas, _ := r.List(ctx, ports.ListFilter{Severity: domain.SeverityAwas})
		if len(awas.Items) != 2 {
			t.Fatalf("severity filter: %d", len(awas.Items))
		}
		src, _ := r.List(ctx, ports.ListFilter{Source: domain.SourcePVMBG})
		if len(src.Items) != 3 {
			t.Fatalf("source filter: %d", len(src.Items))
		}
		since := base.Add(8 * time.Minute)
		recent, _ := r.List(ctx, ports.ListFilter{Since: &since})
		if len(recent.Items) != 2 {
			t.Fatalf("since filter: %d", len(recent.Items))
		}
		if _, err := r.List(ctx, ports.ListFilter{Cursor: "%%%not-a-cursor"}); err == nil {
			t.Fatal("garbage cursor must be rejected")
		}
	})

	t.Run("ping", func(t *testing.T) {
		if err := newRepo(t).Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
