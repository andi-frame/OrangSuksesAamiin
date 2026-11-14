package repotest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// runFence: a write carrying an older leader term than one the store has seen
// is rejected and leaves nothing behind (M2 Problem 2).
func runFence(t *testing.T, newRepo Factory) {
	ctx := context.Background()
	old := ports.WithFence(ctx, ports.Fence{Name: "ingest", Term: 1, Holder: "pod-a"})
	cur := ports.WithFence(ctx, ports.Fence{Name: "ingest", Term: 2, Holder: "pod-b"})

	t.Run("stale_term_cannot_write_after_newer_term_raised_the_fence", func(t *testing.T) {
		r := newRepo(t)
		h1 := hazard("bmkg-f1", domain.HazardSeismic, base, 1, nil)
		if err := r.Save(old, h1, 0, entry(h1)); err != nil {
			t.Fatalf("term 1 before any newer term: %v", err)
		}
		if err := r.RaiseFence(ctx, ports.Fence{Name: "ingest", Term: 2, Holder: "pod-b"}); err != nil {
			t.Fatal(err)
		}
		h2 := hazard("bmkg-f2", domain.HazardSeismic, base, 1, nil)
		if err := r.Save(old, h2, 0, entry(h2)); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("stale Save: want ErrFenced, got %v", err)
		}
		if _, err := r.Get(ctx, "bmkg-f2"); !errors.Is(err, ports.ErrNotFound) {
			t.Fatalf("fenced Save must write nothing, Get: %v", err)
		}
		pending, _ := r.PendingOutbox(ctx, 10)
		if len(pending) != 1 {
			t.Fatalf("fenced Save must not add an outbox entry: %d entries", len(pending))
		}
		if err := r.SetCursor(old, "c", base); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("stale SetCursor: want ErrFenced, got %v", err)
		}
		if _, ok, _ := r.GetCursor(ctx, "c"); ok {
			t.Fatal("fenced SetCursor must write nothing")
		}
		if _, err := r.AddUnprocessed(old, unprocessed("m-1", "k", ports.StatusParked)); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("stale AddUnprocessed: want ErrFenced, got %v", err)
		}
		if err := r.Save(cur, h2, 0, entry(h2)); err != nil {
			t.Fatalf("current term: %v", err)
		}
		if err := r.RaiseFence(ctx, ports.Fence{Name: "ingest", Term: 1}); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("lowering the fence must fail, got %v", err)
		}
		// another lease name is independent
		other := ports.WithFence(ctx, ports.Fence{Name: "satellite", Term: 1})
		if err := r.SetCursor(other, "s", base); err != nil {
			t.Fatalf("independent lease: %v", err)
		}
		// writes without a fence are not checked
		if err := r.SetCursor(ctx, "plain", base); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a_write_with_a_newer_term_raises_the_fence_itself", func(t *testing.T) {
		r := newRepo(t)
		if err := r.SetCursor(cur, "c", base); err != nil {
			t.Fatal(err)
		}
		if err := r.SetCursor(old, "c", base.Add(time.Hour)); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("want ErrFenced, got %v", err)
		}
		got, _, _ := r.GetCursor(ctx, "c")
		if !got.Equal(base) {
			t.Fatalf("cursor overwritten by a stale term: %v", got)
		}
	})
}

func unprocessed(id, key, status string) ports.Unprocessed {
	return ports.Unprocessed{
		MessageID: id, Source: "PVMBG", Kind: "pvmbg.volcanic-report", RecordKey: key,
		ReceivedAt: base, Reason: "schema_incompatible: alert_level: required field is missing",
		Retryable: status == ports.StatusRetrying, Attempts: 1, Status: status, NextAttemptAt: base,
		CorrelationID: "cid-" + id, Raw: []byte("{\"report_id\":\"" + key + "\",\"x\":\"\xff\"}"),
	}
}

// runUnprocessed: the dead-letter store keeps the exact bytes, never
// duplicates a re-delivered message and lists in arrival order (M2 Problem 3).
func runUnprocessed(t *testing.T, newRepo Factory) {
	ctx := context.Background()

	t.Run("dead_letter_roundtrip_dedup_and_raw_bytes", func(t *testing.T) {
		r := newRepo(t)
		m := unprocessed("m-1", "VR-1", ports.StatusParked)
		created, err := r.AddUnprocessed(ctx, m)
		if err != nil || !created {
			t.Fatalf("first add: created=%v err=%v", created, err)
		}
		again := m
		again.Reason = "something else"
		if created, err := r.AddUnprocessed(ctx, again); err != nil || created {
			t.Fatalf("re-delivered message must not be added twice: created=%v err=%v", created, err)
		}
		got, err := r.GetUnprocessed(ctx, "m-1")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Raw, m.Raw) || got.RawSize != int64(len(m.Raw)) {
			t.Fatalf("raw bytes changed: %q (%d)", got.Raw, got.RawSize)
		}
		if got.Reason != m.Reason || got.Status != ports.StatusParked || got.Source != "PVMBG" ||
			got.Kind != m.Kind || got.RecordKey != "VR-1" || !got.ReceivedAt.Equal(base) || got.Seq == 0 {
			t.Fatalf("fields lost: %+v", got)
		}
		if _, err := r.GetUnprocessed(ctx, "nope"); !errors.Is(err, ports.ErrNotFound) {
			t.Fatalf("missing id: %v", err)
		}
		if err := r.UpdateUnprocessed(ctx, ports.Unprocessed{MessageID: "nope"}); !errors.Is(err, ports.ErrNotFound) {
			t.Fatalf("update missing id: %v", err)
		}
	})

	t.Run("dead_letter_list_filters_order_paging_requeue_counts", func(t *testing.T) {
		r := newRepo(t)
		for i, s := range []struct{ id, key, status string }{
			{"m-a", "VR-1", ports.StatusParked}, {"m-b", "VR-2", ports.StatusRetrying},
			{"m-c", "VR-1", ports.StatusRetrying}, {"m-d", "VR-3", ports.StatusParked},
		} {
			m := unprocessed(s.id, s.key, s.status)
			m.NextAttemptAt = base.Add(time.Duration(i) * time.Minute)
			if _, err := r.AddUnprocessed(ctx, m); err != nil {
				t.Fatal(err)
			}
		}
		bmkg := unprocessed("m-e", "ev-1", ports.StatusParked)
		bmkg.Source = "BMKG"
		if _, err := r.AddUnprocessed(ctx, bmkg); err != nil {
			t.Fatal(err)
		}

		var ids []string
		cursor := ""
		for {
			p, err := r.ListUnprocessed(ctx, ports.UnprocessedFilter{Source: "PVMBG", Limit: 3, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range p.Items {
				ids = append(ids, m.MessageID)
			}
			if p.NextCursor == "" {
				break
			}
			cursor = p.NextCursor
		}
		if want := []string{"m-a", "m-b", "m-c", "m-d"}; !slices.Equal(ids, want) {
			t.Fatalf("arrival order / paging: %v, want %v", ids, want)
		}
		key, _ := r.ListUnprocessed(ctx, ports.UnprocessedFilter{Source: "PVMBG", RecordKey: "VR-1", Limit: 1})
		if len(key.Items) != 1 || key.Items[0].MessageID != "m-a" {
			t.Fatalf("oldest open message of a key: %+v", key.Items)
		}
		due := base.Add(90 * time.Second)
		d, _ := r.ListUnprocessed(ctx, ports.UnprocessedFilter{DueBefore: &due})
		if len(d.Items) != 1 || d.Items[0].MessageID != "m-b" {
			t.Fatalf("due retrying: %+v", d.Items)
		}

		resolved := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
		c, _ := r.GetUnprocessed(ctx, "m-c")
		c.Status, c.ResolvedAt, c.Attempts = ports.StatusResolved, &resolved, 2
		if err := r.UpdateUnprocessed(ctx, *c); err != nil {
			t.Fatal(err)
		}
		open, _ := r.ListUnprocessed(ctx, ports.UnprocessedFilter{Source: "PVMBG"})
		if len(open.Items) != 3 {
			t.Fatalf("resolved messages must not be listed by default: %d", len(open.Items))
		}
		got, _ := r.GetUnprocessed(ctx, "m-c")
		if got.ResolvedAt == nil || !got.ResolvedAt.Equal(resolved) || got.Attempts != 2 {
			t.Fatalf("update lost: %+v", got)
		}

		n, err := r.RequeueUnprocessed(ctx, "PVMBG", nil, base.Add(time.Hour))
		if err != nil || n != 2 {
			t.Fatalf("requeue parked PVMBG: n=%d err=%v", n, err)
		}
		a, _ := r.GetUnprocessed(ctx, "m-a")
		if a.Status != ports.StatusRetrying || !a.NextAttemptAt.Equal(base.Add(time.Hour)) {
			t.Fatalf("requeued: %+v", a)
		}
		if e, _ := r.GetUnprocessed(ctx, "m-e"); e.Status != ports.StatusParked {
			t.Fatal("requeue of one source touched another")
		}
		if n, _ := r.RequeueUnprocessed(ctx, "", []string{"m-e"}, base); n != 1 {
			t.Fatalf("requeue by id: %d", n)
		}

		counts, err := r.CountUnprocessed(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts["PVMBG"][ports.StatusRetrying] != 3 || counts["BMKG"][ports.StatusRetrying] != 1 ||
			counts["PVMBG"][ports.StatusParked] != 0 {
			t.Fatalf("counts: %v", counts)
		}
		if _, err := r.ListUnprocessed(ctx, ports.UnprocessedFilter{Cursor: "garbage"}); err == nil {
			t.Fatal("garbage cursor must be rejected")
		}
	})

	t.Run("source_status_is_shared_and_fenced", func(t *testing.T) {
		r := newRepo(t)
		cur := ports.WithFence(ctx, ports.Fence{Name: "ingest", Term: 3})
		if err := r.PutSourceStatus(cur, "PVMBG", []byte(`{"state":"up"}`)); err != nil {
			t.Fatal(err)
		}
		if err := r.PutSourceStatus(cur, "PVMBG", []byte(`{"state":"down"}`)); err != nil {
			t.Fatal(err)
		}
		stale := ports.WithFence(ctx, ports.Fence{Name: "ingest", Term: 2})
		if err := r.PutSourceStatus(stale, "PVMBG", []byte(`{"state":"up"}`)); !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("stale term: %v", err)
		}
		got, err := r.ListSourceStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Source != "PVMBG" || got[0].Age < 0 || got[0].Age > time.Minute {
			t.Fatalf("status: %+v", got)
		}
		var doc map[string]any
		if err := json.Unmarshal(got[0].Doc, &doc); err != nil || doc["state"] != "down" {
			t.Fatalf("doc: %s (%v)", got[0].Doc, err)
		}
	})
}
