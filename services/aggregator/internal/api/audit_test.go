package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func TestAuditRoutesMatchAppendixA(t *testing.T) {
	t.Setenv("CANONICAL_STORE_NODE", "node-a")
	repo := memory.New()
	server := (&Server{Repo: repo, AuditToken: "audit-token", OpsToken: "ops-token",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	for _, row := range []struct {
		id, ref string
		source  domain.Source
	}{
		{"bmkg-b", "aa", domain.SourceBMKG}, {"bmkg-a", "Zz", domain.SourceBMKG}, {"pvmbg-z", "aa", domain.SourcePVMBG},
	} {
		h := domain.HazardEvent{HazardID: row.id, Source: row.source, SourceRefID: row.ref,
			Version: 1, OccurredAt: at.Add(-time.Hour), IngestedAt: at, UpdatedAt: at}
		if err := repo.Save(context.Background(), h, 0, ports.OutboxEntry{}); err != nil {
			t.Fatal(err)
		}
	}
	get := func(path, token string, status int) []byte {
		t.Helper()
		r := httptest.NewRequest("GET", "/internal/audit/"+path, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s: got %d body %s, want %d", path, w.Code, w.Body, status)
		}
		return w.Body.Bytes()
	}
	for _, route := range []string{"events", "checksum", "location?hazard_id=bmkg-a"} {
		get(route, "", 401)
		get(route, "unknown", 401)
		get(route, "ops-token", 403)
	}
	get("events?cursor=garbage", "audit-token", 400)
	get("events?since=garbage", "audit-token", 400)
	get("location", "audit-token", 400)
	get("location?hazard_id=missing", "audit-token", 404)
	get("location?hazard_id=bmkg-a", "audit-token", 200)

	var page struct {
		Items       []domain.HazardEvent
		Cursor      *string  `json:"next_cursor"`
		Unavailable []string `json:"unavailable_units"`
	}
	path := "events?limit=1&source=BMKG&since=2026-11-18T12:00:00Z"
	if err := json.Unmarshal(get(path, "audit-token", 200), &page); err != nil ||
		len(page.Items) != 1 || page.Items[0].HazardID != "bmkg-a" || page.Cursor == nil || page.Unavailable == nil {
		t.Fatalf("first page: %+v, %v", page, err)
	}
	if err := json.Unmarshal(get(path+"&cursor="+*page.Cursor, "audit-token", 200), &page); err != nil ||
		len(page.Items) != 1 || page.Items[0].HazardID != "bmkg-b" || page.Cursor != nil {
		t.Fatalf("last page: %+v, %v", page, err)
	}
	var digest struct {
		Checksum string
		Count    int
	}
	if err := json.Unmarshal(get("checksum", "audit-token", 200), &digest); err != nil ||
		digest.Count != 3 || digest.Checksum != fmt.Sprintf("%x", sha256.Sum256([]byte("Zz\naa\naa"))) {
		t.Fatalf("digest: %+v, %v", digest, err)
	}
}
