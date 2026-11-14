package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

func bearer(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

var badUTF8 = []byte("{\"report_id\":\"VR-9\",\"name\":\"\xff\"}")

func seedDeadLetters(t *testing.T, repo *memory.Repo) {
	t.Helper()
	at := time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC)
	for _, m := range []ports.Unprocessed{
		{MessageID: "msg-a", Source: "PVMBG", Kind: "pvmbg.volcanic-report", RecordKey: "VR-1", ReceivedAt: at,
			Reason: "schema_incompatible: field 'alert_level': required field is missing", Attempts: 1,
			Status: ports.StatusParked, NextAttemptAt: at, Raw: []byte(`{"report_id":"VR-1","level":"Siaga"}`)},
		{MessageID: "msg-b", Source: "PVMBG", Kind: "pvmbg.volcanic-report", ReceivedAt: at.Add(time.Second),
			Reason: "malformed: invalid UTF-8", Attempts: 1, Status: ports.StatusParked, NextAttemptAt: at, Raw: badUTF8},
		{MessageID: "msg-c", Source: "BMKG", Kind: "bmkg.tsunami-warning", RecordKey: "w-1", ReceivedAt: at.Add(2 * time.Second),
			Reason: "dependency_missing: seismic event e-1 has not been stored yet", Retryable: true, Attempts: 2,
			Status: ports.StatusRetrying, NextAttemptAt: at.Add(time.Minute), Raw: []byte(`{"warning_id":"w-1"}`)},
	} {
		if _, err := repo.AddUnprocessed(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
}

// Failed messages may be inspected only by Internal Ops; the auditor sees
// metadata through its own token; the Client-Facing API's key opens neither.
func TestDeadLettersOnlyForTheirPrincipals(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		path string
		hdr  map[string]string
		want int
	}{
		{"/internal/ops/unprocessed", nil, http.StatusUnauthorized},
		{"/internal/ops/unprocessed", ok(), http.StatusUnauthorized}, // service key header is not a bearer token
		{"/internal/ops/unprocessed", bearer(key), http.StatusForbidden},
		{"/internal/ops/unprocessed", bearer(auditKey), http.StatusForbidden},
		{"/internal/ops/unprocessed", bearer("guess"), http.StatusUnauthorized},
		{"/internal/ops/unprocessed", bearer(opsKey), http.StatusOK},
		{"/internal/audit/unprocessed", bearer(opsKey), http.StatusForbidden},
		{"/internal/audit/unprocessed", bearer(key), http.StatusForbidden},
		{"/internal/audit/unprocessed", nil, http.StatusUnauthorized},
		{"/internal/audit/unprocessed", bearer(auditKey), http.StatusOK},
	} {
		r, body := f.get(t, c.path, c.hdr)
		if r.StatusCode != c.want {
			t.Errorf("%s %v: %d, want %d", c.path, c.hdr, r.StatusCode, c.want)
		}
		if c.want != http.StatusOK {
			e, _ := body["error"].(map[string]any)
			if e["code"] == nil || e["message"] == nil {
				t.Errorf("%s: error body %v", c.path, body)
			}
		}
	}
}

func TestAuditUnprocessedMatchesLampiranA(t *testing.T) {
	f := newFixture(t)
	seedDeadLetters(t, f.repo)
	_, b := f.get(t, "/internal/audit/unprocessed?limit=2", bearer(auditKey))
	its := items(b)
	if len(its) != 2 || b["next_cursor"] == nil {
		t.Fatalf("page 1: %v", b)
	}
	want := map[string]bool{"message_id": true, "source": true, "received_at": true, "reason": true,
		"retryable": true, "attempts": true, "status": true}
	for k := range its[0] {
		if !want[k] {
			t.Errorf("audit item exposes %q", k)
		}
	}
	if its[0]["message_id"] != "msg-a" || its[0]["received_at"] != "2026-11-12T02:40:11Z" || its[0]["retryable"] != false {
		t.Fatalf("item: %v", its[0])
	}
	_, b2 := f.get(t, "/internal/audit/unprocessed?limit=2&cursor="+b["next_cursor"].(string), bearer(auditKey))
	if its := items(b2); len(its) != 1 || its[0]["status"] != "retrying" || b2["next_cursor"] != nil {
		t.Fatalf("page 2: %v", b2)
	}
	_, onlyBMKG := f.get(t, "/internal/audit/unprocessed?source=BMKG", bearer(auditKey))
	if len(items(onlyBMKG)) != 1 {
		t.Fatalf("source filter: %v", onlyBMKG)
	}
	if r, _ := f.get(t, "/internal/audit/unprocessed?limit=5000", bearer(auditKey)); r.StatusCode != 400 {
		t.Fatalf("limit above 1000: %d", r.StatusCode)
	}
}

func TestOpsSeesExactBytesAndRequeues(t *testing.T) {
	f := newFixture(t)
	seedDeadLetters(t, f.repo)
	_, b := f.get(t, "/internal/ops/unprocessed?source=PVMBG", bearer(opsKey))
	its := items(b)
	if len(its) != 2 {
		t.Fatalf("ops list: %v", b)
	}
	raw, _ := base64.StdEncoding.DecodeString(its[1]["raw_base64"].(string))
	if !bytes.Equal(raw, badUTF8) || its[1]["raw_text"] != nil {
		t.Fatalf("raw bytes: %q text=%v", raw, its[1]["raw_text"])
	}
	if its[0]["raw_text"] != `{"report_id":"VR-1","level":"Siaga"}` || its[0]["kind"] != "pvmbg.volcanic-report" {
		t.Fatalf("ops item: %v", its[0])
	}

	req, _ := http.NewRequest(http.MethodPost, f.ts.URL+"/internal/ops/unprocessed/reprocess",
		bytes.NewBufferString(`{"source":"pvmbg","message_ids":["msg-a"]}`))
	req.Header.Set("Authorization", "Bearer "+opsKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var rb map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&rb)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || rb["requeued"] != 1.0 {
		t.Fatalf("reprocess: %d %v", resp.StatusCode, rb)
	}
	m, _ := f.repo.GetUnprocessed(context.Background(), "msg-a")
	if m.Status != ports.StatusRetrying {
		t.Fatalf("requeued message: %+v", m)
	}
	if b, _ := f.repo.GetUnprocessed(context.Background(), "msg-b"); b.Status != ports.StatusParked {
		t.Fatal("a message that was not named must stay parked")
	}
}

// Instances that do not poll serve the leader's stored view; a view nobody
// refreshes any more is reported stale, never "up".
func TestStoreStatusSharesLeaderViewAndExpires(t *testing.T) {
	repo := memory.New()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tr := resilience.NewTracker("PVMBG", resilience.NewBreaker(3, time.Minute))
	tr.Success(time.Millisecond)
	doc, _ := json.Marshal(tr.Snapshot())
	if err := repo.PutSourceStatus(context.Background(), "PVMBG", doc); err != nil {
		t.Fatal(err)
	}
	fresh := (&StoreStatus{Repo: repo, Sources: []string{"BMKG", "PVMBG"}, MaxAge: time.Minute, Log: log}).Statuses(context.Background())
	if fresh[0].State != "unknown" || fresh[1].State != "up" || fresh[1].StaleSince != nil || fresh[1].StatusAgeSec == nil {
		t.Fatalf("fresh view: %+v", fresh)
	}
	time.Sleep(20 * time.Millisecond)
	old := (&StoreStatus{Repo: repo, Sources: []string{"PVMBG"}, MaxAge: 5 * time.Millisecond, Log: log}).Statuses(context.Background())
	if old[0].State != "unknown" || old[0].StaleSince == nil || !old[0].StaleSince.Equal(*old[0].LastSuccessAt) {
		t.Fatalf("expired view must be stale: %+v", old[0])
	}
}
