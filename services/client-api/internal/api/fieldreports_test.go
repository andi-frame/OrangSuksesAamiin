package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/aggregator"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/reports"
)

// fakeBroker records what was made durable; fail simulates a broker outage.
type fakeBroker struct {
	mu   sync.Mutex
	recs []struct {
		key string
		val []byte
	}
	fail bool
}

func (b *fakeBroker) Publish(_ context.Context, key string, val []byte, _ map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail {
		return reports.ErrUnavailable
	}
	b.recs = append(b.recs, struct {
		key string
		val []byte
	}{key, val})
	return nil
}

func reportServer(t *testing.T, b reports.Publisher, processor string) *httptest.Server {
	t.Helper()
	log := obs.NewLogger("client-api-test")
	srv := &api.Server{Agg: aggregator.New("http://127.0.0.1:1", "k", log, time.Second), Log: log,
		Reports: b, ReportsURL: processor}
	ts := httptest.NewServer(srv.Handler(secret))
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, ts *httptest.Server, tok, key, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/field-reports", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

const validReport = `{"area_name":"Padang","description":"air laut surut tiba-tiba","hazard_type":"coastal","latitude":-0.95,"longitude":100.35}`

func TestFieldReportIsAcceptedOnlyOnceDurable(t *testing.T) {
	b := &fakeBroker{}
	ts := reportServer(t, b, "")
	code, body := post(t, ts, token(t, "lapangan"), "k-1", validReport)
	if code != http.StatusAccepted || body["status"] != "accepted" {
		t.Fatalf("got %d %v", code, body)
	}
	if len(b.recs) != 1 || b.recs[0].key != body["report_id"] {
		t.Fatal("202 must mean the report is in the broker, keyed by report_id")
	}
	var env reports.Envelope
	_ = json.Unmarshal(b.recs[0].val, &env)
	if env.ClientID != "lapangan-client" || env.IdempotencyKey != "k-1" || !strings.Contains(string(env.Report), `"COASTAL"`) {
		t.Fatalf("envelope %+v", env)
	}
}

func TestFieldReportRetryKeepsTheSameReportID(t *testing.T) {
	b := &fakeBroker{}
	ts := reportServer(t, b, "")
	_, first := post(t, ts, token(t, "lapangan"), "same-key", validReport)
	_, again := post(t, ts, token(t, "lapangan"), "same-key", validReport)
	_, other := post(t, ts, token(t, "lapangan"), "other-key", validReport)
	if first["report_id"] != again["report_id"] || first["report_id"] == other["report_id"] {
		t.Fatalf("report ids: %v %v %v", first["report_id"], again["report_id"], other["report_id"])
	}
}

func TestFieldReportRejections(t *testing.T) {
	b := &fakeBroker{}
	ts := reportServer(t, b, "")
	for _, c := range []struct {
		name, role, key, body string
		want                  int
	}{
		{"media may not write", "media", "k", validReport, 403},
		{"pusat may not write", "pusat", "k", validReport, 403},
		{"key required", "lapangan", "", validReport, 400},
		{"key charset", "lapangan", "bad key!", validReport, 400},
		{"not json", "lapangan", "k", `{"area_name":`, 400},
		{"description required", "lapangan", "k", `{"area_name":"x"}`, 400},
		{"latitude range", "lapangan", "k", `{"area_name":"x","description":"y","latitude":123}`, 400},
		{"unknown hazard type", "lapangan", "k", `{"area_name":"x","description":"y","hazard_type":"ALIEN"}`, 400},
		{"too large", "lapangan", "k", `{"area_name":"x","description":"` + strings.Repeat("a", 70000) + `"}`, 400},
	} {
		if code, _ := post(t, ts, token(t, c.role), c.key, c.body); code != c.want {
			t.Errorf("%s: got %d want %d", c.name, code, c.want)
		}
	}
	if len(b.recs) != 0 {
		t.Fatal("nothing invalid may reach the broker")
	}
}

func TestFieldReportBrokerDownIs503NotA202(t *testing.T) {
	b := &fakeBroker{fail: true}
	ts := reportServer(t, b, "")
	code, body := post(t, ts, token(t, "lapangan"), "k", validReport)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("a report that is not durable must not get 202: %d %v", code, body)
	}
	var nilBroker reports.Publisher
	ts2 := reportServer(t, nilBroker, "")
	if code, _ := post(t, ts2, token(t, "lapangan"), "k", validReport); code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured: %d", code)
	}
}

func TestFieldReportStatusIsScopedToTheReporter(t *testing.T) {
	proc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/reports/fr-1" {
			io.WriteString(w, `{"report_id":"fr-1","client_id":"lapangan-client","status":"processed"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer proc.Close()
	ts := reportServer(t, &fakeBroker{}, proc.URL)
	get := func(tok, id string) int {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/field-reports/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(errors.New("request failed"))
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := get(token(t, "lapangan"), "fr-1"); c != 200 {
		t.Fatalf("own report: %d", c)
	}
	if c := get(token(t, "pusat"), "fr-1"); c != 200 {
		t.Fatalf("pusat sees all: %d", c)
	}
	if c := get(token(t, "media"), "fr-1"); c != 403 {
		t.Fatalf("media: %d", c)
	}
	if c := get(token(t, "lapangan"), "fr-404"); c != 404 {
		t.Fatalf("unknown: %d", c)
	}
}
