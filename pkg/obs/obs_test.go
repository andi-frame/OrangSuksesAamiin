package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCorrelationPropagatesThroughMiddlewareAndClient(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(ctxHandler{slog.NewJSONHandler(&buf, nil)})

	// downstream service: must receive the same correlation ID
	var gotDownstream string
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotDownstream = r.Header.Get(HeaderCorrelationID)
		w.WriteHeader(200)
	}))
	defer down.Close()

	client := NewHTTPClient(log, "down", 2*time.Second)
	up := httptest.NewServer(Middleware(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, down.URL+"/x", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
	})))
	defer up.Close()

	req, _ := http.NewRequest(http.MethodGet, up.URL+"/in", nil)
	req.Header.Set(HeaderCorrelationID, "cid-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if gotDownstream != "cid-123" {
		t.Fatalf("downstream cid = %q, want cid-123", gotDownstream)
	}
	if resp.Header.Get(HeaderCorrelationID) != "cid-123" {
		t.Fatalf("response cid header missing")
	}
	var sawOutbound, sawRequest bool
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		if m["cid"] != "cid-123" {
			t.Errorf("log line without cid: %s", line)
		}
		switch m["msg"] {
		case "outbound_call":
			sawOutbound = true
			if _, ok := m["latency_ms"]; !ok {
				t.Error("outbound_call without latency_ms")
			}
		case "request":
			sawRequest = true
		}
	}
	if !sawOutbound || !sawRequest {
		t.Fatalf("missing log lines: outbound=%v request=%v", sawOutbound, sawRequest)
	}
}

func TestMiddlewareGeneratesIDWhenAbsent(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	rec := httptest.NewRecorder()
	Middleware(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if len(rec.Header().Get(HeaderCorrelationID)) != 16 {
		t.Fatalf("expected generated 16-char id, got %q", rec.Header().Get(HeaderCorrelationID))
	}
}

func TestKafkaHeaderRoundTrip(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "abc")
	h := map[string]string{}
	InjectHeaders(ctx, h)
	if got := CorrelationID(ExtractHeaders(context.Background(), h)); got != "abc" {
		t.Fatalf("got %q", got)
	}
	if CorrelationID(ExtractHeaders(context.Background(), map[string]string{})) == "" {
		t.Fatal("expected a fresh id when header absent")
	}
}

func TestReadinessReports503OnFailingCheck(t *testing.T) {
	h := ReadinessHandler("svc", map[string]Check{
		"db": func(context.Context) error { return context.DeadlineExceeded },
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", rec.Code)
	}
}
