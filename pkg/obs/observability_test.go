package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func quietLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func TestRegistryWritesPrometheusText(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("x_total", "help with \\ and\nnewline", "a")
	c.Add(2, `q"v`)
	g := r.NewGauge("g", "gauge", "k")
	g.Set(1.5, "v")
	h := r.NewHistogram("h_seconds", "hist", []float64{0.1, 1}, "k")
	h.Observe(0.05, "v")
	h.Observe(0.5, "v")
	h.Observe(5, "v")
	var b bytes.Buffer
	r.WriteText(&b)
	out := b.String()
	for _, want := range []string{
		`# TYPE x_total counter`,
		`x_total{a="q\"v"} 2`,
		`# HELP x_total help with \\ and\nnewline`,
		`g{k="v"} 1.5`,
		`h_seconds_bucket{k="v",le="0.1"} 1`,
		`h_seconds_bucket{k="v",le="1"} 2`,
		`h_seconds_bucket{k="v",le="+Inf"} 3`,
		`h_seconds_count{k="v"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	// registering the same name again returns the same family
	if r.NewCounter("x_total", "", "a") != c {
		t.Fatal("re-registration must return the existing counter")
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	_, s := StartSpan(context.Background(), "x", SpanInternal)
	sc, ok := ParseTraceparent(s.SpanContext().Traceparent())
	if !ok || sc.TraceID != s.SpanContext().TraceID || sc.SpanID != s.SpanContext().SpanID || !sc.Sampled {
		t.Fatalf("round trip failed: %+v", sc)
	}
	for _, bad := range []string{"", "00-xyz", "00-00000000000000000000000000000000-0000000000000000-01",
		"00-" + strings.Repeat("g", 32) + "-" + strings.Repeat("b", 16) + "-01"} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestSameCorrelationIDSameTrace(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "abc123")
	_, a := StartSpan(ctx, "poll", SpanInternal)
	_, b := StartSpan(ctx, "relay", SpanProducer) // e.g. the outbox relay, which only knows the cid
	if a.SpanContext().TraceID != b.SpanContext().TraceID {
		t.Fatal("spans of the same cid must share the trace id")
	}
}

func TestKafkaHeadersCarryTraceContext(t *testing.T) {
	ctx, s := StartSpan(WithCorrelationID(context.Background(), "c1"), "produce", SpanProducer)
	h := map[string]string{}
	InjectHeaders(ctx, h)
	got := ExtractHeaders(context.Background(), h)
	if CorrelationID(got) != "c1" {
		t.Fatal("cid lost")
	}
	_, child := StartSpan(got, "consume", SpanConsumer)
	if child.SpanContext().TraceID != s.SpanContext().TraceID || child.parent != s.SpanContext().SpanID {
		t.Fatal("consumer span is not a child of the producer span")
	}
}

// One request through two services: the second service's server span is a
// child of the first service's client span, and both are in one trace.
func TestMiddlewareAndClientPropagateTrace(t *testing.T) {
	var downstreamParent string
	down := httptest.NewServer(Middleware(quietLog())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstreamParent = TraceID(r.Context())
		w.WriteHeader(204)
	})))
	defer down.Close()

	mux := http.NewServeMux()
	var upstreamTrace string
	mux.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		upstreamTrace = TraceID(r.Context())
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, down.URL+"/x", nil)
		resp, err := NewHTTPClient(quietLog(), "down", time.Second).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		w.WriteHeader(200)
	})
	up := httptest.NewServer(Middleware(quietLog(), "/health")(mux))
	defer up.Close()

	resp, err := http.Get(up.URL + "/v1/things/42")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if upstreamTrace == "" || upstreamTrace != downstreamParent {
		t.Fatalf("trace not propagated: %q vs %q", upstreamTrace, downstreamParent)
	}
	if !strings.Contains(resp.Header.Get(HeaderTraceparent), upstreamTrace) {
		t.Fatal("response must expose the traceparent")
	}

	m, _ := http.Get(up.URL + "/metrics")
	body, _ := io.ReadAll(m.Body)
	m.Body.Close()
	if !strings.Contains(string(body), `route="/v1/things/{id}"`) {
		t.Fatalf("route label must be the pattern, not the raw path:\n%s", body)
	}
	if !strings.Contains(string(body), `http_client_requests_total{service=`) {
		t.Fatal("outbound call not counted")
	}
}

func TestExporterSendsOTLPJSON(t *testing.T) {
	got := make(chan map[string]any, 4)
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request %s %s", r.URL.Path, r.Header.Get("Content-Type"))
		}
		var v map[string]any
		_ = json.NewDecoder(r.Body).Decode(&v)
		got <- v
	}))
	defer col.Close()
	e := &exporter{endpoint: col.URL, queue: make(chan finished, 8), client: col.Client()}
	_, s := StartSpan(WithCorrelationID(context.Background(), "cid-x"), "step", SpanInternal, "n", 3)
	s.RecordError(io.EOF)
	e.send([]finished{{s, time.Now()}})
	select {
	case v := <-got:
		b, _ := json.Marshal(v)
		for _, want := range []string{`"resourceSpans"`, `"service.name"`, `"name":"step"`, `"traceId":"`, `"code":2`, `"correlation_id"`} {
			if !strings.Contains(string(b), want) {
				t.Errorf("missing %s in %s", want, b)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nothing exported")
	}
}

func TestSamplerIsDeterministic(t *testing.T) {
	old := sampleRatio
	defer func() { sampleRatio = old }()
	sampleRatio = 0.5
	id := TraceIDForCorrelation("x")
	if sampled(id) != sampled(id) {
		t.Fatal("same trace id must get the same decision")
	}
	n := 0
	for i := range 2000 {
		if sampled(TraceIDForCorrelation(string(rune(i)) + "k")) {
			n++
		}
	}
	if n < 800 || n > 1200 {
		t.Fatalf("ratio 0.5 sampled %d/2000", n)
	}
}
